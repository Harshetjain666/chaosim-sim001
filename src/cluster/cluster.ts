import { createHmac } from 'node:crypto';
import { mkdir, open, readFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';
import { canonicalize, type Attestation } from '../attestation.ts';
import { defaultConfig, defaultShardConfig, type EngineConfig, type ShardConfig } from '../config.ts';
import type { TelemetryKind } from '../types.ts';
import { ClusterError } from './errors.ts';
import { fsyncDir, writeFully } from './frame.ts';
import { kindCode } from './inbox.ts';
import { lockDataDir } from './lock.ts';
import { shardFor } from './placement.ts';
import type {
  ClockMode,
  RouterMessage,
  ShardReplies,
  ShardRequest,
  ShardWorkerData,
  WorkerMessage,
  WorkerStats,
} from './protocol.ts';
import type { OpenResult, PublishedEvent, RecoveryReport, SessionView } from './shard.ts';

export interface RestartPolicy {
  /** Restarts allowed inside `windowMs` before the shard is marked failed. */
  maxRestarts: number;
  windowMs: number;
  /** Delay before the first restart, doubling with each recent crash. */
  baseDelayMs: number;
}

export const defaultRestartPolicy: RestartPolicy = { maxRestarts: 5, windowMs: 60_000, baseDelayMs: 100 };

export interface ClusterOptions {
  dataDir: string;
  shards: number;
  secret: string;
  /** 'realtime' (the default) ticks off the wall clock; 'manual' ticks only on `advance`. */
  clock?: ClockMode;
  engineConfig?: EngineConfig;
  shardConfig?: ShardConfig;
  restart?: RestartPolicy;
  /** Telemetry items held per shard while it restarts. The excess is dropped and counted. */
  ingestBufferLimit?: number;
  /** Requests held per shard while it restarts. The excess is refused as SHARD_UNAVAILABLE. */
  requestBufferLimit?: number;
  /** Director events. Delivery is at least once; see `PublishedEvent.replayed`. */
  onDirectorEvent?: (event: PublishedEvent) => void;
  onShardRecovered?: (report: RecoveryReport) => void;
}

export type ShardState = 'starting' | 'up' | 'down' | 'failed' | 'closed';

export interface ClusterStats {
  shards: {
    shardId: number;
    state: ShardState;
    restarts: number;
    /** Why the shard's last worker stopped, if it did not stop cleanly. */
    lastError: string | null;
    stats: WorkerStats | null;
  }[];
  router: {
    /** Events dropped because their shard had failed, or was down with a full buffer. */
    telemetryDropped: number;
    /** Calls refused at the door: an unknown kind, or a count that is not a positive integer. */
    telemetryRejected: number;
  };
}

interface Pending<T> {
  resolve(value: T): void;
  reject(error: Error): void;
}

interface PendingRequest extends Pending<unknown> {
  op: ShardRequest['op'];
}

interface ShardSlot {
  readonly id: number;
  readonly dir: string;
  state: ShardState;
  worker: Worker | null;
  lastError: Error | null;
  /** Telemetry gathered this event loop turn, posted as one message. */
  pendingIngest: (string | number)[];
  /** Messages held, in order, while the shard is not up. */
  outbox: RouterMessage[];
  /** Telemetry items held while the shard is not up. */
  heldTelemetry: number;
  /** Every unanswered request, posted or held. */
  requests: Map<number, PendingRequest>;
  /** Requests posted to the current worker. They die with it. */
  posted: Set<number>;
  recentCrashes: number[];
  restarts: number;
  recovery: RecoveryReport | null;
  /** `killShard` callers waiting for the shard to come back. */
  recoveryWaiters: Pending<RecoveryReport>[];
}

const MANIFEST = 'cluster.json';
const DEFAULT_INGEST_BUFFER_LIMIT = 100_000;
const DEFAULT_REQUEST_BUFFER_LIMIT = 10_000;

/**
 * What the supervisor does when a shard's worker exits: nothing more once
 * the cluster is closing, mark failed a shard that never came up while the
 * cluster is still starting (start() reports it), and restart anything else.
 */
export function exitAction(when: {
  closing: boolean;
  booting: boolean;
  wasReady: boolean;
}): 'close' | 'fail' | 'restart' {
  if (when.closing) return 'close';
  if (when.booting && !when.wasReady) return 'fail';
  return 'restart';
}

/**
 * The router: it places sessions on shards, batches their telemetry, carries
 * requests, and supervises the worker threads.
 *
 * It owns no simulation state, so it has nothing to recover; everything a
 * shard needs lives in the shard's directory. What it does own is ordering.
 * Telemetry and requests for a shard leave in the order they were made,
 * including across a restart, when they are held and then handed to the
 * recovered worker in the same order.
 */
export class ShardCluster {
  readonly shardCount: number;
  private readonly secret: string;
  private readonly clock: ClockMode;
  private readonly engineConfig: EngineConfig;
  private readonly shardConfig: ShardConfig;
  private readonly restartPolicy: RestartPolicy;
  private readonly ingestBufferLimit: number;
  private readonly requestBufferLimit: number;
  private readonly onDirectorEvent: ((event: PublishedEvent) => void) | undefined;
  private readonly onShardRecovered: ((report: RecoveryReport) => void) | undefined;
  private readonly unlock: () => Promise<void>;
  private readonly slots: ShardSlot[];
  private nextRequestId = 1;
  private flushScheduled = false;
  private booting = true;
  private closing = false;
  private closed: Promise<void> | null = null;
  private telemetryDropped = 0;
  private telemetryRejected = 0;

  private constructor(options: ClusterOptions, engineConfig: EngineConfig, unlock: () => Promise<void>) {
    this.unlock = unlock;
    this.shardCount = options.shards;
    this.secret = options.secret;
    this.clock = options.clock ?? 'realtime';
    this.engineConfig = engineConfig;
    this.shardConfig = options.shardConfig ?? defaultShardConfig;
    this.restartPolicy = options.restart ?? defaultRestartPolicy;
    this.ingestBufferLimit = options.ingestBufferLimit ?? DEFAULT_INGEST_BUFFER_LIMIT;
    this.requestBufferLimit = options.requestBufferLimit ?? DEFAULT_REQUEST_BUFFER_LIMIT;
    this.onDirectorEvent = options.onDirectorEvent;
    this.onShardRecovered = options.onShardRecovered;
    this.slots = Array.from({ length: options.shards }, (_, id) => ({
      id,
      dir: join(options.dataDir, `shard-${id}`),
      state: 'starting' as ShardState,
      worker: null,
      lastError: null,
      pendingIngest: [],
      outbox: [],
      heldTelemetry: 0,
      requests: new Map(),
      posted: new Set(),
      recentCrashes: [],
      restarts: 0,
      recovery: null,
      recoveryWaiters: [],
    }));
  }

  /**
   * Lock the data directory, then start every shard and wait until each has
   * recovered from its directory. If any shard cannot recover, stop the ones
   * that did and fail.
   */
  static async start(options: ClusterOptions): Promise<ShardCluster> {
    if (!Number.isInteger(options.shards) || options.shards < 1) {
      throw new ClusterError('BAD_REQUEST', `shards must be a positive integer, got ${options.shards}`);
    }
    const engineConfig = options.engineConfig ?? defaultConfig;
    await mkdir(options.dataDir, { recursive: true });
    const unlock = await lockDataDir(options.dataDir);
    try {
      await ensureManifest(options.dataDir, options.shards, engineConfig, options.secret);
    } catch (error) {
      await unlock();
      throw error;
    }

    const cluster = new ShardCluster(options, engineConfig, unlock);
    const started = await Promise.allSettled(cluster.slots.map((slot) => cluster.spawn(slot)));
    const failure = started.find((result) => result.status === 'rejected');
    if (failure) {
      // The start failure is the error worth reporting, not how the rest shut down.
      await cluster.close().catch(() => undefined);
      throw failure.reason;
    }
    cluster.booting = false;
    return cluster;
  }

  /** The latest recovery report from each shard that is or was up. */
  get recoveries(): RecoveryReport[] {
    return this.slots.flatMap((slot) => (slot.recovery ? [slot.recovery] : []));
  }

  shardOf(sessionId: string): number {
    return shardFor(sessionId, this.shardCount);
  }

  /** Create a session. Resolves once it is durable. Idempotent. */
  openSession(sessionId: string): Promise<OpenResult> {
    return this.request(this.slotFor(sessionId), { op: 'open', sessionId });
  }

  /**
   * Queue telemetry for a session. Synchronous and cheap: it is batched per
   * shard and posted once per event loop turn. Returns false if the telemetry
   * was refused or dropped.
   */
  ingest(sessionId: string, kind: TelemetryKind, count = 1): boolean {
    const code = kindCode(kind);
    if (code < 0 || !Number.isInteger(count) || count < 1 || this.closing) {
      this.telemetryRejected += 1;
      return false;
    }
    const slot = this.slotFor(sessionId);
    if (slot.state === 'failed' || slot.state === 'closed') {
      this.telemetryDropped += count;
      return false;
    }
    if (slot.state !== 'up') {
      if (slot.heldTelemetry >= this.ingestBufferLimit) {
        this.telemetryDropped += count;
        return false;
      }
      slot.heldTelemetry += 1;
    }
    slot.pendingIngest.push(sessionId, code, count);
    this.scheduleIngestFlush();
    return true;
  }

  /** Seal a session. Resolves with its attestation once the completion is durable. Idempotent. */
  complete(sessionId: string, completedAt: string = new Date().toISOString()): Promise<Attestation> {
    return this.request(this.slotFor(sessionId), { op: 'complete', sessionId, completedAt });
  }

  inspect(sessionId: string): Promise<SessionView | null> {
    return this.request(this.slotFor(sessionId), { op: 'inspect', sessionId });
  }

  /**
   * Manual clock only: run `ticks` ticks on every working shard. Resolves
   * once every one of them has made those ticks durable.
   */
  async advance(ticks = 1): Promise<void> {
    if (this.clock !== 'manual') throw new ClusterError('BAD_REQUEST', 'advance needs clock: manual');
    const working = this.slots.filter((slot) => slot.state !== 'failed');
    await Promise.all(working.map((slot) => this.request(slot, { op: 'advance', ticks })));
  }

  async stats(): Promise<ClusterStats> {
    const shards = await Promise.all(
      this.slots.map(async (slot) => ({
        shardId: slot.id,
        state: slot.state,
        restarts: slot.restarts,
        lastError: slot.lastError?.message ?? null,
        stats: slot.state === 'up' ? await this.request(slot, { op: 'stats' }).catch(() => null) : null,
      })),
    );
    return {
      shards,
      router: { telemetryDropped: this.telemetryDropped, telemetryRejected: this.telemetryRejected },
    };
  }

  /**
   * Fault injection: stop a shard's worker the hard way, with no flush and no
   * final checkpoint, as a crash would. Traffic for it is held from this
   * moment, and the supervisor restarts it. Resolves with the recovery report
   * once it is back, or rejects if it cannot come back.
   */
  killShard(shardId: number): Promise<RecoveryReport> {
    const slot = this.slots[shardId];
    if (slot === undefined || slot.state !== 'up' || slot.worker === null) {
      return Promise.reject(new ClusterError('BAD_REQUEST', `shard ${shardId} is not up`));
    }
    const recovered = new Promise<RecoveryReport>((resolve, reject) => {
      slot.recoveryWaiters.push({ resolve, reject });
    });
    slot.state = 'down';
    void slot.worker.terminate();
    return recovered;
  }

  /**
   * Stop every shard cleanly: flush, final checkpoint, exit. Safe to call more
   * than once. Rejects if any shard could not shut down cleanly; nothing it
   * acknowledged is lost, but its next start replays the log instead of
   * starting from a final checkpoint.
   */
  close(): Promise<void> {
    if (this.closed === null) {
      this.closing = true;
      this.closed = this.shutdown();
    }
    return this.closed;
  }

  private async shutdown(): Promise<void> {
    const closedError = new ClusterError('SHARD_UNAVAILABLE', 'the cluster is closed');
    const unclean: Error[] = [];
    try {
      await Promise.all(
        this.slots.map(async (slot) => {
          const worker = slot.worker;
          if (slot.state === 'up' && worker !== null) {
            const exited = new Promise((resolve) => worker.once('exit', resolve));
            await this.request(slot, { op: 'shutdown' }).catch((error: unknown) => {
              const cause = slot.lastError ?? (error instanceof Error ? error : new Error(String(error)));
              unclean.push(new Error(`shard ${slot.id}: ${cause.message}`, { cause }));
            });
            await exited;
          } else if (worker !== null) {
            await worker.terminate();
          }
          slot.state = 'closed';
          this.rejectAll(slot, closedError);
        }),
      );
    } finally {
      // Only once no worker can write to the directory any more.
      await this.unlock();
    }
    if (unclean.length > 0) {
      throw new AggregateError(
        unclean,
        `${unclean.length} shard(s) did not shut down cleanly: ${unclean.map((e) => e.message).join('; ')}`,
      );
    }
  }

  private slotFor(sessionId: string): ShardSlot {
    return this.slots[this.shardOf(sessionId)]!;
  }

  private spawn(slot: ShardSlot): Promise<RecoveryReport> {
    const workerData: ShardWorkerData = {
      shardId: slot.id,
      dir: slot.dir,
      secret: this.secret,
      engineConfig: this.engineConfig,
      shardConfig: this.shardConfig,
      clock: this.clock,
    };
    const worker = new Worker(new URL('./shardWorker.ts', import.meta.url), { workerData });
    slot.worker = worker;
    slot.state = 'starting';
    slot.lastError = null;

    return new Promise((resolve, reject) => {
      let ready = false;
      worker.on('message', (message: WorkerMessage) => {
        if (message.type === 'ready') {
          ready = true;
          this.onReady(slot, message.recovery);
          resolve(message.recovery);
        } else {
          this.onMessage(slot, message);
        }
      });
      worker.on('error', (error: unknown) => {
        slot.lastError = error instanceof Error ? error : new Error(String(error));
      });
      worker.on('exit', (code) => {
        if (slot.worker !== worker) return;
        slot.worker = null;
        const reason = slot.lastError?.message ?? `exit code ${code}`;
        if (!ready) {
          reject(new Error(`shard ${slot.id} could not start: ${reason}`, { cause: slot.lastError }));
        }
        this.onExit(slot, reason);
      });
    });
  }

  private onReady(slot: ShardSlot, report: RecoveryReport): void {
    slot.state = 'up';
    slot.recovery = report;
    slot.heldTelemetry = 0;
    for (const message of slot.outbox.splice(0)) this.send(slot, message);
    for (const waiter of slot.recoveryWaiters.splice(0)) waiter.resolve(report);
    this.onShardRecovered?.(report);
  }

  private onMessage(slot: ShardSlot, message: WorkerMessage): void {
    if (message.type === 'events') {
      for (const event of message.events) this.onDirectorEvent?.(event);
      return;
    }
    if (message.type === 'fatal') {
      slot.lastError = new Error(message.message);
      return;
    }
    if (message.type !== 'reply') return;
    const pending = slot.requests.get(message.id);
    if (pending === undefined) return;
    slot.requests.delete(message.id);
    slot.posted.delete(message.id);
    if (message.ok) {
      pending.resolve(message.value);
    } else {
      const { code, message: text } = message.error;
      pending.reject(code ? new ClusterError(code, text) : new Error(text));
    }
  }

  private onExit(slot: ShardSlot, reason: string): void {
    // A request the dead worker held may or may not have run. Every request
    // but advance is idempotent, so the caller can simply send it again.
    for (const id of slot.posted) {
      const pending = slot.requests.get(id);
      pending?.reject(
        pending.op === 'advance'
          ? new ClusterError(
              'ADVANCE_INTERRUPTED',
              `shard ${slot.id} stopped (${reason}) during an advance; some of its ticks may be durable, ` +
                'so read positions with inspect() instead of advancing again',
            )
          : new ClusterError('SHARD_UNAVAILABLE', `shard ${slot.id} stopped (${reason})`),
      );
      slot.requests.delete(id);
    }
    slot.posted.clear();

    switch (exitAction({ closing: this.closing, booting: this.booting, wasReady: slot.recovery !== null })) {
      case 'close':
        slot.state = 'closed';
        this.rejectAll(slot, new ClusterError('SHARD_UNAVAILABLE', 'the cluster is closed'));
        return;
      case 'fail':
        slot.state = 'failed';
        this.rejectAll(slot, new ClusterError('SHARD_FAILED', `shard ${slot.id} could not start (${reason})`));
        return;
      case 'restart':
        this.scheduleRestart(slot, reason);
        return;
    }
  }

  private scheduleRestart(slot: ShardSlot, reason: string): void {
    const now = Date.now();
    const policy = this.restartPolicy;
    slot.recentCrashes = slot.recentCrashes.filter((at) => now - at < policy.windowMs);
    slot.recentCrashes.push(now);
    if (slot.recentCrashes.length > policy.maxRestarts) {
      slot.state = 'failed';
      this.rejectAll(
        slot,
        new ClusterError(
          'SHARD_FAILED',
          `shard ${slot.id} crashed ${slot.recentCrashes.length} times within ${policy.windowMs}ms ` +
            `(last: ${reason}); not restarting it`,
        ),
      );
      return;
    }

    slot.state = 'down';
    const delayMs = policy.baseDelayMs * 2 ** (slot.recentCrashes.length - 1);
    setTimeout(() => {
      if (this.closing) return;
      slot.restarts += 1;
      // A restart that cannot recover exits again and comes back through onExit.
      this.spawn(slot).catch(() => undefined);
    }, delayMs);
  }

  private rejectAll(slot: ShardSlot, error: ClusterError): void {
    for (const pending of slot.requests.values()) pending.reject(error);
    slot.requests.clear();
    slot.posted.clear();
    slot.outbox = [];
    slot.pendingIngest = [];
    slot.heldTelemetry = 0;
    for (const waiter of slot.recoveryWaiters.splice(0)) waiter.reject(error);
  }

  private request<R extends ShardRequest>(slot: ShardSlot, request: R): Promise<ShardReplies[R['op']]> {
    if (slot.state === 'failed') {
      return Promise.reject(new ClusterError('SHARD_FAILED', `shard ${slot.id} has failed`));
    }
    if (slot.state === 'closed' || (this.closing && request.op !== 'shutdown')) {
      return Promise.reject(new ClusterError('SHARD_UNAVAILABLE', 'the cluster is closed'));
    }
    const held = slot.requests.size - slot.posted.size;
    if (slot.state !== 'up' && held >= this.requestBufferLimit) {
      return Promise.reject(
        new ClusterError('SHARD_UNAVAILABLE', `shard ${slot.id} is restarting and already holds ${held} requests`),
      );
    }
    // Telemetry queued before this request must reach the shard before it.
    this.flushIngest(slot);
    const id = this.nextRequestId;
    this.nextRequestId += 1;
    return new Promise<ShardReplies[R['op']]>((resolve, reject) => {
      slot.requests.set(id, { resolve: resolve as (value: unknown) => void, reject, op: request.op });
      this.send(slot, { type: 'request', id, request });
    });
  }

  private send(slot: ShardSlot, message: RouterMessage): void {
    if (slot.state === 'up' && slot.worker !== null) {
      if (message.type === 'request') slot.posted.add(message.id);
      slot.worker.postMessage(message);
    } else {
      slot.outbox.push(message);
    }
  }

  private scheduleIngestFlush(): void {
    if (this.flushScheduled) return;
    this.flushScheduled = true;
    setImmediate(() => {
      this.flushScheduled = false;
      for (const slot of this.slots) this.flushIngest(slot);
    });
  }

  private flushIngest(slot: ShardSlot): void {
    if (slot.pendingIngest.length === 0) return;
    const data = slot.pendingIngest;
    slot.pendingIngest = [];
    this.send(slot, { type: 'ingest', data });
  }
}

const MANIFEST_VERSION = 1;

interface Manifest {
  version: typeof MANIFEST_VERSION;
  shards: number;
  engineConfig: EngineConfig;
  /** HMAC of a fixed label under the signing secret: shows the secret is unchanged without storing it. */
  secretCheck: string;
}

function secretCheck(secret: string): string {
  return createHmac('sha256', secret).update('chaosim cluster secret check').digest('hex');
}

/**
 * Pin a data directory to the shard count, engine config and signing secret
 * that wrote it. Placement depends on the shard count and replay depends on
 * the engine config, so starting with either one changed would route
 * sessions away from their state, or replay their logs into a different
 * world. Replay also re-signs every completion after the last checkpoint, so
 * a new secret would hand out attestations that differ from the ones already
 * issued for the same sessions.
 */
async function ensureManifest(
  dataDir: string,
  shards: number,
  engineConfig: EngineConfig,
  secret: string,
): Promise<void> {
  await mkdir(dataDir, { recursive: true });
  const path = join(dataDir, MANIFEST);

  let existing: Partial<Record<keyof Manifest, unknown>> | null = null;
  try {
    existing = JSON.parse(await readFile(path, 'utf8')) as Partial<Record<keyof Manifest, unknown>>;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  if (existing !== null) {
    if (existing.version !== MANIFEST_VERSION) {
      throw new ClusterError(
        'MANIFEST_MISMATCH',
        `${dataDir} has manifest version ${String(existing.version)}; this build reads version ${MANIFEST_VERSION}`,
      );
    }
    if (existing.shards !== shards) {
      throw new ClusterError(
        'MANIFEST_MISMATCH',
        `${dataDir} belongs to a ${existing.shards} shard cluster, not ${shards}; sessions would be routed away from their state`,
      );
    }
    if (canonicalize(existing.engineConfig) !== canonicalize(engineConfig)) {
      throw new ClusterError(
        'MANIFEST_MISMATCH',
        `${dataDir} was written with a different engine config; replaying its logs would diverge`,
      );
    }
    if (existing.secretCheck !== secretCheck(secret)) {
      throw new ClusterError(
        'MANIFEST_MISMATCH',
        `${dataDir} was written under a different signing secret; completions replayed from its logs ` +
          'would be signed differently from the attestations already issued',
      );
    }
    return;
  }

  const manifest: Manifest = { version: MANIFEST_VERSION, shards, engineConfig, secretCheck: secretCheck(secret) };
  const temp = `${path}.tmp`;
  const handle = await open(temp, 'w');
  try {
    await writeFully(handle, Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`, 'utf8'));
    await handle.datasync();
  } finally {
    await handle.close();
  }
  await rename(temp, path);
  await fsyncDir(dataDir);
}
