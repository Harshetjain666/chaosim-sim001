import { mkdir } from 'node:fs/promises';
import type { Attestation } from '../attestation.ts';
import type { EngineConfig, ShardConfig } from '../config.ts';
import type { EngineSnapshot } from '../engine.ts';
import type { DirectorEvent } from '../scenarioDirector.ts';
import { Simulation } from '../simulation.ts';
import {
  encodeCheckpoint,
  loadCheckpoint,
  pruneCheckpoints,
  writeCheckpoint,
  type ShardCheckpoint,
} from './checkpoint.ts';
import { ClusterError, CorruptionError } from './errors.ts';
import { Inbox, expandBatch } from './inbox.ts';
import {
  WalWriter,
  repairTornTail,
  scanWal,
  type TickInput,
  type WalRecord,
  type WalStats,
} from './wal.ts';

export interface PublishedEvent {
  sessionId: string;
  shardId: number;
  /**
   * True when recovery published the event again. It may already have been
   * delivered before the crash; `(sessionId, event.tick, event.type)`
   * identifies it, so a consumer can drop the duplicate.
   */
  replayed: boolean;
  event: DirectorEvent;
}

export interface RecoveryReport {
  shardId: number;
  checkpointSeq: number | null;
  recordsReplayed: number;
  ticksReplayed: number;
  tornBytesDiscarded: number;
  sessions: number;
  tick: number;
  durationMs: number;
}

export type SessionView =
  | {
      status: 'active';
      shardTick: number;
      /** True once everything in `snapshot` is on disk, so no crash can roll it back. */
      durable: boolean;
      snapshot: EngineSnapshot;
    }
  | { status: 'completed'; attestation: Attestation }
  /** Completed long enough ago that its attestation is no longer retained. */
  | { status: 'expired' };

export type OpenResult = 'opened' | 'active' | 'completed';

export interface CheckpointStats {
  written: number;
  skipped: number;
  lastSeq: number;
  lastBytes: number;
  /** Time the tick spent freezing state: the only part that blocks the loop. */
  lastFreezeMs: number;
  /** Freeze to pruned, including the wait for the log and the disk. */
  lastDurationMs: number;
}

export interface ShardStats {
  shardId: number;
  tick: number;
  sessions: number;
  completedRetained: number;
  inboxDrops: number;
  unknownSessionDrops: number;
  wal: WalStats & { lastSeq: number; durableSeq: number; pendingBytes: number };
  checkpoints: CheckpointStats;
}

export interface ShardOptions {
  dir: string;
  shardId: number;
  secret: string;
  engineConfig: EngineConfig;
  shardConfig: ShardConfig;
  /** Director events, released only once the ticks that produced them are durable. */
  onEvents?: (events: PublishedEvent[]) => void;
  /** Called once, when the shard hits an error it cannot safely continue past. */
  onFatal?: (error: Error) => void;
}

interface Session {
  sim: Simulation;
  inbox: Inbox;
}

interface HeldEvent {
  seq: number;
  published: PublishedEvent;
}

/**
 * One shard: many independent sessions, one log, one tick.
 *
 * Every state change goes through an `apply*` method, and the live
 * operations call them right after appending the matching log record, so
 * recovery is the same code replaying the same records. That, plus a pure
 * engine, is what makes a recovered shard bit identical to the lost one.
 *
 * The shard never touches a clock and never blocks on the disk: a caller
 * drives `step()` (the worker's fixed timestep loop, or a test), and all I/O
 * is asynchronous behind the WAL writer and the checkpoint routine.
 */
export class Shard {
  private readonly dir: string;
  private readonly shardId: number;
  private readonly secret: string;
  private readonly engineConfig: EngineConfig;
  private readonly shardConfig: ShardConfig;
  private readonly onEvents: ((events: PublishedEvent[]) => void) | undefined;
  private readonly onFatal: ((error: Error) => void) | undefined;
  private readonly wal: WalWriter;

  private readonly sessions = new Map<string, Session>();
  private readonly completed = new Map<string, Attestation>();
  /** Completed ids whose attestations were dropped, oldest first. */
  private readonly forgotten = new Set<string>();
  private shardTick = 0;
  private held: HeldEvent[] = [];
  private failure: Error | null = null;
  /** Persists frozen checkpoints one after another, in order. */
  private checkpointChain: Promise<void> = Promise.resolve();
  /** Checkpoints frozen but not yet durable. */
  private checkpointsPending = 0;
  private inboxDrops = 0;
  private unknownSessionDrops = 0;
  private readonly checkpointStats: CheckpointStats = {
    written: 0,
    skipped: 0,
    lastSeq: 0,
    lastBytes: 0,
    lastFreezeMs: 0,
    lastDurationMs: 0,
  };

  private constructor(options: ShardOptions, wal: WalWriter) {
    this.dir = options.dir;
    this.shardId = options.shardId;
    this.secret = options.secret;
    this.engineConfig = options.engineConfig;
    this.shardConfig = options.shardConfig;
    this.onEvents = options.onEvents;
    this.onFatal = options.onFatal;
    this.wal = wal;
  }

  /**
   * Open a shard directory and recover it: load the newest checkpoint, cut a
   * torn final record off the log, replay the records after the checkpoint,
   * and start a fresh segment. Every event in the recovered state is then
   * published again, marked `replayed`.
   */
  static async open(options: ShardOptions): Promise<{ shard: Shard; recovery: RecoveryReport }> {
    const started = performance.now();
    await mkdir(options.dir, { recursive: true });
    const checkpoint = await loadCheckpoint(options.dir);
    const afterSeq = checkpoint?.seq ?? 0;
    await pruneCheckpoints(options.dir, afterSeq);

    const scan = await scanWal(options.dir, afterSeq);
    if (scan.tornTail) await repairTornTail(scan.tornTail);

    const wal = await WalWriter.open(options.dir, scan.lastSeq + 1);
    const shard = new Shard(options, wal);
    let ticksReplayed = 0;
    try {
      if (checkpoint) shard.restore(checkpoint);
      for (const record of scan.records) {
        if (record.op === 'tick') ticksReplayed += 1;
        shard.apply(record);
      }
      // Segments a crash stopped the last checkpoint from pruning.
      await wal.pruneBefore(afterSeq + 1);
    } catch (error) {
      await wal.close().catch(() => undefined);
      throw error;
    }

    const recovery: RecoveryReport = {
      shardId: options.shardId,
      checkpointSeq: checkpoint?.seq ?? null,
      recordsReplayed: scan.records.length,
      ticksReplayed,
      tornBytesDiscarded: scan.tornTail?.bytes ?? 0,
      sessions: shard.sessions.size,
      tick: shard.shardTick,
      durationMs: performance.now() - started,
    };
    const republished = shard.recoveredEvents();
    if (republished.length > 0) shard.onEvents?.(republished);
    return { shard, recovery };
  }

  get tick(): number {
    return this.shardTick;
  }

  get sessionCount(): number {
    return this.sessions.size;
  }

  /** Create a session. Idempotent; durable with the next flush. */
  openSession(id: string): OpenResult {
    this.assertHealthy();
    if (this.sessions.has(id)) return 'active';
    if (this.completed.has(id) || this.forgotten.has(id)) return 'completed';
    this.wal.append({ op: 'open', sessionId: id });
    this.applyOpen(id);
    return 'opened';
  }

  /** Count telemetry into a session's inbox for its next tick. Never touches the log. */
  ingest(id: string, code: number, count: number): void {
    const session = this.sessions.get(id);
    if (session === undefined) {
      this.unknownSessionDrops += count;
      return;
    }
    this.inboxDrops += count - session.inbox.add(code, count);
  }

  /**
   * Run one shard tick. Every active session steps once with whatever its
   * inbox collected, and the inputs go to the log as a single record. Events
   * the director emits are held until that record is durable. An idle shard
   * neither ticks nor logs.
   */
  step(): void {
    this.assertHealthy();
    if (this.sessions.size === 0) return;

    const tick = this.shardTick;
    const inputs: TickInput[] = [];
    for (const [id, session] of this.sessions) {
      const counts = session.inbox.drain();
      if (counts) inputs.push([id, counts]);
    }
    const seq = this.wal.append({ op: 'tick', tick, inputs });
    for (const published of this.applyTick(tick, inputs)) this.held.push({ seq, published });

    if (this.shardTick % this.shardConfig.checkpointEveryTicks === 0) this.startCheckpoint();
  }

  /** Make everything so far durable, then release the events that were waiting for it. */
  async flush(): Promise<void> {
    this.assertHealthy();
    let durable: number;
    try {
      durable = await this.wal.flush();
    } catch (error) {
      throw this.fail(error);
    }
    this.release(durable);
  }

  /** Flush without waiting, for the realtime loop. A failure still reaches `onFatal`. */
  requestFlush(): void {
    this.flush().catch(() => undefined);
  }

  /**
   * Seal a session and return its signed attestation once the completion is
   * durable. A retry gets the original attestation back, whatever
   * `completedAt` it carries.
   */
  async complete(id: string, completedAt: string): Promise<Attestation> {
    this.assertHealthy();
    const done = this.completed.get(id);
    if (done) {
      await this.flush();
      return done;
    }
    if (this.forgotten.has(id)) {
      throw new ClusterError(
        'ATTESTATION_EXPIRED',
        `session ${id} completed, but shard ${this.shardId} no longer retains its attestation`,
      );
    }
    if (!this.sessions.has(id)) {
      throw new ClusterError('UNKNOWN_SESSION', `no session ${id} on shard ${this.shardId}`);
    }
    this.wal.append({ op: 'complete', sessionId: id, completedAt });
    const attestation = this.applyComplete(id, completedAt);
    await this.flush();
    return attestation;
  }

  inspect(id: string): SessionView | null {
    const session = this.sessions.get(id);
    if (session) {
      return {
        status: 'active',
        shardTick: this.shardTick,
        durable: this.wal.durableSeq >= this.wal.lastSeq,
        snapshot: session.sim.engine.snapshot(),
      };
    }
    const attestation = this.completed.get(id);
    if (attestation) return { status: 'completed', attestation };
    return this.forgotten.has(id) ? { status: 'expired' } : null;
  }

  /** Counters, plus fsync latency percentiles since the previous call. */
  takeStats(): ShardStats {
    return {
      shardId: this.shardId,
      tick: this.shardTick,
      sessions: this.sessions.size,
      completedRetained: this.completed.size,
      inboxDrops: this.inboxDrops,
      unknownSessionDrops: this.unknownSessionDrops,
      wal: {
        ...this.wal.takeStats(),
        lastSeq: this.wal.lastSeq,
        durableSeq: this.wal.durableSeq,
        pendingBytes: this.wal.pendingBytes,
      },
      checkpoints: { ...this.checkpointStats },
    };
  }

  /**
   * Stop cleanly: let a running checkpoint finish, write a final one so the
   * next start has nothing to replay, and close the log.
   */
  async close(): Promise<void> {
    this.assertHealthy();
    await this.checkpointChain;
    if (this.wal.lastSeq > this.checkpointStats.lastSeq) {
      this.startCheckpoint();
      await this.checkpointChain;
    }
    this.assertHealthy();
    this.failure = new Error(`shard ${this.shardId} is closed`);
    await this.wal.close();
  }

  private apply(record: WalRecord): void {
    switch (record.op) {
      case 'open':
        this.applyOpen(record.sessionId);
        return;
      case 'tick':
        this.applyTick(record.tick, record.inputs);
        return;
      case 'complete':
        this.applyComplete(record.sessionId, record.completedAt);
        return;
    }
  }

  /**
   * Every event the recovered state holds, marked replayed. Delivery is at
   * least once, and an event released just before a crash may never have
   * left the process, whether recovery re-derived it from the log or found it
   * already folded into the checkpoint. So all of them go out again, the
   * live sessions' director logs and the retained attestations' events. The
   * trap fires at most once per session, so this is bounded by the sessions.
   */
  private recoveredEvents(): PublishedEvent[] {
    const events: PublishedEvent[] = [];
    const add = (sessionId: string, event: DirectorEvent) =>
      events.push({ sessionId, shardId: this.shardId, replayed: true, event: { ...event } });
    for (const [id, session] of this.sessions) {
      for (const event of session.sim.engine.directorLog) add(id, event);
    }
    for (const [id, attestation] of this.completed) {
      for (const event of attestation.payload.directorEvents) add(id, event);
    }
    return events;
  }

  private applyOpen(id: string): void {
    if (this.sessions.has(id) || this.completed.has(id) || this.forgotten.has(id)) {
      throw new CorruptionError(`session ${id} opened twice`);
    }
    this.sessions.set(id, this.newSession(new Simulation({ secret: this.secret, config: this.engineConfig })));
  }

  private applyTick(tick: number, inputs: readonly TickInput[]): PublishedEvent[] {
    if (tick !== this.shardTick) {
      throw new CorruptionError(`tick record ${tick} found while the shard is at tick ${this.shardTick}`);
    }
    const countsById = new Map<string, number[]>();
    for (const [id, counts] of inputs) {
      if (!this.sessions.has(id) || countsById.has(id)) {
        throw new CorruptionError(`tick ${tick} carries input for unknown or repeated session ${id}`);
      }
      countsById.set(id, counts);
    }

    const published: PublishedEvent[] = [];
    for (const [id, session] of this.sessions) {
      const counts = countsById.get(id);
      const t = session.sim.engine.tick * this.engineConfig.tickMs;
      const result = session.sim.step(counts ? expandBatch(counts, t) : []);
      for (const event of result.events) {
        published.push({ sessionId: id, shardId: this.shardId, replayed: false, event: { ...event } });
      }
    }
    this.shardTick += 1;
    return published;
  }

  private applyComplete(id: string, completedAt: string): Attestation {
    const session = this.sessions.get(id);
    if (session === undefined) {
      throw new CorruptionError(`session ${id} completed while not active`);
    }
    const attestation = session.sim.complete(completedAt);
    this.sessions.delete(id);
    this.completed.set(id, attestation);
    // Drop the oldest attestations past retention but keep their ids, and
    // past that retention forget the oldest ids too. Replay runs this same
    // code in the same order, so what is remembered is deterministic.
    for (const oldest of this.completed.keys()) {
      if (this.completed.size <= this.shardConfig.completedRetention) break;
      this.completed.delete(oldest);
      this.forgotten.add(oldest);
    }
    for (const oldest of this.forgotten) {
      if (this.forgotten.size <= this.shardConfig.completedIdRetention) break;
      this.forgotten.delete(oldest);
    }
    return attestation;
  }

  private restore(checkpoint: ShardCheckpoint): void {
    if (checkpoint.shardId !== this.shardId) {
      throw new CorruptionError(
        `checkpoint ${checkpoint.seq} belongs to shard ${checkpoint.shardId}, not ${this.shardId}`,
      );
    }
    this.shardTick = checkpoint.tick;
    this.checkpointStats.lastSeq = checkpoint.seq;
    for (const { id, engine } of checkpoint.sessions) {
      const sim = new Simulation({ secret: this.secret, config: this.engineConfig, snapshot: engine });
      this.sessions.set(id, this.newSession(sim));
    }
    for (const { id, attestation } of checkpoint.completed) this.completed.set(id, attestation);
    for (const id of checkpoint.forgotten ?? []) this.forgotten.add(id);
  }

  private newSession(sim: Simulation): Session {
    return { sim, inbox: new Inbox(this.shardConfig.inboxCapPerKind) };
  }

  private release(durableSeq: number): void {
    let count = 0;
    while (count < this.held.length && this.held[count]!.seq <= durableSeq) count += 1;
    if (count === 0) return;
    const ready = this.held.splice(0, count).map((held) => held.published);
    this.onEvents?.(ready);
  }

  /**
   * Freeze now, persist in the background. The freeze runs synchronously
   * inside the tick, so the checkpoint is exactly the state after it, and
   * checkpoints land on schedule however slow the disk is. Persisting is
   * chained so they become durable in order. If one is already waiting
   * behind a running one, the disk is not keeping up and this one is skipped
   * and counted rather than queueing without bound.
   */
  private startCheckpoint(): void {
    if (this.checkpointsPending >= 2) {
      this.checkpointStats.skipped += 1;
      return;
    }
    const started = performance.now();
    const seq = this.wal.lastSeq;
    const bytes = encodeCheckpoint(this.freeze(seq));
    this.wal.rotate();
    const frozenMs = performance.now() - started;

    this.checkpointsPending += 1;
    this.checkpointChain = this.checkpointChain
      .then(() => this.persistCheckpoint(seq, bytes, started, frozenMs))
      .finally(() => {
        this.checkpointsPending -= 1;
      });
  }

  /**
   * The order is what makes a crash at any point safe: the log is durable
   * through the frozen seq (which also releases every event the checkpoint
   * contains) before the checkpoint is written, and old segments and older
   * checkpoints go only once this one is durable.
   */
  private async persistCheckpoint(
    seq: number,
    bytes: Buffer,
    started: number,
    frozenMs: number,
  ): Promise<void> {
    try {
      await this.flush();
      await writeCheckpoint(this.dir, seq, bytes);
      await this.wal.pruneBefore(seq + 1);
      await pruneCheckpoints(this.dir, seq);
    } catch (error) {
      this.fail(error);
      return;
    }

    const stats = this.checkpointStats;
    stats.written += 1;
    stats.lastSeq = seq;
    stats.lastBytes = bytes.length;
    stats.lastFreezeMs = frozenMs;
    stats.lastDurationMs = performance.now() - started;
  }

  private freeze(seq: number): ShardCheckpoint {
    return {
      version: 1,
      shardId: this.shardId,
      seq,
      tick: this.shardTick,
      sessions: [...this.sessions].map(([id, session]) => ({ id, engine: session.sim.engine.snapshot() })),
      completed: [...this.completed].map(([id, attestation]) => ({ id, attestation })),
      forgotten: [...this.forgotten],
    };
  }

  private fail(error: unknown): Error {
    if (this.failure === null) {
      this.failure = error instanceof Error ? error : new Error(String(error));
      this.onFatal?.(this.failure);
    }
    return this.failure;
  }

  private assertHealthy(): void {
    if (this.failure) throw this.failure;
  }
}
