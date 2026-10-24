import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { verify } from '../attestation.ts';
import { defaultConfig } from '../config.ts';
import { TELEMETRY_KINDS } from '../types.ts';
import { ShardCluster } from './cluster.ts';
import type { ShardState } from './cluster.ts';
import type { RecoveryReport } from './shard.ts';
import { SCENARIO_TICKS, scenarioCounts } from './workload.ts';

export interface ClusterDemoOptions {
  secret: string;
  shards: number;
  sessions: number;
  seconds: number;
  /** Kill shard 0 this many seconds in. Leave it out to keep every shard alive. */
  killAtSecond?: number;
  log: (line: string) => void;
  /** Called with everything the run reports, as structured data. The log lines stay as they are. */
  onEvent?: (event: DemoEvent) => void;
  /** Called once, when the sessions are open, with the handle for killing shards while the run is live. */
  onControl?: (control: DemoControl) => void;
  /** Aborting ends the run within a second and rejects with an AbortError. The cluster is still closed and its directory removed. */
  signal?: AbortSignal;
}

/** One shard's second, as the dashboard and the health line show it. A shard that is down has no numbers. */
export interface ShardLine {
  shardId: number;
  state: ShardState;
  restarts: number;
  ticksPerSec: number | null;
  busyRatio: number | null;
  lagP99Ms: number | null;
  fsyncP99Ms: number | null;
  walBytesPerSec: number | null;
}

export type DemoEvent =
  | { type: 'started'; shards: number; sessions: number; seconds: number }
  | { type: 'stats'; second: number; seconds: number; alerts: number; shards: ShardLine[] }
  | { type: 'killed'; shardId: number; second: number }
  | { type: 'recovered'; shardId: number; second: number; report: RecoveryReport }
  | { type: 'kill-failed'; shardId: number; second: number; error: string };

export interface DemoControl {
  /**
   * Fault injection while the run is live. Resolves once the shard is down and
   * its supervisor is bringing it back, and rejects if it cannot be killed:
   * it is not up, or the run is ending. When it is back a `recovered` event
   * follows, and the final checks hold the run to it.
   */
  killShard(shardId: number): Promise<void>;
}

export interface ShardHealth {
  /** Share of one core spent inside shard ticks. */
  busyRatio: number;
  lagP99Ms: number;
  fsyncP99Ms: number;
  /** Time a tick spent freezing state for a checkpoint. */
  checkpointFreezeMs: number;
  checkpointBytes: number;
}

export interface ClusterDemoResult {
  sessionsPerShard: number;
  /** The worst value any shard reported at any point in the run. */
  worst: ShardHealth;
  recovery: RecoveryReport | null;
  alerts: { delivered: number; duplicatesDropped: number };
  checks: { label: string; ok: boolean; detail: string }[];
}

/**
 * Run the cluster in real time.
 *
 * Every session is fed the chaos scenario's telemetry at the tick rate, the
 * shards tick off the wall clock, and one shard can be killed partway
 * through, by default right around the time the traps start tripping. A
 * health line goes out every second. At the end every session is completed
 * and the invariants that matter to a consumer are checked: attestations
 * verify, the trap fired at most once per session, and every alert that made
 * it into a signed history reached the consumer, exactly once after dedup.
 */
export async function runClusterDemo(options: ClusterDemoOptions): Promise<ClusterDemoResult> {
  const { shards, seconds, log, signal } = options;
  const emit = options.onEvent ?? (() => {});
  const tickMs = defaultConfig.tickMs;
  const dataDir = await mkdtemp(join(tmpdir(), 'chaosim-cluster-'));

  const seen = new Set<string>();
  const distinctAlerts = new Map<string, number>();
  let duplicatesDropped = 0;
  let feeder: ReturnType<typeof setInterval> | undefined;
  const cluster = await ShardCluster.start({
    dataDir,
    shards,
    secret: options.secret,
    clock: 'realtime',
    onDirectorEvent: ({ sessionId, event }) => {
      const key = `${sessionId}/${event.tick}/${event.type}`;
      if (seen.has(key)) {
        duplicatesDropped += 1;
        return;
      }
      seen.add(key);
      distinctAlerts.set(sessionId, (distinctAlerts.get(sessionId) ?? 0) + 1);
    },
  });

  try {
    const ids = Array.from({ length: options.sessions }, (_, i) => `live-${i}`);
    await Promise.all(ids.map((id) => cluster.openSession(id)));
    const workloads = ids.map((id, i) => ({ id, counts: scenarioCounts(i + 1, tickMs) }));

    let fed = 0;
    feeder = setInterval(() => {
      const tick = fed % SCENARIO_TICKS;
      for (const { id, counts } of workloads) {
        counts[tick]!.forEach((count, code) => {
          if (count > 0) cluster.ingest(id, TELEMETRY_KINDS[code]!, count);
        });
      }
      fed += 1;
    }, tickMs);

    const recoveries: RecoveryReport[] = [];
    let killsIssued = 0;
    let second = 0;
    let accepting = true;
    const worst: ShardHealth = {
      busyRatio: 0,
      lagP99Ms: 0,
      fsyncP99Ms: 0,
      checkpointFreezeMs: 0,
      checkpointBytes: 0,
    };
    const previous = new Map<number, { tick: number; walBytes: number }>();

    const killShard = async (shardId: number): Promise<void> => {
      if (!accepting) throw new Error('the run is ending');
      const recovered = cluster.killShard(shardId);
      // A shard that cannot be killed has refused by now; one that can is still recovering.
      await Promise.race([recovered, Promise.resolve()]);
      killsIssued += 1;
      log(`        killing shard ${shardId}: worker terminated, no flush, no checkpoint`);
      emit({ type: 'killed', shardId, second });
      recovered.then(
        (report) => {
          recoveries.push(report);
          log(
            `        shard ${shardId} is back: replayed ${report.ticksReplayed} ticks after checkpoint ` +
              `seq ${report.checkpointSeq ?? 'none'} in ${report.durationMs.toFixed(1)}ms`,
          );
          emit({ type: 'recovered', shardId, second, report });
        },
        (error: unknown) => {
          log(`        shard ${shardId} did not come back: ${String(error)}`);
          emit({ type: 'kill-failed', shardId, second, error: String(error) });
        },
      );
    };
    options.onControl?.({ killShard });
    emit({ type: 'started', shards, sessions: ids.length, seconds });

    for (second = 1; second <= seconds; second += 1) {
      await sleep(1000, undefined, signal === undefined ? {} : { signal });
      if (options.killAtSecond === second) {
        void killShard(0).catch((error: unknown) => log(`        shard 0 did not come back: ${String(error)}`));
      }

      const stats = await cluster.stats();
      let up = 0;
      let ticks = 0;
      let walBytes = 0;
      const now: ShardHealth = { busyRatio: 0, lagP99Ms: 0, fsyncP99Ms: 0, checkpointFreezeMs: 0, checkpointBytes: 0 };
      const lines: ShardLine[] = [];
      for (const { shardId, state, restarts, stats: s } of stats.shards) {
        if (s === null) {
          lines.push({
            shardId, state, restarts,
            ticksPerSec: null, busyRatio: null, lagP99Ms: null, fsyncP99Ms: null, walBytesPerSec: null,
          });
          continue;
        }
        up += 1;
        const before = previous.get(shardId) ?? { tick: 0, walBytes: 0 };
        const shardTicks = Math.max(0, s.tick - before.tick);
        const shardWalBytes = Math.max(0, s.wal.bytesWritten - before.walBytes);
        ticks += shardTicks;
        walBytes += shardWalBytes;
        previous.set(shardId, { tick: s.tick, walBytes: s.wal.bytesWritten });
        now.busyRatio = Math.max(now.busyRatio, s.loop.busyRatio);
        now.lagP99Ms = Math.max(now.lagP99Ms, s.loop.lagP99Ms);
        now.fsyncP99Ms = Math.max(now.fsyncP99Ms, s.wal.fsyncP99Ms);
        now.checkpointFreezeMs = Math.max(now.checkpointFreezeMs, s.checkpoints.lastFreezeMs);
        now.checkpointBytes = Math.max(now.checkpointBytes, s.checkpoints.lastBytes);
        lines.push({
          shardId, state, restarts,
          ticksPerSec: shardTicks,
          busyRatio: s.loop.busyRatio,
          lagP99Ms: s.loop.lagP99Ms,
          fsyncP99Ms: s.wal.fsyncP99Ms,
          walBytesPerSec: shardWalBytes,
        });
      }
      for (const key of Object.keys(worst) as (keyof ShardHealth)[]) {
        worst[key] = Math.max(worst[key], now[key]);
      }

      log(
        `  t=${String(second).padStart(3)}s  up ${up}/${shards}  ticks ${String(ticks).padStart(3)}/s  ` +
          `busy ${(now.busyRatio * 100).toFixed(1)}%  lag p99 ${now.lagP99Ms.toFixed(1)}ms  ` +
          `fsync p99 ${now.fsyncP99Ms.toFixed(1)}ms  wal ${(walBytes / 1024).toFixed(0)} KB/s  ` +
          `alerts ${seen.size}`,
      );
      emit({ type: 'stats', second, seconds, alerts: seen.size, shards: lines });
    }
    accepting = false;
    clearInterval(feeder);

    const attestations = await Promise.all(ids.map((id) => cluster.complete(id)));
    const verified = attestations.filter((attestation, i) =>
      verify(attestation, options.secret, { sessionId: ids[i]! }),
    ).length;
    const signed = attestations.map((attestation) => attestation.payload.directorEvents.length);
    const mostSigned = Math.max(0, ...signed);
    const mostDistinct = Math.max(0, ...distinctAlerts.values());
    const undelivered = ids.filter((id, i) => signed[i]! > 0 && !distinctAlerts.has(id)).length;

    const checks: ClusterDemoResult['checks'] = [
      {
        label: 'Every session completed with an attestation that verifies as its own',
        ok: verified === ids.length,
        detail: `${verified}/${ids.length}`,
      },
      {
        label: 'The trap fired at most once per session in the signed history',
        ok: mostSigned <= 1,
        detail: `most alerts signed for one session: ${mostSigned}`,
      },
      {
        label: 'No consumer saw two different alerts for one session',
        ok: mostDistinct <= 1,
        detail: `most distinct alerts for one session: ${mostDistinct}, replayed duplicates dropped: ${duplicatesDropped}`,
      },
      {
        label: 'Every alert in a signed history reached the consumer',
        ok: undelivered === 0,
        detail: `${signed.filter((n) => n > 0).length} signed, ${undelivered} undelivered`,
      },
    ];
    const expected = Math.max(killsIssued, options.killAtSecond === undefined ? 0 : 1);
    if (expected > 0) {
      checks.push({
        label: 'The killed shard recovered and rejoined',
        ok: recoveries.length === expected,
        detail:
          recoveries.length === 0
            ? 'never came back'
            : recoveries
                .map((r) => `replayed ${r.ticksReplayed} ticks in ${r.durationMs.toFixed(1)}ms`)
                .join(', ') + (recoveries.length < expected ? `, ${expected - recoveries.length} never came back` : ''),
      });
    }

    return {
      sessionsPerShard: ids.length / shards,
      worst,
      recovery: recoveries.at(-1) ?? null,
      alerts: { delivered: seen.size, duplicatesDropped },
      checks,
    };
  } finally {
    clearInterval(feeder);
    await cluster.close();
    await rm(dataDir, { recursive: true, force: true });
  }
}
