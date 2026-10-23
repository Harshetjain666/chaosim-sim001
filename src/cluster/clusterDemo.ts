import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { verify } from '../attestation.ts';
import { defaultConfig } from '../config.ts';
import { TELEMETRY_KINDS } from '../types.ts';
import { ShardCluster } from './cluster.ts';
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
  const { shards, seconds, log } = options;
  const tickMs = defaultConfig.tickMs;
  const dataDir = await mkdtemp(join(tmpdir(), 'chaosim-cluster-'));

  const seen = new Set<string>();
  const distinctAlerts = new Map<string, number>();
  let duplicatesDropped = 0;
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
    const feeder = setInterval(() => {
      const tick = fed % SCENARIO_TICKS;
      for (const { id, counts } of workloads) {
        counts[tick]!.forEach((count, code) => {
          if (count > 0) cluster.ingest(id, TELEMETRY_KINDS[code]!, count);
        });
      }
      fed += 1;
    }, tickMs);

    let recovery: RecoveryReport | null = null;
    const worst: ShardHealth = {
      busyRatio: 0,
      lagP99Ms: 0,
      fsyncP99Ms: 0,
      checkpointFreezeMs: 0,
      checkpointBytes: 0,
    };
    const previous = new Map<number, { tick: number; walBytes: number }>();

    for (let second = 1; second <= seconds; second += 1) {
      await sleep(1000);
      if (options.killAtSecond === second) {
        log('        killing shard 0: worker terminated, no flush, no checkpoint');
        void cluster.killShard(0).then(
          (report) => {
            recovery = report;
            log(
              `        shard 0 is back: replayed ${report.ticksReplayed} ticks after checkpoint ` +
                `seq ${report.checkpointSeq ?? 'none'} in ${report.durationMs.toFixed(1)}ms`,
            );
          },
          (error: unknown) => log(`        shard 0 did not come back: ${String(error)}`),
        );
      }

      const stats = await cluster.stats();
      let up = 0;
      let ticks = 0;
      let walBytes = 0;
      const now: ShardHealth = { busyRatio: 0, lagP99Ms: 0, fsyncP99Ms: 0, checkpointFreezeMs: 0, checkpointBytes: 0 };
      for (const { shardId, stats: s } of stats.shards) {
        if (s === null) continue;
        up += 1;
        const before = previous.get(shardId) ?? { tick: 0, walBytes: 0 };
        ticks += Math.max(0, s.tick - before.tick);
        walBytes += Math.max(0, s.wal.bytesWritten - before.walBytes);
        previous.set(shardId, { tick: s.tick, walBytes: s.wal.bytesWritten });
        now.busyRatio = Math.max(now.busyRatio, s.loop.busyRatio);
        now.lagP99Ms = Math.max(now.lagP99Ms, s.loop.lagP99Ms);
        now.fsyncP99Ms = Math.max(now.fsyncP99Ms, s.wal.fsyncP99Ms);
        now.checkpointFreezeMs = Math.max(now.checkpointFreezeMs, s.checkpoints.lastFreezeMs);
        now.checkpointBytes = Math.max(now.checkpointBytes, s.checkpoints.lastBytes);
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
    }
    clearInterval(feeder);

    const attestations = await Promise.all(ids.map((id) => cluster.complete(id)));
    const verified = attestations.filter((attestation) => verify(attestation, options.secret)).length;
    const signed = attestations.map((attestation) => attestation.payload.directorEvents.length);
    const mostSigned = Math.max(0, ...signed);
    const mostDistinct = Math.max(0, ...distinctAlerts.values());
    const undelivered = ids.filter((id, i) => signed[i]! > 0 && !distinctAlerts.has(id)).length;

    const checks: ClusterDemoResult['checks'] = [
      {
        label: 'Every session completed with an attestation that verifies',
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
    if (options.killAtSecond !== undefined) {
      const report = recovery as RecoveryReport | null;
      checks.push({
        label: 'The killed shard recovered and rejoined',
        ok: report !== null,
        detail: report ? `replayed ${report.ticksReplayed} ticks in ${report.durationMs.toFixed(1)}ms` : 'never came back',
      });
    }

    return {
      sessionsPerShard: ids.length / shards,
      worst,
      recovery,
      alerts: { delivered: seen.size, duplicatesDropped },
      checks,
    };
  } finally {
    await cluster.close();
    await rm(dataDir, { recursive: true, force: true });
  }
}
