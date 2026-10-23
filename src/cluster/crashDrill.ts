import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { canonicalize } from '../attestation.ts';
import type { EngineSnapshot, SimulationEngine } from '../engine.ts';
import { ShardCluster } from './cluster.ts';
import type { RecoveryReport } from './shard.ts';
import {
  SCENARIO_TICKS,
  ScenarioDriver,
  referenceEngine,
  referenceFingerprint,
  type DriverSession,
} from './workload.ts';

export const DRILL_COMPLETED_AT = '2026-01-01T00:00:00.000Z';

const CHILD = fileURLToPath(new URL('./crashDrillChild.ts', import.meta.url));
const SECOND_RUN_ROUNDS = 40;

export function drillSessions(count: number): DriverSession[] {
  return Array.from({ length: count }, (_, i) => ({ id: `drill-${i}`, seed: i + 1 }));
}

export interface CrashDrillOptions {
  secret: string;
  shards?: number;
  sessions?: number;
  /**
   * The first kill lands once every session has this many durable ticks.
   * Default: a random tick from 60 to 200, leaving room for the second run.
   */
  killAfterRound?: number;
  /** Where the cluster writes. Default: a fresh temp directory, removed afterwards. */
  dataDir?: string;
  log?: (line: string) => void;
}

export interface CrashDrillResult {
  /** Durable ticks every session had when the first kill landed. */
  killedAfterRound: number;
  /** How each shard recovered from the second kill. */
  recoveries: RecoveryReport[];
  checks: { label: string; ok: boolean; detail: string }[];
}

interface ChildRun {
  signal: NodeJS.Signals | null;
  /** Highest tick the child reported durable for every session. */
  acked: number;
  resumed: { min: number; max: number } | null;
  /** Exact per session ticks the child reported before it paused. */
  paused: Record<string, number> | null;
}

/**
 * Kill a cluster with SIGKILL, twice, and prove it comes back exactly.
 *
 * 1. A child process runs a manual clock cluster through the chaos scenario
 *    and is killed while it is running, so the kill can land in the middle
 *    of a write.
 * 2. A second child recovers the same directory, carries on, reports the
 *    exact tick of every session once they are durable, and is killed.
 * 3. This process recovers the directory and checks every session is at
 *    exactly the reported tick, with exactly the state the never crashed
 *    single session engine has at that tick. Then it finishes the scenario:
 *    every session must end on its reference fingerprint.
 * 4. One more restart must replay nothing and still hold every attestation,
 *    which proves recovery left behind a log that recovers again.
 *
 * Only a kill at a known point can prove nothing durable was lost: a running
 * process can always die with work done that it never got to report.
 */
export async function runCrashDrill(options: CrashDrillOptions): Promise<CrashDrillResult> {
  const shards = options.shards ?? 4;
  const count = options.sessions ?? 32;
  const killAfter = options.killAfterRound ?? 60 + Math.floor(Math.random() * 141);
  if (killAfter < 1 || killAfter + SECOND_RUN_ROUNDS >= SCENARIO_TICKS) {
    throw new RangeError(`killAfterRound must be between 1 and ${SCENARIO_TICKS - SECOND_RUN_ROUNDS - 1}`);
  }
  const log = options.log ?? (() => {});
  const dataDir = options.dataDir ?? (await mkdtemp(join(tmpdir(), 'chaosim-drill-')));
  const sessions = drillSessions(count);
  const start = () => ShardCluster.start({ dataDir, shards, secret: options.secret, clock: 'manual' });

  try {
    const first = await runChild(dataDir, shards, count, options.secret, { killWhenAcked: killAfter });
    log(`kill 1: ${first.signal} while running, after ${first.acked} durable ticks`);

    const second = await runChild(dataDir, shards, count, options.secret, {
      pauseAfterRounds: SECOND_RUN_ROUNDS,
    });
    log(
      `restart: sessions resumed at ticks ${second.resumed?.min}..${second.resumed?.max}; ` +
        `kill 2: ${second.signal} after ${SECOND_RUN_ROUNDS} more acknowledged rounds`,
    );

    const cluster = await start();
    const recoveries = cluster.recoveries;
    const driver = new ScenarioDriver(cluster, sessions, DRILL_COMPLETED_AT);
    let atReportedTick = 0;
    let exact = 0;
    let matching = 0;
    try {
      for (const r of recoveries) {
        log(
          `shard ${r.shardId}: checkpoint seq ${r.checkpointSeq ?? 'none'}, replayed ${r.ticksReplayed} ticks, ` +
            `discarded ${r.tornBytesDiscarded} torn bytes, recovered in ${r.durationMs.toFixed(1)}ms`,
        );
      }
      for (const { id, seed } of sessions) {
        const view = await cluster.inspect(id);
        if (view?.status !== 'active') continue;
        if (view.snapshot.tick === second.paused?.[id]) atReportedTick += 1;
        if (sameEngine(view.snapshot, referenceEngine(seed, view.snapshot.tick))) exact += 1;
      }

      await driver.resync();
      while (await driver.round()) {
        // Finish the scenario on the recovered cluster.
      }
      for (const { id, seed } of sessions) {
        const reference = referenceFingerprint(id, { secret: options.secret, seed, completedAt: DRILL_COMPLETED_AT });
        if (driver.attestations.get(id)?.signature === reference) matching += 1;
      }
    } finally {
      await cluster.close();
    }

    const again = await start();
    let remembered = 0;
    try {
      for (const { id } of sessions) {
        const view = await again.inspect(id);
        const expected = driver.attestations.get(id)?.signature;
        if (view?.status === 'completed' && view.attestation.signature === expected) remembered += 1;
      }
    } finally {
      await again.close();
    }
    const replayedOnRestart = again.recoveries.reduce((n, r) => n + r.recordsReplayed, 0);

    return {
      killedAfterRound: first.acked,
      recoveries,
      checks: [
        {
          label: 'Kill 1 was a SIGKILL while the cluster was running',
          ok: first.signal === 'SIGKILL',
          detail: `signal=${first.signal}, after ${first.acked} durable ticks`,
        },
        {
          label: 'Nothing acknowledged before kill 1 was lost',
          ok: second.resumed !== null && second.resumed.min >= first.acked,
          detail: `acked ${first.acked}, resumed at ${second.resumed?.min}..${second.resumed?.max}`,
        },
        {
          label: 'Kill 2 recovered every session at exactly its acknowledged tick',
          ok: second.signal === 'SIGKILL' && atReportedTick === count,
          detail: `${atReportedTick}/${count} sessions`,
        },
        {
          label: 'Recovered state is identical to the never crashed engine',
          ok: exact === count,
          detail: `${exact}/${count} sessions`,
        },
        {
          label: 'Every session finished on its reference fingerprint',
          ok: matching === count,
          detail: `${matching}/${count} fingerprints`,
        },
        {
          label: 'A clean restart afterwards replays nothing and keeps every attestation',
          ok: remembered === count && replayedOnRestart === 0,
          detail: `${remembered}/${count} attestations, ${replayedOnRestart} records replayed`,
        },
      ],
    };
  } finally {
    if (options.dataDir === undefined) await rm(dataDir, { recursive: true, force: true });
  }
}

function sameEngine(snapshot: EngineSnapshot, reference: SimulationEngine): boolean {
  return (
    canonicalize(snapshot.state) === canonicalize(reference.state) &&
    snapshot.trapTripped === reference.isTrapTripped &&
    canonicalize(snapshot.directorLog) === canonicalize(reference.directorLog)
  );
}

/** Node 22.6 to 23.5 needs the type stripping flag passed on to the child. */
function typeScriptFlags(): string[] {
  return process.execArgv.filter(
    (flag) => flag.startsWith('--experimental-strip-types') || flag.startsWith('--experimental-transform-types'),
  );
}

function runChild(
  dataDir: string,
  shards: number,
  sessions: number,
  secret: string,
  mode: { killWhenAcked: number } | { pauseAfterRounds: number },
): Promise<ChildRun> {
  const args = [...typeScriptFlags(), CHILD, dataDir, String(shards), String(sessions)];
  if ('pauseAfterRounds' in mode) args.push(String(mode.pauseAfterRounds));
  const child = spawn(process.execPath, args, {
    env: { ...process.env, CHAOSIM_SECRET: secret },
    stdio: ['ignore', 'pipe', 'inherit'],
  });

  return new Promise((resolve, reject) => {
    const run: ChildRun = { signal: null, acked: 0, resumed: null, paused: null };
    let exitCode: number | null = null;
    let killed = false;
    const kill = () => {
      if (killed) return;
      killed = true;
      child.kill('SIGKILL');
    };

    createInterface({ input: child.stdout }).on('line', (line) => {
      const [word, first, second] = line.split(' ');
      if (word === 'resumed') {
        run.resumed = { min: Number(first), max: Number(second) };
      } else if (word === 'acked') {
        // Lines still in the pipe after the kill were durable when written, so they count.
        run.acked = Math.max(run.acked, Number(first));
        if ('killWhenAcked' in mode && run.acked >= mode.killWhenAcked) kill();
      } else if (word === 'paused') {
        run.paused = JSON.parse(line.slice('paused '.length)) as Record<string, number>;
        kill();
      }
    });
    child.on('error', reject);
    child.on('exit', (code, signal) => {
      exitCode = code;
      run.signal = signal;
    });
    // 'close' fires once stdout is drained, so every reported line has been seen.
    child.on('close', () => {
      if (run.signal === null) {
        reject(new Error(`the cluster process exited by itself (code ${exitCode}) before it could be killed`));
      } else {
        resolve(run);
      }
    });
  });
}
