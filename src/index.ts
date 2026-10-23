import { defaultConfig } from './config.ts';
import { runChaosLoadTest, type ChaosResult } from './chaosLoadTest.ts';
import { Simulation } from './simulation.ts';
import { verify, type Attestation } from './attestation.ts';
import { mulberry32 } from './rng.ts';
import { telemetryForTick } from './scenario.ts';
import { runLiveLoop } from './liveLoop.ts';
import { CEO_ALERT_MESSAGE } from './scenarioDirector.ts';
import { runCrashDrill } from './cluster/crashDrill.ts';
import { runClusterDemo } from './cluster/clusterDemo.ts';
import type { WorldState } from './types.ts';

const SECRET = process.env.CHAOSIM_SECRET ?? 'sim-001-dev-secret';

/** `--name=N`, or the fallback when the flag is missing or not a number. */
function numberFlag(argv: string[], name: string, fallback: number): number {
  const flag = argv.find((a) => a.startsWith(`--${name}=`));
  if (!flag) return fallback;
  const value = Number(flag.slice(name.length + 3));
  return Number.isFinite(value) ? value : fallback;
}

function rule(char = '='): string {
  return char.repeat(64);
}

function printState(label: string, state: WorldState): void {
  console.log(
    `${label.padEnd(16)} panic=${state.panicIndex.toFixed(1)}  ` +
      `health=${state.codebaseHealth}  ceoAnxiety=${state.ceoAnxietyIndex}  ` +
      `velocity=${state.keystrokeVelocity.toFixed(0)}  backspace=${state.backspaceCount}`,
  );
}

function printAlert(): void {
  console.log('\n[ALERT]');
  console.log('CEO:');
  console.log(`  "${CEO_ALERT_MESSAGE}"\n`);
}

function printAttestation(attestation: Attestation, verified: boolean): void {
  console.log(rule());
  console.log('Cryptographic Attestation');
  console.log(rule());
  console.log(`algorithm      ${attestation.algorithm}`);
  console.log(`completedAt    ${attestation.payload.completedAt}`);
  console.log(`tickCount      ${attestation.payload.tickCount}`);
  console.log(`trapTripped    ${attestation.payload.trapTripped}`);
  console.log('\n[VERIFIED_FINGERPRINT]');
  console.log(attestation.signature);
  console.log(`\nsignature validates final payload: ${verified ? 'YES' : 'NO'}`);
}

function printChaosReport(result: ChaosResult): void {
  console.log(rule());
  console.log('SIM-001  Chaos Load Test');
  console.log(`tick=${result.config.tickMs}ms  window=${result.config.windowMs}ms  ` +
    `seed-driven deterministic run`);
  console.log(rule());

  for (const phase of result.phases) {
    console.log(`\n${phase.label}  (${phase.fromSec}s -> ${phase.toSec}s)`);
    printState('  end state', phase.endState);
    console.log(
      `  peaks            panic=${phase.peakPanic.toFixed(1)}  ` +
        `velocity=${phase.peakVelocity.toFixed(0)}  backspace=${phase.peakBackspace}`,
    );
    for (const event of phase.directorEvents) {
      console.log(`  director         ceo_alert fired at tick ${event.tick}`);
    }
  }

  if (result.trapTick !== null) {
    printAlert();
  }

  printAttestation(result.attestation, result.verified);
  printChecks(result.checks);
}

/** The acceptance block every mode ends with. Any failure makes the exit code non zero. */
function printChecks(checks: readonly { label: string; ok: boolean; detail: string }[]): void {
  console.log('\n' + rule());
  console.log('Acceptance Checks');
  console.log(rule());
  let allOk = true;
  for (const check of checks) {
    const mark = check.ok ? 'PASS' : 'FAIL';
    if (!check.ok) allOk = false;
    console.log(`[${mark}] ${check.label}`);
    console.log(`       ${check.detail}`);
  }
  console.log(rule());
  console.log(allOk ? 'ALL CHECKS PASSED' : 'SOME CHECKS FAILED');

  if (!allOk) process.exitCode = 1;
}

async function runDrill(argv: string[]): Promise<void> {
  const shards = numberFlag(argv, 'shards', 4);
  const sessions = numberFlag(argv, 'sessions', 32);
  const killAfter = numberFlag(argv, 'kill-after', 0);

  console.log(rule());
  console.log('SIM-001  Crash Drill');
  console.log(`${shards} shards, ${sessions} sessions: SIGKILL the cluster twice, recover, verify`);
  console.log(rule());
  const result = await runCrashDrill({
    secret: SECRET,
    shards,
    sessions,
    ...(killAfter > 0 ? { killAfterRound: killAfter } : {}),
    log: (line) => console.log(`  ${line}`),
  });
  printChecks(result.checks);
}

async function runCluster(argv: string[]): Promise<void> {
  const shards = numberFlag(argv, 'shards', 4);
  const sessions = numberFlag(argv, 'sessions', 4000);
  const seconds = numberFlag(argv, 'seconds', 30);
  const killAt = numberFlag(argv, 'kill-at', 21);

  console.log(rule());
  console.log('SIM-001  Sharded Cluster  (real time)');
  console.log(
    `${sessions} sessions on ${shards} worker shards for ${seconds}s` +
      (killAt > 0 && killAt <= seconds ? `, shard 0 killed at ${killAt}s` : ''),
  );
  console.log(rule());
  const result = await runClusterDemo({
    secret: SECRET,
    shards,
    sessions,
    seconds,
    ...(killAt > 0 && killAt <= seconds ? { killAtSecond: killAt } : {}),
    log: (line) => console.log(line),
  });

  const { worst } = result;
  console.log(`\nWorst shard over the run (${result.sessionsPerShard} sessions per shard)`);
  console.log(`  tick work        ${(worst.busyRatio * 100).toFixed(1)}% of one core`);
  console.log(`  loop lag p99     ${worst.lagP99Ms.toFixed(1)}ms`);
  console.log(`  fsync p99        ${worst.fsyncP99Ms.toFixed(1)}ms`);
  console.log(
    `  checkpoint       ${(worst.checkpointBytes / 1024).toFixed(0)} KB, ` +
      `froze the loop for ${worst.checkpointFreezeMs.toFixed(1)}ms`,
  );
  console.log(`  alerts           ${result.alerts.delivered} delivered, ${result.alerts.duplicatesDropped} replayed duplicates dropped`);
  printChecks(result.checks);
}

async function runLive(seed: number): Promise<void> {
  const config = defaultConfig;
  const sim = new Simulation({ secret: SECRET, config });
  const rng = mulberry32(seed);
  const totalTicks = 300;

  console.log(rule());
  console.log('SIM-001  Live Run  (real time, ~30s)');
  console.log(rule());

  let lastLoggedSecond = -1;
  await runLiveLoop({
    sim,
    tickMs: config.tickMs,
    totalTicks,
    telemetry: (tick) => telemetryForTick(tick, config.tickMs, rng),
    onTick: (result) => {
      const second = Math.floor(result.simTimeMs / 1000);
      if (second !== lastLoggedSecond) {
        lastLoggedSecond = second;
        printState(`  t=${second}s`, result.state);
      }
      for (const event of result.events) {
        printAlert();
      }
    },
  });

  const attestation = sim.complete();
  printAttestation(attestation, verify(attestation, SECRET));
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const seed = numberFlag(argv, 'seed', 1);

  if (argv.includes('--live')) {
    await runLive(seed);
    return;
  }
  if (argv.includes('--crash-drill')) {
    await runDrill(argv);
    return;
  }
  if (argv.includes('--cluster')) {
    await runCluster(argv);
    return;
  }

  const result = runChaosLoadTest({
    secret: SECRET,
    seed,
    trace: argv.includes('--trace'),
    ...(argv.includes('--fixed-time') ? { completedAt: '2026-01-01T00:00:00.000Z' } : {}),
  });
  printChaosReport(result);
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
