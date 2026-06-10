import { defaultConfig } from './config.ts';
import { runChaosLoadTest, type ChaosResult } from './chaosLoadTest.ts';
import { Simulation } from './simulation.ts';
import { verify, type Attestation } from './attestation.ts';
import { mulberry32 } from './rng.ts';
import { telemetryForTick } from './scenario.ts';
import { runLiveLoop } from './liveLoop.ts';
import { CEO_ALERT_MESSAGE } from './scenarioDirector.ts';
import type { WorldState } from './types.ts';

const SECRET = process.env.CHAOSIM_SECRET ?? 'sim-001-dev-secret';

function parseSeed(argv: string[]): number {
  const flag = argv.find((a) => a.startsWith('--seed='));
  if (!flag) return 1;
  const value = Number(flag.split('=')[1]);
  return Number.isFinite(value) ? value : 1;
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

  console.log('\n' + rule());
  console.log('Acceptance Checks');
  console.log(rule());
  let allOk = true;
  for (const check of result.checks) {
    const mark = check.ok ? 'PASS' : 'FAIL';
    if (!check.ok) allOk = false;
    console.log(`[${mark}] ${check.label}`);
    console.log(`       ${check.detail}`);
  }
  console.log(rule());
  console.log(allOk ? 'ALL CHECKS PASSED' : 'SOME CHECKS FAILED');

  if (!allOk) process.exitCode = 1;
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
  const seed = parseSeed(argv);

  if (argv.includes('--live')) {
    await runLive(seed);
    return;
  }

  const result = runChaosLoadTest({
    secret: SECRET,
    seed,
    trace: argv.includes('--trace'),
    completedAt: argv.includes('--fixed-time') ? '2026-01-01T00:00:00.000Z' : undefined,
  });
  printChaosReport(result);
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
