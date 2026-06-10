import { defaultConfig, type EngineConfig } from './config.ts';
import { Simulation } from './simulation.ts';
import { verify, type Attestation } from './attestation.ts';
import { mulberry32 } from './rng.ts';
import { telemetryForTick } from './scenario.ts';
import type { DirectorEvent } from './scenarioDirector.ts';
import type { WorldState } from './types.ts';

export interface ChaosOptions {
  secret: string;
  seed?: number;
  config?: EngineConfig;
  /** Fixed completion timestamp for a reproducible fingerprint. */
  completedAt?: string;
  /** Print a per-second metrics table while running. */
  trace?: boolean;
}

interface PhaseSummary {
  label: string;
  fromSec: number;
  toSec: number;
  endState: WorldState;
  peakPanic: number;
  peakVelocity: number;
  peakBackspace: number;
  directorEvents: DirectorEvent[];
}

export interface ChaosResult {
  config: EngineConfig;
  phases: PhaseSummary[];
  trapTick: number | null;
  attestation: Attestation;
  verified: boolean;
  checks: { label: string; ok: boolean; detail: string }[];
}

const TOTAL_TICKS = 300;

interface PhaseWindow {
  label: string;
  startTick: number;
  endTick: number;
}

const PHASE_WINDOWS: PhaseWindow[] = [
  { label: 'Phase 1  baseline', startTick: 0, endTick: 99 },
  { label: 'Phase 2  friction', startTick: 100, endTick: 199 },
  { label: 'Phase 3  cascade', startTick: 200, endTick: 299 },
];

export function runChaosLoadTest(options: ChaosOptions): ChaosResult {
  const config = options.config ?? defaultConfig;
  const rng = mulberry32(options.seed ?? 1);
  const sim = new Simulation({ secret: options.secret, config });

  const summaries: PhaseSummary[] = PHASE_WINDOWS.map((phase) => ({
    label: phase.label,
    fromSec: (phase.startTick * config.tickMs) / 1000,
    toSec: ((phase.endTick + 1) * config.tickMs) / 1000,
    endState: sim.state,
    peakPanic: 0,
    peakVelocity: 0,
    peakBackspace: 0,
    directorEvents: [],
  }));

  let trapTick: number | null = null;

  for (let tick = 0; tick < TOTAL_TICKS; tick += 1) {
    const batch = telemetryForTick(tick, config.tickMs, rng);
    const result = sim.step(batch);

    const phaseIndex = PHASE_WINDOWS.findIndex(
      (p) => tick >= p.startTick && tick <= p.endTick,
    );
    const summary = summaries[phaseIndex]!;
    summary.endState = result.state;
    summary.peakPanic = Math.max(summary.peakPanic, result.state.panicIndex);
    summary.peakVelocity = Math.max(summary.peakVelocity, result.state.keystrokeVelocity);
    summary.peakBackspace = Math.max(summary.peakBackspace, result.state.backspaceCount);
    if (result.events.length > 0) {
      summary.directorEvents.push(...result.events);
      if (trapTick === null) trapTick = result.tick;
    }

    if (options.trace && tick % 10 === 9) {
      const s = result.state;
      process.stdout.write(
        `  t=${String(result.simTimeMs / 1000).padStart(4)}s ` +
          `vel=${s.keystrokeVelocity.toFixed(0).padStart(4)} ` +
          `bks=${String(s.backspaceCount).padStart(3)} ` +
          `panic=${s.panicIndex.toFixed(1).padStart(6)} ` +
          `health=${String(s.codebaseHealth).padStart(3)} ` +
          `ceo=${String(s.ceoAnxietyIndex).padStart(3)}\n`,
      );
    }
  }

  const attestation = sim.complete(options.completedAt);
  const verified = verify(attestation, options.secret);

  const checks = buildChecks(summaries, trapTick, attestation, verified, config);

  return { config, phases: summaries, trapTick, attestation, verified, checks };
}

function buildChecks(
  phases: PhaseSummary[],
  trapTick: number | null,
  attestation: Attestation,
  verified: boolean,
  config: EngineConfig,
): ChaosResult['checks'] {
  const p1 = phases[0]!;
  const p2 = phases[1]!;
  const final = attestation.payload.finalState;
  const totalAlerts = phases.reduce((n, p) => n + p.directorEvents.length, 0);

  return [
    {
      label: 'Phase 1 stays calm (no director action, health intact)',
      ok: p1.directorEvents.length === 0 && p1.endState.codebaseHealth === 100,
      detail: `health=${p1.endState.codebaseHealth}, alerts=${p1.directorEvents.length}`,
    },
    {
      label: 'Phase 1 panic effectively zero',
      ok: p1.peakPanic < 1,
      detail: `peakPanic=${p1.peakPanic.toFixed(2)}`,
    },
    {
      label: 'Phase 2 panic increases but stays below the trap threshold',
      ok: p2.endState.panicIndex > p1.peakPanic && p2.endState.panicIndex < config.trapPanicThreshold,
      detail: `endPanic=${p2.endState.panicIndex.toFixed(2)} (threshold ${config.trapPanicThreshold})`,
    },
    {
      label: 'Phase 2 director has not fired yet',
      ok: p2.directorEvents.length === 0,
      detail: `alerts=${p2.directorEvents.length}`,
    },
    {
      label: 'Trap trips during Phase 3',
      ok: trapTick !== null && trapTick >= 200 && trapTick <= 299,
      detail: `trapTick=${trapTick}`,
    },
    {
      label: 'CEO alert emitted exactly once',
      ok: totalAlerts === 1,
      detail: `alerts=${totalAlerts}`,
    },
    {
      label: 'Codebase health degraded by exactly 30',
      ok: final.codebaseHealth === 100 - config.codebaseHealthPenalty,
      detail: `health=${final.codebaseHealth}`,
    },
    {
      label: 'CEO anxiety pinned to 95',
      ok: final.ceoAnxietyIndex === config.ceoAnxietyOnTrap,
      detail: `ceoAnxietyIndex=${final.ceoAnxietyIndex}`,
    },
    {
      label: 'Attestation signature validates the final payload',
      ok: verified,
      detail: `algorithm=${attestation.algorithm}`,
    },
  ];
}
