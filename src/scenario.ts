import type { TelemetryEvent, TelemetryKind } from './types.ts';

/**
 * Synthetic telemetry for the three-phase chaos load test.
 *
 * The real system reads these events off a WebSocket. Here we generate them so
 * the run is self-contained and reproducible. Rates are expressed as expected
 * events per 100ms tick and realized with stochastic rounding against a seeded
 * RNG, so fractional rates average out correctly across the metric window while
 * still looking like an uneven human typing stream.
 *
 * The phases are shaped to exercise the spec's expected outcomes:
 *   Phase 1 (0-10s):  calm baseline, stress curve dormant.
 *   Phase 2 (10-20s): friction ramps in, panic climbs but stays under the trap.
 *   Phase 3 (20-30s): sustained failure cascade, panic crosses 75 and trips.
 */

interface Rates {
  keystroke: number;
  backspace: number;
  fileSave: number;
}

const PHASE1: Rates = { keystroke: 0.09, backspace: 0.02, fileSave: 0.0 };
const PHASE3: Rates = { keystroke: 0.45, backspace: 1.45, fileSave: 0.25 };

/**
 * Phase 2 ramps from calm to near-cascade across its ten seconds. Velocity
 * climbs early (fast, frustrated typing), but the backspace curve is held back
 * with a cubic so the AND in the stress rule only latches in the final couple
 * of seconds. That keeps panic rising late in the phase without crossing the
 * trap threshold until the Phase 3 cascade sustains the pressure.
 */
function phase2Rates(tick: number): Rates {
  const span = (tick - 100) / 100; // 0 at 10s, 1 approaching 20s
  return {
    keystroke: 0.1 + 0.34 * span,
    backspace: 0.05 + 0.95 * span ** 3,
    fileSave: 0.05 + 0.15 * span,
  };
}

export function ratesForTick(tick: number): Rates {
  if (tick < 100) return PHASE1;
  if (tick < 200) return phase2Rates(tick);
  return PHASE3;
}

function emit(
  out: TelemetryEvent[],
  kind: TelemetryKind,
  rate: number,
  tickStartMs: number,
  tickMs: number,
  rng: () => number,
): void {
  let count = Math.floor(rate);
  if (rng() < rate - count) count += 1;
  for (let i = 0; i < count; i += 1) {
    out.push({ t: tickStartMs + Math.floor(rng() * tickMs), kind });
  }
}

/** Build one tick's worth of raw telemetry events. */
export function telemetryForTick(
  tick: number,
  tickMs: number,
  rng: () => number,
): TelemetryEvent[] {
  const rates = ratesForTick(tick);
  const tickStartMs = tick * tickMs;
  const batch: TelemetryEvent[] = [];
  emit(batch, 'keystroke', rates.keystroke, tickStartMs, tickMs, rng);
  emit(batch, 'backspace', rates.backspace, tickStartMs, tickMs, rng);
  emit(batch, 'fileSave', rates.fileSave, tickStartMs, tickMs, rng);
  return batch;
}
