import type { EngineConfig } from './config.ts';
import type { WorldState } from './types.ts';
import type { TelemetryWindow } from './telemetryWindow.ts';

export interface Metrics {
  keystrokeVelocity: number;
  backspaceCount: number;
}

/**
 * Summarize the raw window into the two authoritative metrics.
 *
 * keystrokeVelocity is reported per minute so the spec's threshold of 120 reads
 * naturally as "120 keystrokes a minute". backspaceCount is the raw number of
 * backspaces still inside the window, which is what the stress rule compares
 * against. Both are recomputed every tick, so they rise and fall with recent
 * behavior rather than accumulating forever.
 */
export function computeMetrics(window: TelemetryWindow, config: EngineConfig): Metrics {
  const perMinute = 60_000 / config.windowMs;
  return {
    keystrokeVelocity: window.countByKind('keystroke') * perMinute,
    backspaceCount: window.countByKind('backspace'),
  };
}

/** True when the operator is in the high-stress regime defined by the spec. */
export function isStressed(state: WorldState, config: EngineConfig): boolean {
  return (
    state.backspaceCount > config.backspaceThreshold &&
    state.keystrokeVelocity > config.velocityThreshold
  );
}

/**
 * The stress curve.
 *
 *   if backspaceCount > 10 AND keystrokeVelocity > 120  -> panicIndex += 2.5
 *   otherwise                                           -> decay toward zero
 *
 * Decay is multiplicative so panic eases off quickly at first and then tapers,
 * which feels more like a human calming down than a flat linear drop. Values
 * are clamped to [0, panicCeil].
 */
export function nextPanicIndex(state: WorldState, config: EngineConfig): number {
  if (isStressed(state, config)) {
    return Math.min(config.panicCeil, state.panicIndex + config.panicGainPerTick);
  }
  const decayed = state.panicIndex * config.panicDecayFactor;
  return decayed < 0.01 ? 0 : decayed;
}
