/**
 * Engine tunables in one place. Keeping these as data (rather than scattering
 * magic numbers through the loop) means the stress model, the trap threshold,
 * and the metric window can all be tuned or A/B tested without touching logic.
 */
export interface EngineConfig {
  /** Fixed simulation step. The spec mandates 100ms. */
  tickMs: number;
  /** Sliding window over which velocity and backspace counts are measured. */
  windowMs: number;

  /** panicIndex gain per tick while the operator is in the stressed regime. */
  panicGainPerTick: number;
  /** Multiplicative decay applied to panicIndex when not stressed (toward 0). */
  panicDecayFactor: number;
  /** panicIndex is clamped to this ceiling so a long cascade stays bounded. */
  panicCeil: number;

  /** Stress curve inputs, taken verbatim from the spec. */
  backspaceThreshold: number;
  velocityThreshold: number;

  /** Adaptive Scenario Director trap. */
  trapPanicThreshold: number;
  codebaseHealthPenalty: number;
  ceoAnxietyOnTrap: number;
}

export const defaultConfig: EngineConfig = {
  tickMs: 100,
  windowMs: 2000,

  panicGainPerTick: 2.5,
  panicDecayFactor: 0.92,
  panicCeil: 100,

  backspaceThreshold: 10,
  velocityThreshold: 120,

  trapPanicThreshold: 75,
  codebaseHealthPenalty: 30,
  ceoAnxietyOnTrap: 95,
};
