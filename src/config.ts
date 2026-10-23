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

/**
 * Shard tunables. Unlike EngineConfig these never change what a session
 * computes, only how a shard runs it, so they may differ from one run of a
 * data directory to the next.
 */
export interface ShardConfig {
  /** Shard ticks between checkpoints. 50 ticks is five seconds at 100ms. */
  checkpointEveryTicks: number;
  /** Most events of one kind a session can land in one tick; the excess is dropped and counted. */
  inboxCapPerKind: number;
  /** Completed attestations a shard remembers, so a retried completion gets the same answer. */
  completedRetention: number;
  /**
   * Completed session ids remembered after their attestation is dropped, so
   * a reused id is refused instead of quietly starting a second session.
   * Ids are cheap, so this is far larger than completedRetention.
   */
  completedIdRetention: number;
  /** Most ticks a shard runs back to back while catching up after a stall. */
  maxCatchUpTicks: number;
}

export const defaultShardConfig: ShardConfig = {
  checkpointEveryTicks: 50,
  inboxCapPerKind: 255,
  completedRetention: 1000,
  completedIdRetention: 10_000,
  maxCatchUpTicks: 10,
};

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
