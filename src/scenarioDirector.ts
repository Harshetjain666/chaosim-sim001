import type { EngineConfig } from './config.ts';
import type { WorldState } from './types.ts';

export const CEO_ALERT_MESSAGE =
  'Production is down. Why are your logs showing 80% syntax errors? Explain immediately.';

export interface DirectorEvent {
  type: 'ceo_alert';
  tick: number;
  message: string;
}

/**
 * The Adaptive Scenario Director.
 *
 * It watches the authoritative world and injects an environment mutation when
 * conditions deteriorate past the trap threshold. The `isTrapTripped` flag is
 * what guarantees the cascade fires exactly once: once tripped, every later
 * evaluation short-circuits. State is mutated in place here because the director
 * is the only component (besides the stress curve) allowed to author world
 * changes, and routing every mutation through it keeps that authority in one
 * obvious place.
 */
export class ScenarioDirector {
  isTrapTripped = false;

  evaluate(state: WorldState, tick: number, config: EngineConfig): DirectorEvent[] {
    if (this.isTrapTripped) return [];
    if (state.panicIndex < config.trapPanicThreshold) return [];

    this.isTrapTripped = true;
    state.codebaseHealth = Math.max(0, state.codebaseHealth - config.codebaseHealthPenalty);
    state.ceoAnxietyIndex = config.ceoAnxietyOnTrap;

    return [ceoAlert(tick)];
  }
}

/** The event the trap emits on the tick it trips. */
export function ceoAlert(tick: number): DirectorEvent {
  return { type: 'ceo_alert', tick, message: CEO_ALERT_MESSAGE };
}
