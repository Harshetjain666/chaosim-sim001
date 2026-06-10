import type { WorldState } from './types.ts';

/**
 * Build a fresh authoritative world. Initial values are fixed by the spec:
 *   panicIndex = 0, codebaseHealth = 100, ceoAnxietyIndex = 10.
 * The two telemetry-derived metrics start at 0 because no events have landed.
 */
export function createWorldState(): WorldState {
  return {
    keystrokeVelocity: 0,
    backspaceCount: 0,
    panicIndex: 0,
    codebaseHealth: 100,
    ceoAnxietyIndex: 10,
  };
}
