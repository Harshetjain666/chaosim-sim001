/**
 * Shared domain types for the SIM-001 engine.
 *
 * Telemetry is the only thing the outside world (the candidate IDE) is allowed
 * to push in. Everything downstream is derived by the authoritative tick engine,
 * never by the client.
 */

export type TelemetryKind =
  | 'keystroke'
  | 'backspace'
  | 'shell'
  | 'fileSave'
  | 'windowBlur';

export interface TelemetryEvent {
  /** Monotonic simulation timestamp in milliseconds since session start. */
  t: number;
  kind: TelemetryKind;
}

/**
 * The authoritative world. Field names and the documented initial values come
 * straight from the SIM-001 spec, so this is the single source of truth that
 * the attestation layer eventually signs.
 */
export interface WorldState {
  keystrokeVelocity: number;
  backspaceCount: number;
  panicIndex: number;
  codebaseHealth: number;
  ceoAnxietyIndex: number;
}
