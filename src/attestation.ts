import { createHmac, timingSafeEqual } from 'node:crypto';
import type { DirectorEvent } from './scenarioDirector.ts';
import type { WorldState } from './types.ts';

interface AttestationBody {
  finalState: WorldState;
  tickCount: number;
  trapTripped: boolean;
  directorEvents: DirectorEvent[];
  /** ISO-8601 completion timestamp, part of the signed body. */
  completedAt: string;
}

/**
 * A standalone run's payload. It names no session, which is unambiguous only
 * while one process runs one session, and it is what the single session
 * regression fingerprint signs.
 */
export interface AttestationPayloadV1 extends AttestationBody {
  schema: 'chaosim.sim001.attestation.v1';
}

/**
 * A payload from a system that runs many sessions, as the cluster does. The
 * session id is signed with the rest, so one session's attestation cannot be
 * presented as another's.
 */
export interface AttestationPayloadV2 extends AttestationBody {
  schema: 'chaosim.sim001.attestation.v2';
  sessionId: string;
}

export type AttestationPayload = AttestationPayloadV1 | AttestationPayloadV2;

export interface Attestation {
  payload: AttestationPayload;
  /** The exact bytes that were signed, kept so a verifier never has to guess. */
  canonical: string;
  algorithm: 'HMAC-SHA256';
  signature: string;
}

/**
 * Canonical JSON: keys sorted at every level, no incidental whitespace.
 *
 * JSON.stringify preserves insertion order, so two payloads that are equal as
 * objects can serialize to different bytes and therefore different signatures.
 * That would make verification depend on field ordering, which is exactly the
 * kind of soft spot an attestation layer must not have. Sorting keys gives one
 * unambiguous byte string per logical payload.
 */
export function canonicalize(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value) ?? 'null';
  }
  if (Array.isArray(value)) {
    return '[' + value.map(canonicalize).join(',') + ']';
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  const body = keys
    .map((key) => JSON.stringify(key) + ':' + canonicalize(obj[key]))
    .join(',');
  return '{' + body + '}';
}

/** Freeze -> serialize -> sign. The output fingerprint commits to the payload. */
export function sign(payload: AttestationPayload, secret: string): Attestation {
  const canonical = canonicalize(payload);
  const signature = createHmac('sha256', secret).update(canonical).digest('hex');
  return { payload, canonical, algorithm: 'HMAC-SHA256', signature };
}

/**
 * Recompute the HMAC over the payload and compare in constant time. Any change
 * to the final state, the tick count, the alert log, the timestamp, or the
 * session id produces a different signature and fails the check.
 *
 * Schemas v1 and v2 verify and nothing else does, and a v2 payload must carry
 * its session id. A caller that knows which session the attestation should
 * belong to passes `sessionId`: then only a v2 payload naming that session
 * verifies, because a v1 payload names none and could be any session's.
 */
export function verify(
  attestation: Attestation,
  secret: string,
  expected: { sessionId?: string } = {},
): boolean {
  const { payload } = attestation;
  switch (payload.schema) {
    case 'chaosim.sim001.attestation.v1':
      if (expected.sessionId !== undefined) return false;
      break;
    case 'chaosim.sim001.attestation.v2':
      if (typeof payload.sessionId !== 'string') return false;
      if (expected.sessionId !== undefined && payload.sessionId !== expected.sessionId) return false;
      break;
    default:
      return false;
  }
  const mac = createHmac('sha256', secret).update(canonicalize(payload)).digest();
  const provided = Buffer.from(attestation.signature, 'hex');
  return mac.length === provided.length && timingSafeEqual(mac, provided);
}
