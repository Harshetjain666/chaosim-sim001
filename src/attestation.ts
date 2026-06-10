import { createHmac, timingSafeEqual } from 'node:crypto';
import type { DirectorEvent } from './scenarioDirector.ts';
import type { WorldState } from './types.ts';

export interface AttestationPayload {
  schema: 'chaosim.sim001.attestation.v1';
  finalState: WorldState;
  tickCount: number;
  trapTripped: boolean;
  directorEvents: DirectorEvent[];
  /** ISO-8601 completion timestamp, part of the signed body. */
  completedAt: string;
}

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
 * to the final state, the tick count, the alert log, or the timestamp produces
 * a different signature and fails the check.
 */
export function verify(attestation: Attestation, secret: string): boolean {
  const expected = createHmac('sha256', secret)
    .update(canonicalize(attestation.payload))
    .digest();
  const provided = Buffer.from(attestation.signature, 'hex');
  return expected.length === provided.length && timingSafeEqual(expected, provided);
}
