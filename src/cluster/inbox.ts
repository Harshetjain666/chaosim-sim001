import { TELEMETRY_KINDS, type TelemetryEvent, type TelemetryKind } from '../types.ts';

const KIND_COUNT = TELEMETRY_KINDS.length;

/** A kind's compact code: its index in TELEMETRY_KINDS. */
export function kindCode(kind: TelemetryKind): number {
  return TELEMETRY_KINDS.indexOf(kind);
}

/**
 * The batch a tick's inbox counts stand for, every event stamped at `t`.
 *
 * A session's inbox between two ticks is one counter per kind (it lives in
 * the shard's session columns, src/cluster/sessionTable.ts). The engine only
 * ever counts the events inside its window, so counts lose nothing: a burst
 * is absorbed as increments, memory stays constant per session however hard
 * a client floods, and each kind is capped per tick. Rebuilding the batch
 * from the counts in a fixed kind order is what lets a replayed tick hand
 * the engine exactly the batch the live tick did.
 *
 * Stamping the whole tick at one instant is exact, not an approximation: the
 * sliding window evicts all of a tick's events on the same later tick wherever
 * inside the tick they landed.
 */
export function expandBatch(counts: ArrayLike<number>, t: number): TelemetryEvent[] {
  const batch: TelemetryEvent[] = [];
  for (let code = 0; code < KIND_COUNT; code += 1) {
    const kind = TELEMETRY_KINDS[code]!;
    const count = counts[code] ?? 0;
    for (let i = 0; i < count; i += 1) batch.push({ t, kind });
  }
  return batch;
}
