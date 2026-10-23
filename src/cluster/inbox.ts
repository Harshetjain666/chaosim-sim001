import { TELEMETRY_KINDS, type TelemetryEvent, type TelemetryKind } from '../types.ts';

const KIND_COUNT = TELEMETRY_KINDS.length;

/** A kind's compact code: its index in TELEMETRY_KINDS. */
export function kindCode(kind: TelemetryKind): number {
  return TELEMETRY_KINDS.indexOf(kind);
}

/**
 * One session's bounded inbox between two ticks.
 *
 * The engine only ever counts the events inside its window, so the inbox
 * keeps counts rather than event objects. A burst is absorbed as increments,
 * memory stays constant per session however hard a client floods, and each
 * kind is capped per tick. The tick's batch is rebuilt from the counts in a
 * fixed order, which is what lets a replayed tick hand the engine exactly the
 * batch the live tick did.
 */
export class Inbox {
  private readonly cap: number;
  private counts: number[] = new Array<number>(KIND_COUNT).fill(0);
  private empty = true;

  constructor(capPerKind: number) {
    this.cap = capPerKind;
  }

  /** Count `count` events of one kind. Returns how many fit under the cap. */
  add(code: number, count: number): number {
    const current = this.counts[code]!;
    const accepted = Math.max(0, Math.min(count, this.cap - current));
    if (accepted > 0) {
      this.counts[code] = current + accepted;
      this.empty = false;
    }
    return accepted;
  }

  /** Take this tick's counts, or null when nothing arrived. */
  drain(): number[] | null {
    if (this.empty) return null;
    const counts = this.counts;
    this.counts = new Array<number>(KIND_COUNT).fill(0);
    this.empty = true;
    return counts;
  }
}

/**
 * Rebuild a tick's batch from inbox counts, every event stamped at `t`.
 *
 * Stamping the whole tick at one instant is exact, not an approximation: the
 * sliding window evicts all of a tick's events on the same later tick wherever
 * inside the tick they landed.
 */
export function expandBatch(counts: readonly number[], t: number): TelemetryEvent[] {
  const batch: TelemetryEvent[] = [];
  for (let code = 0; code < KIND_COUNT; code += 1) {
    const kind = TELEMETRY_KINDS[code]!;
    const count = counts[code] ?? 0;
    for (let i = 0; i < count; i += 1) batch.push({ t, kind });
  }
  return batch;
}
