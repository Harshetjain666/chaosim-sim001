import type { TelemetryEvent, TelemetryKind } from './types.ts';

/**
 * Per-session sliding window of raw telemetry.
 *
 * The IDE fires sub-second events (keystrokes, backspaces, saves, blurs). We do
 * NOT touch a database on every one of them. Instead each event lands in this
 * bounded in-memory window, and the tick engine summarizes it once per tick.
 * Anything older than `windowMs` is evicted so memory stays flat regardless of
 * how long the session runs.
 *
 * The backing store is a plain array used as a FIFO, which is cheap for one
 * session with a 2s window. A shard holds thousands of sessions and keeps the
 * same window as a ring of per tick counts in typed arrays
 * (src/cluster/sessionTable.ts). This readable version is the reference the
 * ring is tested against.
 */
export class TelemetryWindow {
  private readonly windowMs: number;
  private events: TelemetryEvent[] = [];

  constructor(windowMs: number) {
    this.windowMs = windowMs;
  }

  ingest(batch: readonly TelemetryEvent[]): void {
    for (const event of batch) {
      this.events.push(event);
    }
  }

  /** Drop everything that fell out of the trailing window ending at `now`. */
  evict(now: number): void {
    const cutoff = now - this.windowMs;
    let drop = 0;
    while (drop < this.events.length && this.events[drop]!.t < cutoff) {
      drop += 1;
    }
    if (drop > 0) {
      this.events.splice(0, drop);
    }
  }

  countByKind(kind: TelemetryKind): number {
    let n = 0;
    for (const event of this.events) {
      if (event.kind === kind) n += 1;
    }
    return n;
  }

  size(): number {
    return this.events.length;
  }

  /** Copy of the buffered events, oldest first, for engine snapshots. */
  toArray(): TelemetryEvent[] {
    return this.events.map((event) => ({ t: event.t, kind: event.kind }));
  }

  /** Replace the contents with a snapshot's events (oldest first). */
  load(events: readonly TelemetryEvent[]): void {
    this.events = events.map((event) => ({ t: event.t, kind: event.kind }));
  }
}
