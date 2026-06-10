import type { Simulation } from './simulation.ts';
import type { TickResult } from './engine.ts';
import type { TelemetryEvent } from './types.ts';

export interface LiveLoopOptions {
  sim: Simulation;
  tickMs: number;
  totalTicks: number;
  telemetry: (tick: number) => readonly TelemetryEvent[];
  onTick?: (result: TickResult) => void;
}

/**
 * Fixed-timestep driver for running the engine in real time.
 *
 * A naive `setInterval(step, 100)` accumulates drift: the timer is never exactly
 * 100ms, and any slow tick pushes every later tick later still. Instead we poll
 * faster than the tick rate and advance the simulation off an accumulator of
 * real elapsed time. The world clock therefore stays pinned to tick boundaries:
 * if a pump runs late, the accumulator simply releases several fixed ticks to
 * catch up, and no individual session's clock skews relative to the others.
 *
 * Crucially the per-tick math is identical to the deterministic path, so live
 * mode and the chaos test exercise exactly the same engine.
 */
export function runLiveLoop(options: LiveLoopOptions): Promise<void> {
  const { sim, tickMs, totalTicks, telemetry, onTick } = options;

  return new Promise((resolve) => {
    let last = process.hrtime.bigint();
    let accumulatorMs = 0;
    let tick = 0;

    const handle = setInterval(() => {
      const now = process.hrtime.bigint();
      accumulatorMs += Number(now - last) / 1_000_000;
      last = now;

      while (accumulatorMs >= tickMs && tick < totalTicks) {
        const result = sim.step(telemetry(tick));
        onTick?.(result);
        tick += 1;
        accumulatorMs -= tickMs;
      }

      if (tick >= totalTicks) {
        clearInterval(handle);
        resolve();
      }
    }, Math.max(1, Math.floor(tickMs / 2)));
  });
}
