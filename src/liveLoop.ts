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

export interface FixedStepOptions {
  tickMs: number;
  /**
   * Most ticks one pump may release. The rest stay in the accumulator, so a
   * long stall catches up over a few pumps instead of freezing the thread.
   */
  maxTicksPerPump?: number;
  onTick: () => void;
}

export interface FixedStepHandle {
  stop(): void;
  /** Real time already earned but not yet released as ticks, in ms. */
  readonly backlogMs: number;
}

/** Split accumulated real time into whole ticks, never more than `maxTicks`. */
export function releaseTicks(
  accumulatorMs: number,
  tickMs: number,
  maxTicks: number,
): { ticks: number; accumulatorMs: number } {
  const ticks = Math.min(Math.floor(accumulatorMs / tickMs), maxTicks);
  return { ticks, accumulatorMs: accumulatorMs - ticks * tickMs };
}

/**
 * Fixed-timestep clock.
 *
 * A naive `setInterval(step, 100)` accumulates drift: the timer is never exactly
 * 100ms, and any slow tick pushes every later tick later still. Instead we poll
 * faster than the tick rate and release ticks off an accumulator of real
 * elapsed time. The world clock therefore stays pinned to tick boundaries: if
 * a pump runs late, the accumulator simply releases several fixed ticks to
 * catch up, and no individual session's clock skews relative to the others.
 *
 * The single session live run and every shard's loop share this clock.
 */
export function startFixedStep(options: FixedStepOptions): FixedStepHandle {
  const { tickMs, onTick } = options;
  const maxTicks = options.maxTicksPerPump ?? Infinity;
  let last = process.hrtime.bigint();
  let accumulatorMs = 0;
  let stopped = false;

  const handle = setInterval(() => {
    const now = process.hrtime.bigint();
    accumulatorMs += Number(now - last) / 1_000_000;
    last = now;

    const release = releaseTicks(accumulatorMs, tickMs, maxTicks);
    accumulatorMs = release.accumulatorMs;
    for (let i = 0; i < release.ticks && !stopped; i += 1) {
      onTick();
    }
  }, Math.max(1, Math.floor(tickMs / 2)));

  return {
    stop() {
      stopped = true;
      clearInterval(handle);
    },
    get backlogMs() {
      return accumulatorMs;
    },
  };
}

/**
 * Drive one simulation in real time for `totalTicks` ticks.
 *
 * Crucially the per-tick math is identical to the deterministic path, so live
 * mode and the chaos test exercise exactly the same engine.
 */
export function runLiveLoop(options: LiveLoopOptions): Promise<void> {
  const { sim, tickMs, totalTicks, telemetry, onTick } = options;

  return new Promise((resolve) => {
    if (totalTicks <= 0) {
      resolve();
      return;
    }

    let tick = 0;
    const clock = startFixedStep({
      tickMs,
      onTick: () => {
        const result = sim.step(telemetry(tick));
        onTick?.(result);
        tick += 1;
        if (tick >= totalTicks) {
          clock.stop();
          resolve();
        }
      },
    });
  });
}
