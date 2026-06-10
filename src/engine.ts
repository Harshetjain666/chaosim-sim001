import { defaultConfig, type EngineConfig } from './config.ts';
import { createWorldState } from './worldState.ts';
import { TelemetryWindow } from './telemetryWindow.ts';
import { computeMetrics, nextPanicIndex } from './stressModel.ts';
import { ScenarioDirector, type DirectorEvent } from './scenarioDirector.ts';
import type { TelemetryEvent, WorldState } from './types.ts';

export interface TickResult {
  tick: number;
  /** Logical simulation time at the start of this tick, in ms. */
  simTimeMs: number;
  /** Snapshot copy of the authoritative state after the tick resolved. */
  state: WorldState;
  /** Anything the Scenario Director emitted on this tick. */
  events: DirectorEvent[];
}

/**
 * The authoritative tick engine.
 *
 * One method matters: `step`. It is a pure function of (current state, this
 * tick's telemetry batch). Given the same starting state and the same batches
 * in the same order, it always produces the same trajectory, with no dependence
 * on wall-clock time. That property is what makes the simulation deterministic
 * and what lets the chaos test, the unit tests, and the live loop all share the
 * exact same logic.
 *
 * Per the spec, every tick: process telemetry, update metrics, evaluate world
 * conditions, trigger environment mutations. Nothing mutates the world except
 * this loop.
 */
export class SimulationEngine {
  readonly state: WorldState;
  readonly directorLog: DirectorEvent[] = [];

  private readonly config: EngineConfig;
  private readonly window: TelemetryWindow;
  private readonly director: ScenarioDirector;
  private tickCount = 0;

  constructor(config: EngineConfig = defaultConfig) {
    this.config = config;
    this.state = createWorldState();
    this.window = new TelemetryWindow(config.windowMs);
    this.director = new ScenarioDirector();
  }

  get tick(): number {
    return this.tickCount;
  }

  get isTrapTripped(): boolean {
    return this.director.isTrapTripped;
  }

  /** Advance exactly one fixed-size tick. */
  step(batch: readonly TelemetryEvent[] = []): TickResult {
    const simTimeMs = this.tickCount * this.config.tickMs;
    const windowEnd = simTimeMs + this.config.tickMs;

    // 1. Process telemetry: buffer this tick's raw events, drop stale ones.
    this.window.ingest(batch);
    this.window.evict(windowEnd);

    // 2. Update metrics from the sliding window.
    const metrics = computeMetrics(this.window, this.config);
    this.state.keystrokeVelocity = metrics.keystrokeVelocity;
    this.state.backspaceCount = metrics.backspaceCount;

    // 3. Update stress.
    this.state.panicIndex = nextPanicIndex(this.state, this.config);

    // 4. Evaluate world conditions and trigger environment mutations.
    const events = this.director.evaluate(this.state, this.tickCount, this.config);
    if (events.length > 0) {
      this.directorLog.push(...events);
    }

    const result: TickResult = {
      tick: this.tickCount,
      simTimeMs,
      state: { ...this.state },
      events,
    };
    this.tickCount += 1;
    return result;
  }
}
