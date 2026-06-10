import { defaultConfig, type EngineConfig } from './config.ts';
import { SimulationEngine, type TickResult } from './engine.ts';
import { sign, type Attestation, type AttestationPayload } from './attestation.ts';
import type { TelemetryEvent, WorldState } from './types.ts';

export interface SimulationOptions {
  secret: string;
  config?: EngineConfig;
}

/**
 * Thin façade over the engine that owns the lifecycle: stepping while the sim
 * runs, then a one-way `complete()` that freezes the world and produces the
 * signed attestation. Keeping completion here (rather than in the engine) keeps
 * the engine focused purely on the tick math.
 */
export class Simulation {
  readonly engine: SimulationEngine;

  private readonly secret: string;
  private completed = false;

  constructor(options: SimulationOptions) {
    this.secret = options.secret;
    this.engine = new SimulationEngine(options.config ?? defaultConfig);
  }

  step(batch: readonly TelemetryEvent[] = []): TickResult {
    if (this.completed) {
      throw new Error('Simulation already completed; no further ticks allowed.');
    }
    return this.engine.step(batch);
  }

  get state(): WorldState {
    return this.engine.state;
  }

  /**
   * Freeze the final state, attach a timestamp, and sign it. `completedAt` can
   * be injected so a reproducible run yields a reproducible fingerprint; left
   * out, it stamps the real completion time.
   */
  complete(completedAt: string = new Date().toISOString()): Attestation {
    this.completed = true;

    const finalState: WorldState = { ...this.engine.state };
    Object.freeze(finalState);

    const payload: AttestationPayload = {
      schema: 'chaosim.sim001.attestation.v1',
      finalState,
      tickCount: this.engine.tick,
      trapTripped: this.engine.isTrapTripped,
      directorEvents: this.engine.directorLog.map((event) => ({ ...event })),
      completedAt,
    };

    return sign(payload, this.secret);
  }
}
