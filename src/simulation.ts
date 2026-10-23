import { defaultConfig, type EngineConfig } from './config.ts';
import { SimulationEngine, type EngineSnapshot, type TickResult } from './engine.ts';
import { sign, type Attestation, type AttestationPayload } from './attestation.ts';
import type { TelemetryEvent, WorldState } from './types.ts';

export interface SimulationOptions {
  secret: string;
  config?: EngineConfig;
  /** Resume from a frozen engine instead of the spec's initial world (crash recovery). */
  snapshot?: EngineSnapshot;
  /**
   * The session this simulation runs as, in a system that runs many. Given,
   * the attestation is schema v2 and signs it. A standalone run leaves it out
   * and signs v1, so the single session fingerprint stays where it is.
   */
  sessionId?: string;
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
  private readonly sessionId: string | undefined;
  private completed = false;

  constructor(options: SimulationOptions) {
    this.secret = options.secret;
    this.sessionId = options.sessionId;
    const config = options.config ?? defaultConfig;
    this.engine = options.snapshot
      ? SimulationEngine.restore(options.snapshot, config)
      : new SimulationEngine(config);
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
   * Freeze the final state, attach a timestamp, and sign it, naming the
   * session when there is one. `completedAt` can be injected so a
   * reproducible run yields a reproducible fingerprint; left out, it stamps
   * the real completion time.
   */
  complete(completedAt: string = new Date().toISOString()): Attestation {
    this.completed = true;

    const finalState: WorldState = { ...this.engine.state };
    Object.freeze(finalState);

    const body = {
      finalState,
      tickCount: this.engine.tick,
      trapTripped: this.engine.isTrapTripped,
      directorEvents: this.engine.directorLog.map((event) => ({ ...event })),
      completedAt,
    };
    const payload: AttestationPayload =
      this.sessionId === undefined
        ? { schema: 'chaosim.sim001.attestation.v1', ...body }
        : { schema: 'chaosim.sim001.attestation.v2', sessionId: this.sessionId, ...body };

    return sign(payload, this.secret);
  }
}
