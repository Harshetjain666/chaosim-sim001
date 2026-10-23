import { defaultConfig, type EngineConfig } from './config.ts';
import { SimulationEngine, type EngineSnapshot, type TickResult } from './engine.ts';
import { sign, type Attestation, type AttestationPayload } from './attestation.ts';
import type { TelemetryEvent, WorldState } from './types.ts';

export interface SimulationOptions {
  secret: string;
  config?: EngineConfig;
  /** Resume from a frozen engine instead of the spec's initial world (crash recovery). */
  snapshot?: EngineSnapshot;
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
   * Freeze the final state, attach a timestamp, and sign it. `completedAt` can
   * be injected so a reproducible run yields a reproducible fingerprint; left
   * out, it stamps the real completion time.
   */
  complete(completedAt: string = new Date().toISOString()): Attestation {
    this.completed = true;
    const { engine } = this;
    return attestCompletion(
      { tick: engine.tick, state: engine.state, trapTripped: engine.isTrapTripped, directorLog: engine.directorLog },
      completedAt,
      this.secret,
    );
  }
}

/**
 * What `complete()` signs, for anything that holds a session's final state.
 * The shard keeps its sessions as columns rather than engines and signs
 * through this, so a sharded attestation is byte for byte the one a
 * Simulation would produce.
 */
export function attestCompletion(
  session: Omit<EngineSnapshot, 'window'>,
  completedAt: string,
  secret: string,
): Attestation {
  const finalState: WorldState = { ...session.state };
  Object.freeze(finalState);

  const payload: AttestationPayload = {
    schema: 'chaosim.sim001.attestation.v1',
    finalState,
    tickCount: session.tick,
    trapTripped: session.trapTripped,
    directorEvents: session.directorLog.map((event) => ({ ...event })),
    completedAt,
  };

  return sign(payload, secret);
}
