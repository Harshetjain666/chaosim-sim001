import { sign, type Attestation } from '../attestation.ts';
import { runChaosLoadTest } from '../chaosLoadTest.ts';
import { defaultConfig, type EngineConfig } from '../config.ts';
import { SimulationEngine } from '../engine.ts';
import { mulberry32 } from '../rng.ts';
import { telemetryForTick } from '../scenario.ts';
import { TELEMETRY_KINDS } from '../types.ts';
import type { ShardCluster } from './cluster.ts';
import { kindCode } from './inbox.ts';

/** Length of the three phase chaos scenario, in ticks. */
export const SCENARIO_TICKS = 300;

/**
 * The chaos scenario for one seed as per tick inbox counts: exactly the
 * events `runChaosLoadTest` would generate, counted by kind.
 */
export function scenarioCounts(seed: number, tickMs: number, ticks = SCENARIO_TICKS): number[][] {
  const rng = mulberry32(seed);
  const perTick: number[][] = [];
  for (let tick = 0; tick < ticks; tick += 1) {
    const counts = new Array<number>(TELEMETRY_KINDS.length).fill(0);
    for (const event of telemetryForTick(tick, tickMs, rng)) {
      const code = kindCode(event.kind);
      counts[code] = counts[code]! + 1;
    }
    perTick.push(counts);
  }
  return perTick;
}

/**
 * The plain single session engine, never sharded and never crashed, driven
 * `ticks` ticks into the scenario for `seed`. Recovered sessions are compared
 * against this.
 */
export function referenceEngine(
  seed: number,
  ticks: number,
  config: EngineConfig = defaultConfig,
): SimulationEngine {
  const rng = mulberry32(seed);
  const engine = new SimulationEngine(config);
  for (let tick = 0; tick < ticks; tick += 1) {
    engine.step(telemetryForTick(tick, config.tickMs, rng));
  }
  return engine;
}

/**
 * The fingerprint a session run through the cluster must finish on: the
 * payload the plain single session run signs for `seed`, signed again as
 * schema v2 with the session's id, which is how every shard completion signs.
 */
export function referenceFingerprint(
  sessionId: string,
  options: { secret: string; seed: number; completedAt: string },
): string {
  const { payload } = runChaosLoadTest(options).attestation;
  return sign({ ...payload, schema: 'chaosim.sim001.attestation.v2', sessionId }, options.secret).signature;
}

export interface DriverSession {
  id: string;
  seed: number;
}

/**
 * Drives scenario sessions through a manual clock cluster in lockstep rounds.
 *
 * A round feeds every active session the counts for its own next tick,
 * advances every shard by one tick, and completes the sessions that reached
 * the end of the scenario. Each session keeps its own position, so after a
 * crash sessions can sit at different ticks, and `resync` picks each one up
 * exactly where its shard left it.
 */
export class ScenarioDriver {
  readonly attestations = new Map<string, Attestation>();
  private readonly cluster: ShardCluster;
  private readonly completedAt: string;
  private readonly workloads = new Map<string, number[][]>();
  private readonly positions = new Map<string, number>();

  constructor(
    cluster: ShardCluster,
    sessions: readonly DriverSession[],
    completedAt: string,
    tickMs: number = defaultConfig.tickMs,
  ) {
    this.cluster = cluster;
    this.completedAt = completedAt;
    for (const { id, seed } of sessions) {
      this.workloads.set(id, scenarioCounts(seed, tickMs));
      this.positions.set(id, 0);
    }
  }

  get done(): boolean {
    return this.attestations.size === this.workloads.size;
  }

  /** Ticks the session has run, as far as this driver knows. */
  localTick(id: string): number {
    return this.positions.get(id) ?? 0;
  }

  async openAll(): Promise<void> {
    await Promise.all([...this.workloads.keys()].map((id) => this.cluster.openSession(id)));
  }

  /** Read every session's position back from the cluster, after a crash or a restart. */
  async resync(): Promise<void> {
    await Promise.all(
      [...this.workloads.keys()].map(async (id) => {
        const view = await this.cluster.inspect(id);
        if (view === null) {
          // Its open never became durable, so it never existed. Start it again.
          await this.cluster.openSession(id);
          this.positions.set(id, 0);
        } else if (view.status === 'completed') {
          this.attestations.set(id, view.attestation);
          this.positions.set(id, SCENARIO_TICKS);
        } else if (view.status === 'expired') {
          throw new Error(`session ${id} completed so long ago that its attestation is gone`);
        } else {
          this.positions.set(id, view.snapshot.tick);
        }
      }),
    );
  }

  /** Run one lockstep round. Resolves false once every session has completed. */
  async round(): Promise<boolean> {
    const due: string[] = [];
    const stepping: [string, number][] = [];
    for (const [id, tick] of this.positions) {
      if (this.attestations.has(id)) continue;
      if (tick >= SCENARIO_TICKS) due.push(id);
      else stepping.push([id, tick]);
    }

    if (stepping.length > 0) {
      for (const [id, tick] of stepping) {
        this.workloads.get(id)![tick]!.forEach((count, code) => {
          if (count > 0) this.cluster.ingest(id, TELEMETRY_KINDS[code]!, count);
        });
      }
      await this.cluster.advance();
      for (const [id, tick] of stepping) {
        this.positions.set(id, tick + 1);
        if (tick + 1 >= SCENARIO_TICKS) due.push(id);
      }
    }

    await Promise.all(
      due.map(async (id) => {
        this.attestations.set(id, await this.cluster.complete(id, this.completedAt));
      }),
    );
    return !this.done;
  }
}
