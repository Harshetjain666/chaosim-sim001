import type { Attestation } from '../attestation.ts';
import type { EngineConfig, ShardConfig } from '../config.ts';
import type { ClusterErrorCode } from './errors.ts';
import type { OpenResult, PublishedEvent, RecoveryReport, SessionView, ShardStats } from './shard.ts';

/**
 * Messages between the router (main thread) and a shard worker. Ordering is
 * the contract that matters: a worker handles messages in the order the
 * router posted them, so telemetry posted before a request is always in the
 * inbox before that request runs.
 */

/** Who drives shard ticks: the wall clock, or the router one `advance` at a time. */
export type ClockMode = 'realtime' | 'manual';

export interface ShardWorkerData {
  shardId: number;
  dir: string;
  secret: string;
  engineConfig: EngineConfig;
  shardConfig: ShardConfig;
  clock: ClockMode;
}

export type ShardRequest =
  | { op: 'open'; sessionId: string }
  | { op: 'complete'; sessionId: string; completedAt: string }
  | { op: 'inspect'; sessionId: string }
  | { op: 'advance'; ticks: number }
  | { op: 'stats' }
  | { op: 'shutdown' };

/** What each request resolves with. */
export interface ShardReplies {
  open: OpenResult;
  complete: Attestation;
  inspect: SessionView | null;
  /** The shard's tick after advancing. */
  advance: number;
  stats: WorkerStats;
  shutdown: null;
}

export interface LoopStats {
  lagP50Ms: number;
  lagP99Ms: number;
  lagMaxMs: number;
  /** Share of wall time spent inside shard ticks since the last stats call. */
  busyRatio: number;
  /** Real time the clock owes the shard but has not released as ticks yet. */
  backlogMs: number;
}

export interface WorkerStats extends ShardStats {
  loop: LoopStats;
}

export type RouterMessage =
  /** Flattened `[sessionId, kindCode, count]` triples: one message per shard per event loop turn. */
  | { type: 'ingest'; data: (string | number)[] }
  | { type: 'request'; id: number; request: ShardRequest };

export type WorkerMessage =
  | { type: 'ready'; recovery: RecoveryReport }
  | { type: 'reply'; id: number; ok: true; value: unknown }
  | { type: 'reply'; id: number; ok: false; error: { message: string; code?: ClusterErrorCode } }
  | { type: 'events'; events: PublishedEvent[] }
  /** The shard hit an error it cannot continue past. The worker exits right after sending this. */
  | { type: 'fatal'; message: string };
