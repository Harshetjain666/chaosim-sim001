import { open, readdir, readFile, rename, rm } from 'node:fs/promises';
import { join } from 'node:path';
import type { Attestation } from '../attestation.ts';
import type { EngineSnapshot } from '../engine.ts';
import { CorruptionError } from './errors.ts';
import { assertKnownFormat, encodeFrame, fsyncDir, readFrame, writeFully } from './frame.ts';

/**
 * A shard frozen at a tick boundary. Recovery loads the newest one and replays
 * only the log records after `seq`, which bounds replay to one checkpoint
 * interval however long the shard has been running.
 */
export interface ShardCheckpoint {
  version: 1;
  shardId: number;
  /** Last WAL seq folded into this checkpoint. Replay starts after it. */
  seq: number;
  /** Shard ticks executed. */
  tick: number;
  sessions: { id: string; engine: EngineSnapshot }[];
  /** Retained completions, oldest first, so retries stay idempotent across restarts. */
  completed: { id: string; attestation: Attestation }[];
  /** Completed ids whose attestations were dropped, oldest first. Absent in older checkpoints. */
  forgotten?: string[];
}

/** File header. The trailing digits are the format version. */
const CHECKPOINT_MAGIC = Buffer.from('CSCKP001', 'latin1');
const CHECKPOINT_FILE = /^checkpoint-(\d{16})\.ckpt$/;

export function checkpointName(seq: number): string {
  return `checkpoint-${String(seq).padStart(16, '0')}.ckpt`;
}

/**
 * Serialize a checkpoint. The shard calls this synchronously at the end of a
 * tick: the stringify is the copy, and once it returns the live sessions are
 * free to keep moving while the bytes go to disk.
 */
export function encodeCheckpoint(checkpoint: ShardCheckpoint): Buffer {
  const payload = Buffer.from(JSON.stringify(checkpoint), 'utf8');
  return Buffer.concat([CHECKPOINT_MAGIC, encodeFrame(payload)]);
}

/** Write atomically: temp file, fsync, rename into place, fsync the directory. */
export async function writeCheckpoint(dir: string, seq: number, bytes: Buffer): Promise<void> {
  const target = join(dir, checkpointName(seq));
  const temp = `${target}.tmp`;
  const handle = await open(temp, 'w');
  try {
    await writeFully(handle, bytes);
    await handle.datasync();
  } finally {
    await handle.close();
  }
  await rename(temp, target);
  await fsyncDir(dir);
}

async function listCheckpoints(dir: string): Promise<number[]> {
  const seqs: number[] = [];
  for (const name of await readdir(dir)) {
    const match = CHECKPOINT_FILE.exec(name);
    if (match) seqs.push(Number(match[1]));
  }
  return seqs.sort((a, b) => a - b);
}

/**
 * The newest checkpoint, or null when there is none. One that fails its
 * checksum is an error rather than a reason to fall back to an older one: the
 * log segments the older one would need were deleted when the newer one
 * became durable.
 */
export async function loadCheckpoint(dir: string): Promise<ShardCheckpoint | null> {
  const newest = (await listCheckpoints(dir)).at(-1);
  if (newest === undefined) return null;

  const name = checkpointName(newest);
  const buf = await readFile(join(dir, name));
  if (!buf.subarray(0, CHECKPOINT_MAGIC.length).equals(CHECKPOINT_MAGIC)) {
    assertKnownFormat(buf, CHECKPOINT_MAGIC, name);
    throw new CorruptionError(`${name}: bad header`);
  }
  const frame = readFrame(buf, CHECKPOINT_MAGIC.length);
  if (frame.kind !== 'frame' || frame.next !== buf.length) {
    throw new CorruptionError(`${name}: failed its checksum`);
  }
  let checkpoint: ShardCheckpoint;
  try {
    checkpoint = JSON.parse(frame.payload.toString('utf8')) as ShardCheckpoint;
  } catch {
    throw new CorruptionError(`${name}: payload is not valid JSON`);
  }
  if (checkpoint.version !== 1 || checkpoint.seq !== newest) {
    throw new CorruptionError(`${name}: unexpected version or seq`);
  }
  return checkpoint;
}

/** Delete checkpoints older than `keepSeq`, and any temp file a crash left behind. */
export async function pruneCheckpoints(dir: string, keepSeq: number): Promise<void> {
  let removed = 0;
  for (const name of await readdir(dir)) {
    const match = CHECKPOINT_FILE.exec(name);
    const stale = name.endsWith('.ckpt.tmp') || (match !== null && Number(match[1]) < keepSeq);
    if (stale) {
      await rm(join(dir, name));
      removed += 1;
    }
  }
  if (removed > 0) await fsyncDir(dir);
}
