import { open, readdir, readFile, rename, rm } from 'node:fs/promises';
import { endianness } from 'node:os';
import { join } from 'node:path';
import type { Attestation } from '../attestation.ts';
import { TELEMETRY_KINDS } from '../types.ts';
import { CorruptionError, UnsupportedFormatError } from './errors.ts';
import { FRAME_HEADER_BYTES, assertKnownFormat, fsyncDir, readFrame, sealFrame, writeFully } from './frame.ts';
import { NUMBER_COLUMNS, type SessionColumns } from './sessionTable.ts';

/**
 * A shard frozen at a tick boundary. Recovery loads the newest one and replays
 * only the log records after `seq`, which bounds replay to one checkpoint
 * interval however long the shard has been running.
 */
export interface ShardCheckpoint {
  version: 2;
  shardId: number;
  /** Last WAL seq folded into this checkpoint. Replay starts after it. */
  seq: number;
  /** Shard ticks executed. */
  tick: number;
  /** Every active session, as the shard's columns in slot order. */
  sessions: SessionColumns;
  /** Retained completions, oldest first, so retries stay idempotent across restarts. */
  completed: { id: string; attestation: Attestation }[];
  /** Completed ids whose attestations were dropped, oldest first. */
  forgotten: string[];
}

/** The JSON at the front of a checkpoint's payload. The column bytes follow it. */
interface CheckpointHeader {
  version: 2;
  shardId: number;
  seq: number;
  tick: number;
  /** The columns are copied as they sit in memory, so they are in the writer's byte order. */
  byteOrder: string;
  /** The kinds and number columns in the order their bytes follow, so a reordered build cannot misread them. */
  kinds: readonly string[];
  columns: readonly string[];
  windowTicks: number;
  /** Session ids, in slot order. */
  sessions: string[];
  completed: { id: string; attestation: Attestation }[];
  forgotten: string[];
}

/** File header. The trailing digits are the format version. */
const CHECKPOINT_MAGIC = Buffer.from('CSCKP002', 'latin1');
const CHECKPOINT_FILE = /^checkpoint-(\d{16})\.ckpt$/;
const HEADER_LENGTH_BYTES = 4;

export function checkpointName(seq: number): string {
  return `checkpoint-${String(seq).padStart(16, '0')}.ckpt`;
}

/**
 * Serialize a checkpoint. The shard calls this synchronously at the end of a
 * tick, and this is the freeze: once it returns, the live columns are free to
 * keep moving while the bytes go to disk.
 *
 * After the magic comes one frame whose payload is
 *
 *   [u32 LE header length][header JSON][number columns][window counts]
 *
 * The JSON header carries what is not a number per session: the ids in slot
 * order, the retained attestations, the forgotten ids. Then each number
 * column as one float64 per session, in NUMBER_COLUMNS order, and the window
 * counts as uint16. A session costs the same bytes however busy its window
 * is, and the columns are copied once, straight into the frame.
 */
export function encodeCheckpoint(checkpoint: ShardCheckpoint): Buffer {
  const { sessions } = checkpoint;
  const header: CheckpointHeader = {
    version: checkpoint.version,
    shardId: checkpoint.shardId,
    seq: checkpoint.seq,
    tick: checkpoint.tick,
    byteOrder: endianness(),
    kinds: TELEMETRY_KINDS,
    columns: NUMBER_COLUMNS,
    windowTicks: sessions.windowTicks,
    sessions: sessions.ids,
    completed: checkpoint.completed,
    forgotten: checkpoint.forgotten,
  };
  const json = Buffer.from(JSON.stringify(header), 'utf8');
  const columns = [...NUMBER_COLUMNS.map((name) => sessions[name]), sessions.window];
  const columnBytes = columns.reduce((total, column) => total + column.byteLength, 0);

  const frameStart = CHECKPOINT_MAGIC.length;
  const buf = Buffer.allocUnsafe(frameStart + FRAME_HEADER_BYTES + HEADER_LENGTH_BYTES + json.length + columnBytes);
  CHECKPOINT_MAGIC.copy(buf, 0);
  let offset = buf.writeUInt32LE(json.length, frameStart + FRAME_HEADER_BYTES);
  offset += json.copy(buf, offset);
  for (const column of columns) {
    offset += Buffer.from(column.buffer, column.byteOffset, column.byteLength).copy(buf, offset);
  }
  sealFrame(buf, frameStart);
  return buf;
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
  const checkpoint = decodeCheckpoint(frame.payload, name);
  if (checkpoint.seq !== newest) {
    throw new CorruptionError(`${name}: unexpected version or seq`);
  }
  return checkpoint;
}

/**
 * The checksum matched, so these are the bytes a shard wrote. Anything that
 * does not add up is a bug or tampering, never a torn write.
 */
function decodeCheckpoint(payload: Buffer, name: string): ShardCheckpoint {
  const corrupt = (why: string) => new CorruptionError(`${name}: ${why}`);
  if (payload.length < HEADER_LENGTH_BYTES) throw corrupt('payload too short for its header');
  const headerEnd = HEADER_LENGTH_BYTES + payload.readUInt32LE(0);
  if (headerEnd > payload.length) throw corrupt('header runs past the payload');
  let header: CheckpointHeader;
  try {
    header = JSON.parse(payload.toString('utf8', HEADER_LENGTH_BYTES, headerEnd)) as CheckpointHeader;
  } catch {
    throw corrupt('header is not valid JSON');
  }
  if (
    typeof header !== 'object' ||
    header === null ||
    header.version !== 2 ||
    !Number.isSafeInteger(header.tick) ||
    !Number.isInteger(header.windowTicks) ||
    header.windowTicks < 1 ||
    !Array.isArray(header.sessions) ||
    !header.sessions.every((id) => typeof id === 'string') ||
    !Array.isArray(header.completed) ||
    !Array.isArray(header.forgotten)
  ) {
    throw corrupt('unexpected version or header');
  }
  if (header.byteOrder !== endianness()) {
    throw new UnsupportedFormatError(`${name} holds ${header.byteOrder} columns; this host is ${endianness()}`);
  }
  if (!sameNames(header.kinds, TELEMETRY_KINDS) || !sameNames(header.columns, NUMBER_COLUMNS)) {
    throw new UnsupportedFormatError(`${name} lays out its columns differently from this build`);
  }

  const n = header.sessions.length;
  const windowLength = n * header.windowTicks * TELEMETRY_KINDS.length;
  const expected =
    headerEnd + NUMBER_COLUMNS.length * n * Float64Array.BYTES_PER_ELEMENT + windowLength * Uint16Array.BYTES_PER_ELEMENT;
  if (payload.length !== expected) throw corrupt(`columns hold ${payload.length - headerEnd} bytes, not the ${expected - headerEnd} the header implies`);

  // Copy each column out: the file buffer need not be aligned for a typed array view.
  let offset = headerEnd;
  const take = <T extends Float64Array | Uint16Array>(column: T): T => {
    Buffer.from(column.buffer, column.byteOffset, column.byteLength).set(payload.subarray(offset, offset + column.byteLength));
    offset += column.byteLength;
    return column;
  };
  const sessions = { ids: header.sessions, windowTicks: header.windowTicks } as SessionColumns;
  for (const column of NUMBER_COLUMNS) sessions[column] = take(new Float64Array(n));
  sessions.window = take(new Uint16Array(windowLength));

  return {
    version: 2,
    shardId: header.shardId,
    seq: header.seq,
    tick: header.tick,
    sessions,
    completed: header.completed,
    forgotten: header.forgotten,
  };
}

function sameNames(found: unknown, expected: readonly string[]): boolean {
  return Array.isArray(found) && found.length === expected.length && found.every((name, i) => name === expected[i]);
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
