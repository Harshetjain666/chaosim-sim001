import { mkdir, open, readdir, readFile, rm, stat, truncate, type FileHandle } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { createHistogram } from 'node:perf_hooks';
import { CorruptionError } from './errors.ts';
import { assertKnownFormat, encodeFrame, fsyncDir, readFrame, writeFully } from './frame.ts';

/**
 * The per shard write ahead log.
 *
 * It is a command log: records carry each tick's inputs and the session
 * lifecycle, never the resulting state. Recovery re-executes them through the
 * engine, which is deterministic, so the rebuilt state is identical to the
 * one that was lost. docs/SCALE_OUT.md explains why state deltas would not be
 * enough.
 *
 * On disk the log is a run of segment files, `wal-<first seq>.log`, each an
 * 8 byte magic followed by framed JSON records. A new segment starts at every
 * checkpoint so whole segments can be deleted once a checkpoint covers them.
 */

/** One session's inbox counters for a tick, in TELEMETRY_KINDS order. */
export type TickInput = [sessionId: string, counts: number[]];

export type WalEntry =
  | { op: 'open'; sessionId: string }
  | { op: 'tick'; tick: number; inputs: TickInput[] }
  | { op: 'complete'; sessionId: string; completedAt: string };

export type WalRecord = WalEntry & { seq: number };

/** Segment header. The trailing digits are the format version. */
export const WAL_MAGIC = Buffer.from('CSWAL001', 'latin1');

const SEGMENT_FILE = /^wal-(\d{16})\.log$/;

export function segmentName(startSeq: number): string {
  return `wal-${String(startSeq).padStart(16, '0')}.log`;
}

async function listSegments(dir: string): Promise<number[]> {
  const starts: number[] = [];
  for (const name of await readdir(dir)) {
    const match = SEGMENT_FILE.exec(name);
    if (match) starts.push(Number(match[1]));
  }
  return starts.sort((a, b) => a - b);
}

function decodeRecord(payload: Buffer, where: string): WalRecord {
  let record: { seq?: unknown; op?: unknown } | null;
  try {
    record = JSON.parse(payload.toString('utf8')) as { seq?: unknown; op?: unknown } | null;
  } catch {
    // The checksum matched, so these are the bytes we wrote: a bug, not a torn write.
    throw new CorruptionError(`${where}: record is not valid JSON`);
  }
  if (
    typeof record !== 'object' ||
    record === null ||
    typeof record.seq !== 'number' ||
    (record.op !== 'open' && record.op !== 'tick' && record.op !== 'complete')
  ) {
    throw new CorruptionError(`${where}: record has no valid seq and op`);
  }
  return record as WalRecord;
}

export interface TornTail {
  file: string;
  /** Byte offset of the first damaged frame. Everything before it is intact. */
  offset: number;
  bytes: number;
}

export interface WalScan {
  /** Records after `afterSeq`, in order, with no gaps. */
  records: WalRecord[];
  /** Highest seq accounted for: the last intact record, or `afterSeq` if that is newer. */
  lastSeq: number;
  tornTail: TornTail | null;
}

/**
 * Read the log for recovery.
 *
 * A damaged frame at the very end of the last segment is a write the crash
 * interrupted: it is reported as the torn tail and the scan stops there. The
 * same damage anywhere else, a gap in the sequence, or a log that resumes
 * past `afterSeq + 1` is corruption. Replaying across any of those would
 * silently build the wrong state, so the scan refuses instead.
 */
export async function scanWal(dir: string, afterSeq: number): Promise<WalScan> {
  const starts = await listSegments(dir);
  const records: WalRecord[] = [];
  let expected: number | null = null;
  let tornTail: TornTail | null = null;

  for (let i = 0; i < starts.length && tornTail === null; i += 1) {
    const start = starts[i]!;
    const isLast = i === starts.length - 1;
    const name = segmentName(start);
    const file = join(dir, name);
    const buf = await readFile(file);

    if (!buf.subarray(0, WAL_MAGIC.length).equals(WAL_MAGIC)) {
      // A crash while the segment was being created leaves part of the magic,
      // or, on some filesystems after a power loss, zeros where it should be.
      const tornHeader =
        (buf.length < WAL_MAGIC.length && WAL_MAGIC.subarray(0, buf.length).equals(buf)) ||
        buf.every((byte) => byte === 0);
      if (isLast && tornHeader) {
        tornTail = { file, offset: 0, bytes: buf.length };
        break;
      }
      assertKnownFormat(buf, WAL_MAGIC, name);
      throw new CorruptionError(`${name}: bad segment header`);
    }
    if (expected !== null && start !== expected) {
      throw new CorruptionError(
        `${name}: starts at seq ${start} but the previous segment ends at ${expected - 1}`,
      );
    }
    expected = start;

    let offset = WAL_MAGIC.length;
    for (;;) {
      const frame = readFrame(buf, offset);
      if (frame.kind === 'end') break;
      if (frame.kind === 'torn') {
        if (!isLast) {
          throw new CorruptionError(`${name}: damaged record at byte ${offset} of a sealed segment`);
        }
        tornTail = { file, offset, bytes: buf.length - offset };
        break;
      }
      const record = decodeRecord(frame.payload, `${name} byte ${offset}`);
      if (record.seq !== expected) {
        throw new CorruptionError(`${name}: found seq ${record.seq} where ${expected} belongs`);
      }
      expected += 1;
      if (record.seq > afterSeq) records.push(record);
      offset = frame.next;
    }
  }

  const first = records[0];
  if (first !== undefined && first.seq !== afterSeq + 1) {
    throw new CorruptionError(
      `log resumes at seq ${first.seq} but nothing accounts for seqs ${afterSeq + 1} to ${first.seq - 1}`,
    );
  }
  const lastSeq = Math.max(afterSeq, expected === null ? 0 : expected - 1);
  return { records, lastSeq, tornTail };
}

/** Cut a torn tail off so the segment is clean, or remove a segment that holds no intact record. */
export async function repairTornTail(tail: TornTail): Promise<void> {
  if (tail.offset <= WAL_MAGIC.length) {
    await rm(tail.file);
  } else {
    await truncate(tail.file, tail.offset);
    const handle = await open(tail.file, 'r+');
    try {
      await handle.datasync();
    } finally {
      await handle.close();
    }
  }
  await fsyncDir(dirname(tail.file));
}

async function createSegment(dir: string, start: number): Promise<FileHandle> {
  const handle = await open(join(dir, segmentName(start)), 'wx');
  try {
    await writeFully(handle, WAL_MAGIC);
    await handle.datasync();
    await fsyncDir(dir);
  } catch (error) {
    await handle.close();
    throw error;
  }
  return handle;
}

export interface WalStats {
  fsyncs: number;
  bytesWritten: number;
  fsyncP50Ms: number;
  fsyncP99Ms: number;
  fsyncMaxMs: number;
}

interface PendingRecord {
  seq: number;
  bytes: Buffer;
}

/**
 * Appends records and makes them durable with group commit.
 *
 * `append` only touches memory and returns the record's seq. `flush` hands
 * everything appended so far to one write and one fdatasync on the libuv
 * thread pool, so the owner's event loop never blocks on the disk. One disk
 * operation runs at a time, and records appended while a flush is running
 * ride the next one.
 *
 * Any I/O error is final. After a failed fsync the kernel may already have
 * dropped the dirty pages, so a retry could report success for data that is
 * gone. Every later call fails with the original error, and the owner is
 * expected to crash and recover from what is provably on disk.
 */
export class WalWriter {
  private readonly dir: string;
  private nextSeq: number;
  private durable: number;
  private pending: PendingRecord[] = [];
  private pendingByteCount = 0;
  /** Segment starts the next flush must open, in order. */
  private rotations: number[] = [];
  /** Start of the newest segment, open or scheduled. */
  private newestStart: number;
  private segmentStart: number;
  private file: FileHandle;
  /** Closed segments still on disk, oldest first. */
  private readonly sealed: number[];
  private chain: Promise<unknown> = Promise.resolve();
  private failure: Error | null = null;
  private fsyncs = 0;
  private bytesWritten = 0;
  private readonly fsyncMicros = createHistogram();

  private constructor(dir: string, nextSeq: number, file: FileHandle, sealed: number[]) {
    this.dir = dir;
    this.nextSeq = nextSeq;
    this.durable = nextSeq - 1;
    this.segmentStart = nextSeq;
    this.newestStart = nextSeq;
    this.file = file;
    this.sealed = sealed;
  }

  /** Start a fresh segment at `nextSeq`. Everything before it must already be on disk. */
  static async open(dir: string, nextSeq: number): Promise<WalWriter> {
    await mkdir(dir, { recursive: true });
    const name = segmentName(nextSeq);
    const existing = await stat(join(dir, name)).catch((error: NodeJS.ErrnoException) => {
      if (error.code === 'ENOENT') return null;
      throw error;
    });
    if (existing !== null) {
      if (existing.size > WAL_MAGIC.length) {
        throw new CorruptionError(`${name} already holds records; refusing to overwrite it`);
      }
      // An empty segment left by a rotation just before a crash.
      await rm(join(dir, name));
    }

    const sealed = await listSegments(dir);
    if (sealed.some((start) => start > nextSeq)) {
      throw new CorruptionError(`segments past seq ${nextSeq} exist; refusing to fork the log`);
    }
    const file = await createSegment(dir, nextSeq);
    return new WalWriter(dir, nextSeq, file, sealed);
  }

  get lastSeq(): number {
    return this.nextSeq - 1;
  }

  get durableSeq(): number {
    return this.durable;
  }

  /** Bytes appended but not yet durable. */
  get pendingBytes(): number {
    return this.pendingByteCount;
  }

  append(entry: WalEntry): number {
    if (this.failure) throw this.failure;
    const seq = this.nextSeq;
    this.nextSeq += 1;
    const bytes = encodeFrame(Buffer.from(JSON.stringify({ seq, ...entry }), 'utf8'));
    this.pending.push({ seq, bytes });
    this.pendingByteCount += bytes.length;
    return seq;
  }

  /** Begin a new segment at the next appended seq. It takes effect with the next flush. */
  rotate(): void {
    if (this.nextSeq === this.newestStart) return;
    this.newestStart = this.nextSeq;
    this.rotations.push(this.nextSeq);
  }

  /**
   * Make everything appended so far durable. Resolves with the durable seq.
   *
   * Flushes queue behind each other. The first one to start takes every
   * record appended by then, so the ones behind it usually find nothing left
   * and resolve without touching the disk. That is the group commit.
   */
  flush(): Promise<number> {
    if (this.failure) return Promise.reject(this.failure);
    const flush = this.chain.then(() => this.writePending());
    this.chain = flush.catch(() => undefined);
    return flush;
  }

  /**
   * Delete sealed segments that hold nothing at or after `seq`, oldest first so
   * a crash midway never leaves a hole. Only call it once a durable checkpoint
   * covers everything before `seq`. Resolves with how many were removed.
   */
  pruneBefore(seq: number): Promise<number> {
    const prune = this.chain.then(async () => {
      if (this.failure) throw this.failure;
      let removed = 0;
      while (this.sealed.length > 0) {
        const nextStart = this.sealed[1] ?? this.segmentStart;
        if (nextStart > seq) break;
        await rm(join(this.dir, segmentName(this.sealed[0]!)));
        this.sealed.shift();
        removed += 1;
      }
      if (removed > 0) await fsyncDir(this.dir);
      return removed;
    });
    this.chain = prune.catch(() => undefined);
    return prune;
  }

  /** Counters since open, and fsync latency percentiles since the previous call. */
  takeStats(): WalStats {
    const h = this.fsyncMicros;
    const empty = h.count === 0;
    const stats: WalStats = {
      fsyncs: this.fsyncs,
      bytesWritten: this.bytesWritten,
      fsyncP50Ms: empty ? 0 : h.percentile(50) / 1000,
      fsyncP99Ms: empty ? 0 : h.percentile(99) / 1000,
      fsyncMaxMs: empty ? 0 : h.max / 1000,
    };
    h.reset();
    return stats;
  }

  /** Flush what is buffered and close the open segment. */
  async close(): Promise<void> {
    try {
      await this.flush();
    } finally {
      await this.chain;
      this.failure ??= new Error('WAL writer is closed');
      await this.file.close();
    }
  }

  private async writePending(): Promise<number> {
    if (this.failure) throw this.failure;
    const batch = this.pending;
    const rotations = this.rotations;
    this.pending = [];
    this.rotations = [];

    try {
      let chunk: PendingRecord[] = [];
      let next = 0;
      for (const record of batch) {
        while (next < rotations.length && record.seq >= rotations[next]!) {
          await this.writeChunk(chunk);
          chunk = [];
          await this.startSegment(rotations[next]!);
          next += 1;
        }
        chunk.push(record);
      }
      await this.writeChunk(chunk);
      for (; next < rotations.length; next += 1) {
        await this.startSegment(rotations[next]!);
      }
      return this.durable;
    } catch (error) {
      this.failure = error instanceof Error ? error : new Error(String(error));
      throw this.failure;
    }
  }

  private async writeChunk(chunk: PendingRecord[]): Promise<void> {
    const last = chunk.at(-1);
    if (last === undefined) return;
    const bytes = Buffer.concat(chunk.map((record) => record.bytes));
    await writeFully(this.file, bytes);

    const started = performance.now();
    await this.file.datasync();
    this.fsyncMicros.record(Math.max(1, Math.round((performance.now() - started) * 1000)));

    this.fsyncs += 1;
    this.bytesWritten += bytes.length;
    this.pendingByteCount -= bytes.length;
    this.durable = last.seq;
  }

  /** Open the next segment, then seal the current one. The new file is durable before any record lands in it. */
  private async startSegment(start: number): Promise<void> {
    const next = await createSegment(this.dir, start);
    await this.file.close();
    this.sealed.push(this.segmentStart);
    this.segmentStart = start;
    this.file = next;
  }
}
