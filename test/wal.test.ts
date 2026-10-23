import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { chmod, mkdtemp, readdir, readFile, rm, stat, truncate, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { checkpointName, loadCheckpoint } from '../src/cluster/checkpoint.ts';
import { CorruptionError, UnsupportedFormatError } from '../src/cluster/errors.ts';
import {
  WAL_MAGIC,
  WalWriter,
  repairTornTail,
  scanWal,
  segmentName,
  type WalEntry,
} from '../src/cluster/wal.ts';

const AT = '2026-01-01T00:00:00.000Z';

async function tempDir(t: TestContext): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'chaosim-wal-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return dir;
}

async function segmentFiles(dir: string): Promise<string[]> {
  return (await readdir(dir)).filter((name) => name.startsWith('wal-')).sort();
}

function opened(id: string): WalEntry {
  return { op: 'open', sessionId: id };
}

async function writeOpens(wal: WalWriter, count: number): Promise<void> {
  for (let i = 0; i < count; i += 1) wal.append(opened(`s${wal.lastSeq + 1}`));
  await wal.flush();
}

async function flipByte(file: string, offset: number): Promise<void> {
  const bytes = await readFile(file);
  bytes[offset] = bytes[offset]! ^ 0xff;
  await writeFile(file, bytes);
}

test('records round trip with contiguous sequence numbers', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  assert.equal(wal.append(opened('a')), 1);
  assert.equal(wal.append({ op: 'tick', tick: 0, inputs: [['a', [1, 2, 0, 0, 0]]] }), 2);
  assert.equal(wal.append({ op: 'complete', sessionId: 'a', completedAt: AT }), 3);
  assert.equal(wal.durableSeq, 0);
  assert.equal(await wal.flush(), 3);
  await wal.close();

  const scan = await scanWal(dir, 0);
  assert.equal(scan.tornTail, null);
  assert.equal(scan.lastSeq, 3);
  assert.deepEqual(scan.records, [
    { seq: 1, op: 'open', sessionId: 'a' },
    { seq: 2, op: 'tick', tick: 0, inputs: [['a', [1, 2, 0, 0, 0]]] },
    { seq: 3, op: 'complete', sessionId: 'a', completedAt: AT },
  ]);
  assert.deepEqual((await scanWal(dir, 2)).records.map((r) => r.seq), [3]);
});

test('concurrent flushes coalesce into one group commit', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  for (let i = 0; i < 50; i += 1) wal.append(opened(`s${i}`));

  const results = await Promise.all([wal.flush(), wal.flush(), wal.flush(), wal.flush(), wal.flush()]);
  assert.deepEqual(results, [50, 50, 50, 50, 50]);
  assert.equal(wal.takeStats().fsyncs, 1);
  await wal.close();
});

test('records appended during an in flight flush ride the next one', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  for (let i = 0; i < 10; i += 1) wal.append(opened(`a${i}`));
  const first = wal.flush();
  await new Promise((resolve) => setImmediate(resolve));

  for (let i = 0; i < 10; i += 1) wal.append(opened(`b${i}`));
  const second = wal.flush();
  const third = wal.flush();
  assert.equal(await first, 10);
  assert.equal(await second, 20);
  assert.equal(await third, 20);
  assert.equal(wal.takeStats().fsyncs, 2);
  await wal.close();
});

test('rotation starts a new segment and pruning drops only covered ones', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  await writeOpens(wal, 3);
  wal.rotate();
  await writeOpens(wal, 2);

  assert.deepEqual(await segmentFiles(dir), [segmentName(1), segmentName(4)]);
  assert.equal(await wal.pruneBefore(3), 0, 'segment 1 still holds seq 3');
  assert.equal(await wal.pruneBefore(4), 1);
  assert.deepEqual(await segmentFiles(dir), [segmentName(4)]);
  await wal.close();

  assert.deepEqual((await scanWal(dir, 3)).records.map((r) => r.seq), [4, 5]);
});

test('a torn final record is reported, repaired, and writing resumes after it', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  await writeOpens(wal, 4);
  const file = join(dir, segmentName(1));
  const offsetOfFifth = (await stat(file)).size;
  await writeOpens(wal, 1);
  await wal.close();

  const fullSize = (await stat(file)).size;
  await truncate(file, fullSize - 3);

  const scan = await scanWal(dir, 0);
  assert.deepEqual(scan.records.map((r) => r.seq), [1, 2, 3, 4]);
  assert.deepEqual(scan.tornTail, { file, offset: offsetOfFifth, bytes: fullSize - 3 - offsetOfFifth });

  await repairTornTail(scan.tornTail!);
  assert.equal((await stat(file)).size, offsetOfFifth);
  const repaired = await scanWal(dir, 0);
  assert.equal(repaired.tornTail, null);
  assert.equal(repaired.lastSeq, 4);

  const resumed = await WalWriter.open(dir, 5);
  await writeOpens(resumed, 1);
  await resumed.close();
  assert.deepEqual((await scanWal(dir, 0)).records.map((r) => r.seq), [1, 2, 3, 4, 5]);
});

test('a checksum failure in the final record is treated as torn', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  await writeOpens(wal, 4);
  const file = join(dir, segmentName(1));
  const offsetOfFifth = (await stat(file)).size;
  await writeOpens(wal, 1);
  await wal.close();

  await flipByte(file, offsetOfFifth + 12);
  const scan = await scanWal(dir, 0);
  assert.deepEqual(scan.records.map((r) => r.seq), [1, 2, 3, 4]);
  assert.equal(scan.tornTail?.offset, offsetOfFifth);
});

test('damage inside a sealed segment is corruption, not a torn tail', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  await writeOpens(wal, 3);
  wal.rotate();
  await writeOpens(wal, 2);
  await wal.close();

  await flipByte(join(dir, segmentName(1)), 20);
  await assert.rejects(scanWal(dir, 0), CorruptionError);
});

test('a missing segment is a gap and refuses to replay', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  await writeOpens(wal, 3);
  wal.rotate();
  await writeOpens(wal, 2);
  wal.rotate();
  await writeOpens(wal, 2);
  await wal.close();
  assert.deepEqual(await segmentFiles(dir), [segmentName(1), segmentName(4), segmentName(6)]);

  await rm(join(dir, segmentName(4)));
  await assert.rejects(scanWal(dir, 0), CorruptionError);
});

test('a log that resumes past the checkpoint is a gap', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 10);
  await writeOpens(wal, 2);
  await wal.close();

  await assert.rejects(scanWal(dir, 5), CorruptionError);
  assert.deepEqual((await scanWal(dir, 9)).records.map((r) => r.seq), [10, 11]);
});

test('an empty last segment is fine and is reused by the next writer', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  await writeOpens(wal, 3);
  wal.rotate();
  await wal.flush();
  await wal.close();
  assert.deepEqual(await segmentFiles(dir), [segmentName(1), segmentName(4)]);

  const scan = await scanWal(dir, 0);
  assert.equal(scan.tornTail, null);
  assert.equal(scan.lastSeq, 3);

  const resumed = await WalWriter.open(dir, 4);
  await writeOpens(resumed, 1);
  await resumed.close();
  assert.deepEqual((await scanWal(dir, 0)).records.map((r) => r.seq), [1, 2, 3, 4]);
});

test('a segment cut off inside its header is repaired away', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  await writeOpens(wal, 3);
  await wal.close();
  await writeFile(join(dir, segmentName(4)), 'CSW');

  const scan = await scanWal(dir, 0);
  assert.equal(scan.lastSeq, 3);
  assert.deepEqual(scan.tornTail, { file: join(dir, segmentName(4)), offset: 0, bytes: 3 });
  await repairTornTail(scan.tornTail!);
  assert.deepEqual(await segmentFiles(dir), [segmentName(1)]);
});

test('a last segment left as zeros by a power loss is torn, not corrupt', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  await writeOpens(wal, 3);
  await wal.close();
  // A file the crash extended but never wrote can read back as zeros on some filesystems.
  await writeFile(join(dir, segmentName(4)), Buffer.alloc(8));

  const scan = await scanWal(dir, 0);
  assert.equal(scan.lastSeq, 3);
  assert.deepEqual(scan.tornTail, { file: join(dir, segmentName(4)), offset: 0, bytes: 8 });

  // The same zeros anywhere but the last segment are still damage.
  await writeFile(join(dir, segmentName(5)), WAL_MAGIC);
  await assert.rejects(scanWal(dir, 0), CorruptionError);
});

test('a segment or checkpoint in a newer format is unsupported, not corrupt', async (t) => {
  const dir = await tempDir(t);
  // Telling an operator "corrupt" for files a newer build wrote invites deleting good data.
  await writeFile(join(dir, segmentName(1)), Buffer.concat([Buffer.from('CSWAL002', 'latin1'), Buffer.alloc(16, 7)]));
  await assert.rejects(scanWal(dir, 0), (error: unknown) => {
    assert.ok(error instanceof UnsupportedFormatError, `got ${String(error)}`);
    assert.match(error.message, /002/);
    return true;
  });

  await writeFile(join(dir, checkpointName(5)), Buffer.from('CSCKP003 and whatever a newer build wrote', 'latin1'));
  await assert.rejects(loadCheckpoint(dir), UnsupportedFormatError);
});

test('a writer refuses to overwrite a segment that already holds records', async (t) => {
  const dir = await tempDir(t);
  const wal = await WalWriter.open(dir, 1);
  await writeOpens(wal, 2);
  await wal.close();
  await assert.rejects(WalWriter.open(dir, 1), CorruptionError);
});

test(
  'an I/O failure fails that flush and every later one',
  { skip: process.getuid?.() === 0 ? 'root ignores directory permissions' : false },
  async (t) => {
    const dir = await tempDir(t);
    const wal = await WalWriter.open(dir, 1);
    await writeOpens(wal, 1);

    await chmod(dir, 0o500);
    try {
      wal.append(opened('x'));
      wal.rotate();
      wal.append(opened('y'));
      await assert.rejects(wal.flush(), { code: 'EACCES' });
      await assert.rejects(wal.flush(), { code: 'EACCES' });
      assert.throws(() => wal.append(opened('z')), { code: 'EACCES' });
      assert.equal(wal.durableSeq, 2, 'the record written before the failed rotation is durable');
    } finally {
      await chmod(dir, 0o700);
      await wal.close().catch(() => {});
    }
  },
);
