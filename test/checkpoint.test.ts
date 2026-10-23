import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { endianness, tmpdir } from 'node:os';
import { join } from 'node:path';

import { defaultConfig, defaultShardConfig } from '../src/config.ts';
import { runChaosLoadTest } from '../src/chaosLoadTest.ts';
import {
  checkpointName,
  encodeCheckpoint,
  loadCheckpoint,
  writeCheckpoint,
  type ShardCheckpoint,
} from '../src/cluster/checkpoint.ts';
import { CorruptionError, UnsupportedFormatError } from '../src/cluster/errors.ts';
import { encodeFrame } from '../src/cluster/frame.ts';
import { SessionTable } from '../src/cluster/sessionTable.ts';

const AT = '2026-01-01T00:00:00.000Z';
const CAP = defaultShardConfig.inboxCapPerKind;
/** Eight bytes of magic, then the frame's length and checksum. */
const PAYLOAD_START = 16;

async function tempDir(t: TestContext): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'chaosim-checkpoint-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return dir;
}

function checkpointOf(table: SessionTable, overrides: Partial<ShardCheckpoint> = {}): ShardCheckpoint {
  return { version: 2, shardId: 3, seq: 77, tick: 35, sessions: table.columns(), completed: [], forgotten: [], ...overrides };
}

function busyTable(): SessionTable {
  const table = new SessionTable(defaultConfig, CAP);
  for (const id of ['a', 'b', 'ünïcödé "quoted"']) table.open(id);
  for (let tick = 0; tick < 35; tick += 1) {
    table.step([
      ['a', [40, 40, 0, 0, 0]],
      ['ünïcödé "quoted"', [1, 2, 3, 4, 5]],
    ]);
  }
  return table;
}

/** Rewrite a checkpoint's payload and frame it again, so only the change differs. */
function reframe(bytes: Buffer, payload: Buffer): Buffer {
  return Buffer.concat([bytes.subarray(0, 8), encodeFrame(payload)]);
}

test('a checkpoint round trips its columns, completions and forgotten ids', async (t) => {
  const dir = await tempDir(t);
  const attestation = runChaosLoadTest({ secret: 'test-secret', seed: 1, completedAt: AT }).attestation;
  const checkpoint = checkpointOf(busyTable(), {
    completed: [{ id: 'done', attestation }],
    forgotten: ['older', 'oldest'],
  });
  await writeCheckpoint(dir, 77, encodeCheckpoint(checkpoint));
  assert.deepEqual(await loadCheckpoint(dir), checkpoint);
});

test('a checkpoint costs the same bytes per session whatever the windows hold', () => {
  const ids = (n: number) => Array.from({ length: n }, (_, i) => `s${String(i).padStart(3, '0')}`);
  const bytesFor = (sessions: string[], counts: number[] | null) => {
    const table = new SessionTable(defaultConfig, CAP);
    for (const id of sessions) table.open(id);
    for (let tick = 0; tick < 25; tick += 1) table.step(counts ? sessions.map((id) => [id, counts]) : []);
    return encodeCheckpoint(checkpointOf(table)).length;
  };

  // A flooded window used to cost hundreds of kilobytes per session as JSON.
  assert.equal(bytesFor(ids(100), [CAP, CAP, CAP, CAP, CAP]), bytesFor(ids(100), null));
  const perSession = (bytesFor(ids(200), null) - bytesFor(ids(100), null)) / 100;
  assert.ok(perSession < 300, `${perSession} bytes per session`);
});

test('a checkpoint written in the other byte order is unsupported, not misread', async (t) => {
  const dir = await tempDir(t);
  const bytes = encodeCheckpoint(checkpointOf(busyTable()));
  const payload = Buffer.from(bytes.subarray(PAYLOAD_START));
  const host = endianness();
  const at = payload.indexOf(`"byteOrder":"${host}"`, 0, 'latin1');
  assert.ok(at > 0, 'the header names its byte order');
  payload.write(`"byteOrder":"${host === 'LE' ? 'BE' : 'LE'}"`, at, 'latin1');

  await writeFile(join(dir, checkpointName(77)), reframe(bytes, payload));
  await assert.rejects(loadCheckpoint(dir), UnsupportedFormatError);
});

test('a checkpoint whose columns do not match its header is corrupt', async (t) => {
  const dir = await tempDir(t);
  const bytes = encodeCheckpoint(checkpointOf(busyTable()));
  // Intact framing around a payload one count short: a bug, not a torn write.
  await writeFile(join(dir, checkpointName(77)), reframe(bytes, bytes.subarray(PAYLOAD_START, bytes.length - 2)));
  await assert.rejects(loadCheckpoint(dir), CorruptionError);
});

test('a checkpoint from the previous format is unsupported, not corrupt', async (t) => {
  const dir = await tempDir(t);
  const json = Buffer.from(JSON.stringify({ version: 1, shardId: 0, seq: 9, tick: 8, sessions: [], completed: [] }));
  await writeFile(join(dir, checkpointName(9)), Buffer.concat([Buffer.from('CSCKP001', 'latin1'), encodeFrame(json)]));
  await assert.rejects(loadCheckpoint(dir), (error: unknown) => {
    assert.ok(error instanceof UnsupportedFormatError, `got ${String(error)}`);
    assert.match(error.message, /001/);
    return true;
  });
});
