import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { appendFile, chmod, mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { verify } from '../src/attestation.ts';
import { defaultConfig, defaultShardConfig, type ShardConfig } from '../src/config.ts';
import { CEO_ALERT_MESSAGE } from '../src/scenarioDirector.ts';
import type { EngineSnapshot } from '../src/engine.ts';
import { kindCode } from '../src/cluster/inbox.ts';
import { Shard, type PublishedEvent } from '../src/cluster/shard.ts';
import { checkpointName } from '../src/cluster/checkpoint.ts';
import { segmentName } from '../src/cluster/wal.ts';
import { referenceEngine, referenceFingerprint, scenarioCounts } from '../src/cluster/workload.ts';

const config = defaultConfig;
const SECRET = 'sim-001-dev-secret';
const AT = '2026-01-01T00:00:00.000Z';
/**
 * Seed 1 run as session `s1`. Derived outside this code base: the canonical
 * bytes behind the single session anchor (`node src/index.ts --fixed-time`),
 * with the schema set to v2 and `"sessionId":"s1"` added after it, signed by
 * `openssl dgst -sha256 -hmac`.
 */
const S1_SEED_1_FINGERPRINT = 'a6b44f2e428bab60445f18a1303c999e3c517eed9f1dff0c46caac909a256c23';

async function tempDir(t: TestContext): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'chaosim-shard-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return dir;
}

async function openShard(
  dir: string,
  overrides: Partial<ShardConfig> = {},
  onEvents?: (events: PublishedEvent[]) => void,
) {
  return Shard.open({
    dir,
    shardId: 0,
    secret: SECRET,
    engineConfig: config,
    shardConfig: { ...defaultShardConfig, ...overrides },
    ...(onEvents ? { onEvents } : {}),
  });
}

const abandoned: Shard[] = [];

/**
 * Simulate a crash: abandon the shard without flushing or checkpointing, its
 * segment handle left open as a dead process leaves it. The shard stays
 * reachable so the handle is not collected mid run; the process closes it at
 * exit. Callers make sure no write is still in flight.
 */
function crash(shard: Shard): void {
  abandoned.push(shard);
}

async function waitFor(condition: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + 5000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function feed(shard: Shard, id: string, counts: readonly number[]): void {
  counts.forEach((count, code) => {
    if (count > 0) shard.ingest(id, code, count);
  });
}

/** Drive scenario sessions from `from` up to (not including) `to`, flushing every `flushEvery` ticks. */
async function drive(
  shard: Shard,
  workloads: Map<string, number[][]>,
  from: number,
  to: number,
  flushEvery = 25,
): Promise<void> {
  for (let tick = from; tick < to; tick += 1) {
    for (const [id, perTick] of workloads) feed(shard, id, perTick[tick]!);
    shard.step();
    if ((tick + 1) % flushEvery === 0) await shard.flush();
  }
  await shard.flush();
}

function workloadsFor(seeds: number[]): Map<string, number[][]> {
  return new Map(seeds.map((seed) => [`s${seed}`, scenarioCounts(seed, config.tickMs)]));
}

function activeSnapshot(shard: Shard, id: string): EngineSnapshot {
  const view = shard.inspect(id);
  assert.equal(view?.status, 'active', `${id} should be active`);
  return (view as { snapshot: EngineSnapshot }).snapshot;
}

function assertMatchesReference(shard: Shard, id: string, seed: number, ticks: number): void {
  const snapshot = activeSnapshot(shard, id);
  const reference = referenceEngine(seed, ticks);
  assert.equal(snapshot.tick, ticks);
  assert.deepEqual(snapshot.state, reference.state, `${id} state`);
  assert.equal(snapshot.trapTripped, reference.isTrapTripped, `${id} latch`);
  assert.deepEqual(snapshot.directorLog, reference.directorLog, `${id} director log`);
}

test('sessions stepped by a shard sign the single session results under their own ids', async (t) => {
  const dir = await tempDir(t);
  const { shard } = await openShard(dir);
  const seeds = [1, 2, 3, 4, 5];
  const workloads = workloadsFor(seeds);
  for (const id of workloads.keys()) assert.equal(shard.openSession(id), 'opened');

  await drive(shard, workloads, 0, 300);

  const signatures = new Map<number, string>();
  for (const seed of seeds) signatures.set(seed, (await shard.complete(`s${seed}`, AT)).signature);
  assert.equal(signatures.get(1), S1_SEED_1_FINGERPRINT);
  assert.equal(referenceFingerprint('s1', { secret: SECRET, seed: 1, completedAt: AT }), S1_SEED_1_FINGERPRINT);
  for (const seed of seeds) {
    const reference = referenceFingerprint(`s${seed}`, { secret: SECRET, seed, completedAt: AT });
    assert.equal(signatures.get(seed), reference, `seed ${seed}`);
  }
  await shard.close();
});

test('an attestation names its session, so it cannot pass for another one', async (t) => {
  const dir = await tempDir(t);
  const { shard } = await openShard(dir);
  shard.openSession('s1');
  shard.step();
  const attestation = await shard.complete('s1', AT);
  const { payload } = attestation;
  assert.ok(payload.schema === 'chaosim.sim001.attestation.v2', `schema ${payload.schema}`);
  assert.equal(payload.sessionId, 's1');
  assert.equal(verify(attestation, SECRET, { sessionId: 's1' }), true);
  assert.equal(verify(attestation, SECRET, { sessionId: 's2' }), false);

  const renamed = { ...attestation, payload: { ...payload, sessionId: 's2' } };
  assert.equal(verify(renamed, SECRET), false);
  assert.equal(verify(renamed, SECRET, { sessionId: 's2' }), false);
  await shard.close();
});

test('recovery after a crash is exact and the run finishes on the reference fingerprints', async (t) => {
  const dir = await tempDir(t);
  const seeds = [11, 12, 13];
  const workloads = workloadsFor(seeds);
  const { shard: first } = await openShard(dir);
  for (const id of workloads.keys()) first.openSession(id);
  await drive(first, workloads, 0, 137);
  await waitFor(() => first.takeStats().checkpoints.written === 2, 'checkpoints at ticks 50 and 100');
  await crash(first);

  const { shard: second, recovery } = await openShard(dir);
  assert.equal(recovery.ticksReplayed, 37, 'checkpoint at tick 100, crash at 137');
  assert.equal(recovery.tornBytesDiscarded, 0);
  assert.equal(second.tick, 137);
  for (const seed of seeds) assertMatchesReference(second, `s${seed}`, seed, 137);

  await drive(second, workloads, 137, 300);
  for (const seed of seeds) {
    const attestation = await second.complete(`s${seed}`, AT);
    const reference = referenceFingerprint(`s${seed}`, { secret: SECRET, seed, completedAt: AT });
    assert.equal(attestation.signature, reference, `seed ${seed}`);
  }
  await second.close();
});

test('recovery brings back the exact sessions, windows and all, after a completion moved one', async (t) => {
  const dir = await tempDir(t);
  const workloads = workloadsFor([71, 72, 73, 74]);
  const { shard: first } = await openShard(dir);
  for (const id of workloads.keys()) first.openSession(id);
  await drive(first, workloads, 0, 20);
  await first.complete('s72', AT); // the last session moves into its slot
  workloads.delete('s72');
  await drive(first, workloads, 20, 63);
  await waitFor(() => first.takeStats().checkpoints.written === 1, 'the checkpoint at tick 50');
  const before = [...workloads.keys()].map((id) => activeSnapshot(first, id));
  await crash(first);

  const { shard: second, recovery } = await openShard(dir);
  assert.equal(recovery.ticksReplayed, 13, 'checkpoint at tick 50, crash at 63');
  assert.deepEqual(
    [...workloads.keys()].map((id) => activeSnapshot(second, id)),
    before,
  );
  for (const seed of [71, 73, 74]) assertMatchesReference(second, `s${seed}`, seed, 63);
  await second.close();
});

test('ticks that never reached the disk are lost and everything flushed survives', async (t) => {
  const dir = await tempDir(t);
  const workloads = workloadsFor([21, 22]);
  const { shard: first } = await openShard(dir, { checkpointEveryTicks: 1000 });
  for (const id of workloads.keys()) first.openSession(id);
  await drive(first, workloads, 0, 40);
  for (let tick = 40; tick < 45; tick += 1) first.step();
  await crash(first);

  const { shard: second } = await openShard(dir, { checkpointEveryTicks: 1000 });
  assert.equal(second.tick, 40);
  assertMatchesReference(second, 's21', 21, 40);
  assertMatchesReference(second, 's22', 22, 40);
  await second.close();
});

test('recovering twice gives the same state and replays the same records', async (t) => {
  const dir = await tempDir(t);
  const workloads = workloadsFor([31, 32]);
  const { shard: first } = await openShard(dir);
  for (const id of workloads.keys()) first.openSession(id);
  await drive(first, workloads, 0, 60);
  await waitFor(() => first.takeStats().checkpoints.written === 1, 'checkpoint at tick 50');
  await crash(first);

  const a = await openShard(dir);
  const aStates = [...workloads.keys()].map((id) => activeSnapshot(a.shard, id));
  await crash(a.shard);
  const b = await openShard(dir);
  const bStates = [...workloads.keys()].map((id) => activeSnapshot(b.shard, id));

  assert.equal(a.recovery.recordsReplayed, 10);
  assert.equal(b.recovery.recordsReplayed, 10);
  assert.deepEqual(bStates, aStates);
  await b.shard.close();
});

test('director events wait for the log, and recovery republishes them as replayed', async (t) => {
  const dir = await tempDir(t);
  const published: PublishedEvent[] = [];
  const { shard } = await openShard(dir, { checkpointEveryTicks: 1000 }, (events) => published.push(...events));
  shard.openSession('hot');

  // 40 keystrokes and 40 backspaces a tick is stressed from the first tick:
  // panic climbs 2.5 a tick and crosses 75 on tick 29.
  let trappedAt = -1;
  for (let tick = 0; tick < 40 && trappedAt < 0; tick += 1) {
    shard.ingest('hot', kindCode('keystroke'), 40);
    shard.ingest('hot', kindCode('backspace'), 40);
    shard.step();
    if (activeSnapshot(shard, 'hot').trapTripped) trappedAt = tick;
  }
  assert.equal(trappedAt, 29);
  assert.equal(published.length, 0, 'nothing leaves the shard before its tick is durable');

  await shard.flush();
  const alert: PublishedEvent = {
    sessionId: 'hot',
    shardId: 0,
    replayed: false,
    event: { type: 'ceo_alert', tick: 29, message: CEO_ALERT_MESSAGE },
  };
  assert.deepEqual(published, [alert]);
  await crash(shard);

  const republished: PublishedEvent[] = [];
  const { shard: recovered } = await openShard(dir, { checkpointEveryTicks: 1000 }, (events) =>
    republished.push(...events),
  );
  assert.deepEqual(republished, [{ ...alert, replayed: true }]);
  await recovered.close();
});

test('recovery republishes alerts already folded into a checkpoint', async (t) => {
  const dir = await tempDir(t);
  const { shard } = await openShard(dir, { checkpointEveryTicks: 40 });
  shard.openSession('active');
  shard.openSession('sealed');
  for (let tick = 0; tick < 40; tick += 1) {
    for (const id of ['active', 'sealed']) {
      if (shard.inspect(id)?.status !== 'active') continue;
      shard.ingest(id, kindCode('keystroke'), 40);
      shard.ingest(id, kindCode('backspace'), 40);
    }
    shard.step();
    if (tick === 34) await shard.complete('sealed', AT);
  }
  // Both sessions trapped on tick 29. The checkpoint taken after tick 39 holds
  // the active session's alert in its director log and the sealed session's
  // in its retained attestation. An alert released just before a crash may
  // never have left the process, so recovery has to send both again.
  await waitFor(() => shard.takeStats().checkpoints.written === 1, 'the checkpoint at tick 40');
  await crash(shard);

  const republished: PublishedEvent[] = [];
  const { shard: recovered, recovery } = await openShard(dir, { checkpointEveryTicks: 40 }, (events) =>
    republished.push(...events),
  );
  assert.equal(recovery.recordsReplayed, 0, 'everything came from the checkpoint');
  assert.deepEqual(
    republished.map((p) => `${p.sessionId} tick ${p.event.tick} replayed ${p.replayed}`).sort(),
    ['active tick 29 replayed true', 'sealed tick 29 replayed true'],
  );
  await recovered.close();
});

test('completion is durable and idempotent across a crash', async (t) => {
  const dir = await tempDir(t);
  const workloads = workloadsFor([7]);
  const { shard: first } = await openShard(dir);
  first.openSession('s7');
  await drive(first, workloads, 0, 20);
  const original = await first.complete('s7', AT);
  await crash(first);

  const { shard: second } = await openShard(dir);
  assert.deepEqual(second.inspect('s7'), { status: 'completed', attestation: original });
  const retried = await second.complete('s7', '2030-01-01T00:00:00.000Z');
  assert.equal(retried.signature, original.signature);
  assert.equal(second.openSession('s7'), 'completed');
  await assert.rejects(second.complete('nobody', AT), { code: 'UNKNOWN_SESSION' });
  await second.close();
});

test('a completed id stays completed after its attestation is dropped, across a crash', async (t) => {
  const dir = await tempDir(t);
  const config = { checkpointEveryTicks: 2, completedRetention: 1, completedIdRetention: 5 };
  const { shard: first } = await openShard(dir, config);
  for (const id of ['a', 'b', 'c']) first.openSession(id);
  first.step();
  await first.complete('a', AT);
  await first.complete('b', AT); // only one attestation is kept, so a's goes but its id stays

  assert.equal(first.openSession('a'), 'completed', 'a reused id must not start a second session');
  await assert.rejects(first.complete('a', AT), { code: 'ATTESTATION_EXPIRED' });
  assert.deepEqual(first.inspect('a'), { status: 'expired' });

  first.step(); // tick 2 checkpoints, with a among the remembered ids
  await waitFor(() => first.takeStats().checkpoints.written === 1, 'the checkpoint at tick 2');
  await crash(first);

  const { shard: second, recovery } = await openShard(dir, config);
  assert.equal(recovery.recordsReplayed, 0, 'everything came from the checkpoint');
  assert.equal(second.openSession('a'), 'completed');
  assert.equal(second.openSession('b'), 'completed');
  await second.close();
});

test('a checkpoint prunes the log behind it and bounds the replay', async (t) => {
  const dir = await tempDir(t);
  const workloads = workloadsFor([41]);
  const { shard: first } = await openShard(dir);
  first.openSession('s41');
  await drive(first, workloads, 0, 120, 10);
  await waitFor(() => first.takeStats().checkpoints.written === 2, 'checkpoints at ticks 50 and 100');

  // seq 1 is the open, tick k is seq k + 2: the tick 100 checkpoint covers
  // seq 101, and the segment after it starts at seq 102.
  const files = (await readdir(dir)).sort();
  assert.deepEqual(files.filter((name) => name.startsWith('checkpoint-')), [checkpointName(101)]);
  assert.deepEqual(files.filter((name) => name.startsWith('wal-')), [segmentName(102)]);
  await crash(first);

  const { shard: second, recovery } = await openShard(dir);
  assert.equal(recovery.checkpointSeq, 101);
  assert.equal(recovery.ticksReplayed, 20);
  assertMatchesReference(second, 's41', 41, 120);
  await second.close();
});

test('checkpoints freeze on schedule, at most one waits, and the rest are skipped', async (t) => {
  const dir = await tempDir(t);
  const { shard } = await openShard(dir, { checkpointEveryTicks: 1 });
  shard.openSession('a');
  // Ten ticks with no await between them: every one is due a checkpoint and
  // none can reach the disk yet, so one runs, one waits, and eight are skipped.
  for (let tick = 0; tick < 10; tick += 1) shard.step();
  await waitFor(() => shard.takeStats().checkpoints.written === 2, 'the running and the waiting checkpoint');

  const stats = shard.takeStats().checkpoints;
  assert.equal(stats.skipped, 8);
  // seq 1 is the open and tick k is seq k + 2, so the waiting one froze after tick 1.
  assert.equal(stats.lastSeq, 3);
  assert.deepEqual((await readdir(dir)).filter((name) => name.startsWith('checkpoint-')), [checkpointName(3)]);
  await shard.close();
});

test('a torn record at the end of the log is discarded on recovery', async (t) => {
  const dir = await tempDir(t);
  const workloads = workloadsFor([51]);
  const { shard: first } = await openShard(dir, { checkpointEveryTicks: 1000 });
  first.openSession('s51');
  await drive(first, workloads, 0, 30);
  await crash(first);
  await appendFile(join(dir, segmentName(1)), Buffer.from([1, 2, 3, 4, 5]));

  const { shard: second, recovery } = await openShard(dir, { checkpointEveryTicks: 1000 });
  assert.equal(recovery.tornBytesDiscarded, 5);
  assertMatchesReference(second, 's51', 51, 30);
  await crash(second);

  // The repair has to stick: left in place, the torn bytes would sit inside a
  // sealed segment on the next recovery and read as corruption.
  const { shard: third, recovery: again } = await openShard(dir, { checkpointEveryTicks: 1000 });
  assert.equal(again.tornBytesDiscarded, 0);
  assertMatchesReference(third, 's51', 51, 30);
  await third.close();
});

test('a checkpoint temp file left by a crash is removed on recovery', async (t) => {
  const dir = await tempDir(t);
  const workloads = workloadsFor([61]);
  const { shard: first } = await openShard(dir);
  first.openSession('s61');
  await drive(first, workloads, 0, 10);
  await crash(first);
  await writeFile(join(dir, `${checkpointName(999)}.tmp`), 'half written');

  const { shard: second } = await openShard(dir);
  assert.deepEqual((await readdir(dir)).filter((name) => name.endsWith('.tmp')), []);
  assert.equal(second.tick, 10);
  await second.close();
});

test('inspect says whether what it shows is on disk yet', async (t) => {
  const dir = await tempDir(t);
  const { shard } = await openShard(dir, { checkpointEveryTicks: 1000 });
  shard.openSession('a');
  shard.step();
  const before = shard.inspect('a');
  assert.ok(before?.status === 'active');
  assert.equal(before.durable, false, 'a crash now would roll this tick back');

  await shard.flush();
  const after = shard.inspect('a');
  assert.ok(after?.status === 'active');
  assert.equal(after.durable, true);
  await shard.close();
});

test('an idle shard does not tick, and stray or excess telemetry is counted', async (t) => {
  const dir = await tempDir(t);
  const { shard } = await openShard(dir);
  shard.step();
  assert.equal(shard.tick, 0, 'no sessions, no tick');

  shard.ingest('ghost', kindCode('keystroke'), 3);
  assert.equal(shard.openSession('a'), 'opened');
  assert.equal(shard.openSession('a'), 'active');
  shard.ingest('a', kindCode('backspace'), 300);
  shard.step();

  const stats = shard.takeStats();
  assert.equal(stats.unknownSessionDrops, 3);
  assert.equal(stats.inboxDrops, 300 - defaultShardConfig.inboxCapPerKind);
  assert.equal(activeSnapshot(shard, 'a').state.backspaceCount, defaultShardConfig.inboxCapPerKind);
  await shard.close();
});

test(
  'a failed write stops the shard for good',
  { skip: process.getuid?.() === 0 ? 'root ignores directory permissions' : false },
  async (t) => {
    const dir = await tempDir(t);
    let fatal: NodeJS.ErrnoException | null = null;
    const { shard } = await Shard.open({
      dir,
      shardId: 0,
      secret: SECRET,
      engineConfig: config,
      shardConfig: { ...defaultShardConfig, checkpointEveryTicks: 5 },
      onFatal: (error) => {
        fatal = error;
      },
    });
    shard.openSession('a');
    for (let tick = 0; tick < 4; tick += 1) shard.step();
    await shard.flush();

    await chmod(dir, 0o500);
    try {
      shard.step(); // tick 5 checkpoints, which needs a new segment file
      await waitFor(() => fatal !== null, 'the fatal error');
      assert.equal(fatal!.code, 'EACCES');
      assert.throws(() => shard.step(), { code: 'EACCES' });
      await assert.rejects(shard.complete('a', AT), { code: 'EACCES' });
    } finally {
      await chmod(dir, 0o700);
    }
  },
);
