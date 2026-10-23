import { describe, test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, open, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { defaultConfig, defaultShardConfig } from '../src/config.ts';
import { runChaosLoadTest } from '../src/chaosLoadTest.ts';
import { ShardCluster, exitAction, type ClusterOptions } from '../src/cluster/cluster.ts';
import { ClusterError } from '../src/cluster/errors.ts';
import { shardFor } from '../src/cluster/placement.ts';
import { segmentName } from '../src/cluster/wal.ts';
import { ScenarioDriver, referenceEngine, type DriverSession } from '../src/cluster/workload.ts';

const SECRET = 'sim-001-dev-secret';
const AT = '2026-01-01T00:00:00.000Z';

const referenceSignatures = new Map<number, string>();
function referenceSignature(seed: number): string {
  let signature = referenceSignatures.get(seed);
  if (signature === undefined) {
    signature = runChaosLoadTest({ secret: SECRET, seed, completedAt: AT }).attestation.signature;
    referenceSignatures.set(seed, signature);
  }
  return signature;
}

interface Harness {
  dir: string;
  /** Start a manual clock cluster on `dir`; it is closed, cleanly, when the test ends. */
  start(overrides?: Partial<ClusterOptions>): Promise<ShardCluster>;
}

/**
 * A data directory and every cluster a test starts on it. One after hook
 * closes the clusters and only then removes the directory: node:test runs
 * after hooks in the order they were registered, so two separate hooks would
 * delete the directory out from under live shards. A close that is not clean
 * fails the test.
 */
async function harness(t: TestContext): Promise<Harness> {
  const dir = await mkdtemp(join(tmpdir(), 'chaosim-cluster-'));
  const clusters: ShardCluster[] = [];
  t.after(async () => {
    try {
      await Promise.all(clusters.map((cluster) => cluster.close()));
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
  return {
    dir,
    async start(overrides = {}) {
      const cluster = await ShardCluster.start({ dataDir: dir, shards: 4, secret: SECRET, clock: 'manual', ...overrides });
      clusters.push(cluster);
      return cluster;
    },
  };
}

/** Scenario sessions with distinct seeds. One id is deliberately not ASCII. */
function plan(count: number): DriverSession[] {
  return Array.from({ length: count }, (_, i) => ({
    id: i === 0 ? 'café-☕-0' : `session-${i}`,
    seed: 100 + i,
  }));
}

function idOnShard(shard: number, shards: number): string {
  for (let i = 0; ; i += 1) {
    const id = `probe-${i}`;
    if (shardFor(id, shards) === shard) return id;
  }
}

async function runToEnd(driver: ScenarioDriver): Promise<void> {
  while (await driver.round()) {
    // Each round ingests for every session and then advances with no await
    // in between, so this also proves telemetry is never overtaken by the
    // control message that follows it.
  }
}

function assertReferenceFingerprints(driver: ScenarioDriver, sessions: DriverSession[]): void {
  for (const { id, seed } of sessions) {
    assert.equal(driver.attestations.get(id)?.signature, referenceSignature(seed), id);
  }
}

test('placement is stable and balanced', () => {
  const counts = [0, 0, 0, 0];
  for (let i = 0; i < 10_000; i += 1) {
    const shard = shardFor(`session-${i}`, 4);
    assert.equal(shardFor(`session-${i}`, 4), shard);
    counts[shard] = counts[shard]! + 1;
  }
  for (const count of counts) assert.ok(count > 2250 && count < 2750, `unbalanced: ${counts.join(', ')}`);
});

test('close reports a shard that could not shut down cleanly', async (t) => {
  const { dir } = await harness(t);
  const cluster = await ShardCluster.start({ dataDir: dir, shards: 2, secret: SECRET, clock: 'manual' });
  await cluster.openSession(idOnShard(0, 2));
  // Take the shard's directory away so its final checkpoint has nowhere to go.
  await rm(join(dir, 'shard-0'), { recursive: true, force: true });
  await assert.rejects(cluster.close(), /shard 0.*ENOENT/s);
});

test('a shard that dies after coming up is restarted even while the others are still starting', () => {
  assert.equal(exitAction({ closing: false, booting: true, wasReady: true }), 'restart');
  assert.equal(
    exitAction({ closing: false, booting: true, wasReady: false }),
    'fail',
    'start() reports a shard that never came up',
  );
  assert.equal(exitAction({ closing: false, booting: false, wasReady: true }), 'restart');
  assert.equal(exitAction({ closing: true, booting: false, wasReady: true }), 'close');
});

test('growing the cluster moves only the sessions the new shard wins', () => {
  let moved = 0;
  for (let i = 0; i < 10_000; i += 1) {
    const before = shardFor(`session-${i}`, 4);
    const after = shardFor(`session-${i}`, 5);
    if (after !== before) {
      moved += 1;
      assert.equal(after, 4, 'a session only ever moves to the new shard');
    }
  }
  assert.ok(moved > 1500 && moved < 2500, `moved ${moved} of 10000`);
});

// Every test below owns its own directory and its own workers, so they run
// side by side; most of their time is spent waiting on fsync.
describe('a running cluster', { concurrency: true }, () => {
  test('a sharded cluster reproduces the single session fingerprints', async (t) => {
    const { start } = await harness(t);
    const cluster = await start();
    const sessions = plan(24);
    assert.equal(new Set(sessions.map((s) => cluster.shardOf(s.id))).size, 4, 'every shard hosts sessions');

    const driver = new ScenarioDriver(cluster, sessions, AT);
    await driver.openAll();
    await runToEnd(driver);
    assertReferenceFingerprints(driver, sessions);
  });

  test('a killed shard restarts, recovers exactly, and the run still matches the reference', async (t) => {
    const { start } = await harness(t);
    const cluster = await start();
    const sessions = plan(16);
    const driver = new ScenarioDriver(cluster, sessions, AT);
    await driver.openAll();
    for (let round = 0; round < 120; round += 1) await driver.round();

    const victim = cluster.shardOf(sessions[0]!.id);
    const recovered = cluster.killShard(victim);
    // Asked while the shard is down: held, then answered by the recovered shard.
    const queued = cluster.inspect(sessions[0]!.id);
    const report = await recovered;
    assert.equal(report.shardId, victim);
    assert.equal(report.tick, 120, 'every acknowledged round survived');
    assert.notEqual(report.checkpointSeq, null);
    assert.equal((await queued)?.status, 'active');

    for (const { id, seed } of sessions.filter((s) => cluster.shardOf(s.id) === victim)) {
      const view = await cluster.inspect(id);
      assert.ok(view?.status === 'active', `${id} should be active`);
      const reference = referenceEngine(seed, 120);
      assert.equal(view.snapshot.tick, 120);
      assert.deepEqual(view.snapshot.state, reference.state, id);
      assert.deepEqual(view.snapshot.directorLog, reference.directorLog, id);
    }

    await driver.resync();
    await runToEnd(driver);
    assertReferenceFingerprints(driver, sessions);
  });

  test('a data dir in use is refused, and a lock left by a dead process is taken over', async (t) => {
    const { dir, start } = await harness(t);
    const first = await start({ shards: 2 });
    // Two clusters writing one directory would silently destroy each other's logs.
    await assert.rejects(start({ shards: 2 }), { code: 'DATA_DIR_LOCKED' });
    await first.close();

    // A lock whose owner has exited, after a crash or a SIGKILL, must not block a restart.
    const { pid } = spawnSync(process.execPath, ['-e', '']);
    await writeFile(join(dir, 'LOCK'), `${pid}\n`);
    const second = await start({ shards: 2 });
    assert.equal(await second.openSession('after-takeover'), 'opened');
  });

  test('open and complete retried after a crash give the original answers', async (t) => {
    const { start } = await harness(t);
    const cluster = await start({ shards: 1 });
    await cluster.openSession('sealed');
    await cluster.openSession('live');
    for (let tick = 0; tick < 5; tick += 1) {
      cluster.ingest('sealed', 'keystroke', 3);
      await cluster.advance();
    }
    const original = await cluster.complete('sealed', AT);
    await cluster.killShard(0);

    // A client that never heard back retries. Nothing it does may fork a session.
    assert.equal(await cluster.openSession('live'), 'active');
    assert.equal(await cluster.openSession('sealed'), 'completed');
    const retried = await cluster.complete('sealed', '2030-01-01T00:00:00.000Z');
    assert.equal(retried.signature, original.signature);
    assert.equal(retried.payload.completedAt, AT);
  });

  test('telemetry and requests sent while a shard restarts reach it in order', async (t) => {
    const { start } = await harness(t);
    const cluster = await start({ shards: 1 });
    await cluster.openSession('a');

    const recovered = cluster.killShard(0);
    cluster.ingest('a', 'backspace', 5);
    const advanced = cluster.advance();
    const seen = cluster.inspect('a');
    await recovered;
    await advanced;

    const view = await seen;
    assert.ok(view?.status === 'active');
    assert.equal(view.snapshot.tick, 1);
    assert.equal(view.snapshot.state.backspaceCount, 5, 'the held telemetry landed in the held tick');
  });

  test('requests for a restarting shard are held only up to a limit', async (t) => {
    const { start } = await harness(t);
    const cluster = await start({
      shards: 1,
      requestBufferLimit: 3,
      restart: { maxRestarts: 5, windowMs: 60_000, baseDelayMs: 300 },
    });
    await cluster.openSession('a');

    const recovered = cluster.killShard(0);
    const held = [cluster.inspect('a'), cluster.inspect('a'), cluster.inspect('a')];
    await assert.rejects(cluster.inspect('a'), (error: unknown) => {
      assert.ok(error instanceof ClusterError);
      assert.equal(error.code, 'SHARD_UNAVAILABLE');
      assert.equal(error.retryable, true);
      return true;
    });
    await recovered;
    for (const view of await Promise.all(held)) assert.equal(view?.status, 'active');
  });

  test('an advance cut short by a crash is reported as not safe to retry', async (t) => {
    const { start } = await harness(t);
    const cluster = await start({ shards: 1 });
    await Promise.all(Array.from({ length: 16 }, (_, i) => cluster.openSession(`s${i}`)));

    // Long enough that the kill always lands while the shard is still ticking.
    const advancing = cluster.advance(100_000);
    const recovered = cluster.killShard(0);
    await assert.rejects(advancing, (error: unknown) => {
      // Some of its ticks may already be durable, so sending it again could
      // run them twice. The caller has to resync from inspect() instead.
      assert.ok(error instanceof ClusterError);
      assert.equal(error.code, 'ADVANCE_INTERRUPTED');
      assert.equal(error.retryable, false);
      return true;
    });
    await recovered;
  });

  test('restarting the whole cluster resumes from disk', async (t) => {
    const { start } = await harness(t);
    const sessions = plan(8);
    const first = await start();
    const before = new ScenarioDriver(first, sessions, AT);
    await before.openAll();
    for (let round = 0; round < 80; round += 1) await before.round();
    await first.close();

    const second = await start();
    for (const report of second.recoveries) {
      assert.equal(report.ticksReplayed, 0, 'a clean shutdown leaves a final checkpoint');
    }
    const after = new ScenarioDriver(second, sessions, AT);
    await after.resync();
    for (const { id } of sessions) assert.equal(after.localTick(id), 80);
    await runToEnd(after);
    assertReferenceFingerprints(after, sessions);
  });

  test('the cluster refuses a data dir written with another shard count or engine config', async (t) => {
    const { dir, start } = await harness(t);
    const first = await start({ shards: 2 });
    await first.close();

    await assert.rejects(
      ShardCluster.start({ dataDir: dir, shards: 3, secret: SECRET, clock: 'manual' }),
      { code: 'MANIFEST_MISMATCH' },
    );
    await assert.rejects(
      ShardCluster.start({
        dataDir: dir,
        shards: 2,
        secret: SECRET,
        clock: 'manual',
        engineConfig: { ...defaultConfig, trapPanicThreshold: 80 },
      }),
      { code: 'MANIFEST_MISMATCH' },
    );
  });

  test('the cluster refuses a data dir from another manifest version or signing secret', async (t) => {
    const { dir, start } = await harness(t);
    const first = await start({ shards: 2 });
    await first.close();

    // A new secret would re-sign completions replayed from the logs differently
    // from the attestations already handed out for them.
    await assert.rejects(start({ shards: 2, secret: 'a-different-secret' }), (error: unknown) => {
      assert.ok(error instanceof ClusterError);
      assert.equal(error.code, 'MANIFEST_MISMATCH');
      assert.match(error.message, /secret/);
      return true;
    });

    const manifest = JSON.parse(await readFile(join(dir, 'cluster.json'), 'utf8')) as Record<string, unknown>;
    await writeFile(join(dir, 'cluster.json'), JSON.stringify({ ...manifest, version: 2 }));
    await assert.rejects(start({ shards: 2 }), { code: 'MANIFEST_MISMATCH', message: /version 2/ });
  });

  test('the realtime clock ticks on its own and reports health', async (t) => {
    const { start } = await harness(t);
    const cluster = await start({ shards: 2, clock: 'realtime' });
    await cluster.openSession('a');
    await cluster.openSession('b');
    const pump = setInterval(() => {
      cluster.ingest('a', 'keystroke', 2);
      cluster.ingest('b', 'backspace');
    }, 20);
    await new Promise((resolve) => setTimeout(resolve, 700));
    clearInterval(pump);

    for (const id of ['a', 'b']) {
      const view = await cluster.inspect(id);
      assert.ok(view?.status === 'active' && view.snapshot.tick >= 4, `${id} should have ticked by itself`);
      // Ticks run in the background here, yet what the cluster shows can never be rolled back.
      assert.equal(view.durable, true, `${id} was shown before it was on disk`);
    }
    const stats = await cluster.stats();
    for (const shard of stats.shards) {
      assert.equal(shard.state, 'up');
      if (shard.stats === null || shard.stats.sessions === 0) continue;
      assert.ok(shard.stats.wal.fsyncs > 0, 'ticks were made durable');
      // A nearly idle shard has almost no lag. The 10ms sampling interval of
      // the delay monitor must not be reported as lag.
      assert.ok(shard.stats.loop.lagP99Ms < 9, `lag p99 ${shard.stats.loop.lagP99Ms}ms`);
      assert.ok(shard.stats.loop.busyRatio >= 0 && shard.stats.loop.busyRatio < 1);
    }
    await assert.rejects(cluster.advance(), { code: 'BAD_REQUEST' });
    assert.equal(cluster.ingest('a', 'nonsense' as never), false);
  });

  test('a shard that keeps crashing is marked failed and the rest keep serving', async (t) => {
    const { dir, start } = await harness(t);
    const cluster = await start({
      shards: 2,
      restart: { maxRestarts: 2, windowMs: 60_000, baseDelayMs: 1 },
    });
    const doomed = idOnShard(0, 2);
    await cluster.openSession(doomed);

    // Damage the shard's only segment so every recovery attempt refuses to start.
    const segment = await open(join(dir, 'shard-0', segmentName(1)), 'r+');
    await segment.write(Buffer.from('GARBAGE!'), 0, 8, 0);
    await segment.close();

    await assert.rejects(cluster.killShard(0), { code: 'SHARD_FAILED' });
    await assert.rejects(cluster.inspect(doomed), { code: 'SHARD_FAILED' });
    assert.equal(cluster.ingest(doomed, 'keystroke'), false);

    assert.equal(await cluster.openSession(idOnShard(1, 2)), 'opened');
    const shards = (await cluster.stats()).shards;
    assert.deepEqual(shards.map((s) => s.state), ['failed', 'up']);
  });

  test('shard config never needs to match between runs', async (t) => {
    const { start } = await harness(t);
    const sessions = plan(4);
    const first = await start({ shards: 2, shardConfig: { ...defaultShardConfig, checkpointEveryTicks: 7 } });
    const before = new ScenarioDriver(first, sessions, AT);
    await before.openAll();
    for (let round = 0; round < 30; round += 1) await before.round();
    await first.close();

    const second = await start({ shards: 2 });
    const after = new ScenarioDriver(second, sessions, AT);
    await after.resync();
    await runToEnd(after);
    assertReferenceFingerprints(after, sessions);
  });
});
