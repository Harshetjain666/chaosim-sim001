import { test } from 'node:test';
import assert from 'node:assert/strict';

import { runCrashDrill } from '../src/cluster/crashDrill.ts';

test('a cluster killed with SIGKILL recovers exactly and finishes on the reference fingerprints', async () => {
  const result = await runCrashDrill({
    secret: 'sim-001-dev-secret',
    shards: 3,
    sessions: 12,
    killAfterRound: 130,
  });

  assert.ok(result.killedAfterRound >= 130, `killed after ${result.killedAfterRound} rounds`);
  assert.equal(result.recoveries.length, 3);
  assert.ok(
    result.recoveries.every((r) => r.checkpointSeq !== null),
    'with checkpoints every 50 ticks, each shard recovers from one',
  );
  for (const check of result.checks) {
    assert.equal(check.ok, true, `${check.label}: ${check.detail}`);
  }
});
