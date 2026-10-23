import { test } from 'node:test';
import assert from 'node:assert/strict';

import { runClusterDemo } from '../src/cluster/clusterDemo.ts';

test('the realtime demo survives a shard kill and keeps every invariant', async () => {
  const lines: string[] = [];
  const result = await runClusterDemo({
    secret: 'sim-001-dev-secret',
    shards: 2,
    sessions: 20,
    seconds: 3,
    killAtSecond: 1,
    log: (line) => lines.push(line),
  });

  assert.notEqual(result.recovery, null, 'shard 0 came back');
  assert.ok(lines.length >= 3, 'a health line every second');
  for (const check of result.checks) {
    assert.equal(check.ok, true, `${check.label}: ${check.detail}`);
  }
});
