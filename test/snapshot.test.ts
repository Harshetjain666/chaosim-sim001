import { test } from 'node:test';
import assert from 'node:assert/strict';

import { SimulationEngine, type EngineSnapshot } from '../src/engine.ts';
import { Simulation } from '../src/simulation.ts';
import { defaultConfig } from '../src/config.ts';
import { mulberry32 } from '../src/rng.ts';
import { telemetryForTick } from '../src/scenario.ts';
import { releaseTicks } from '../src/liveLoop.ts';
import type { TelemetryEvent } from '../src/types.ts';

const config = defaultConfig;
const DEV_SECRET = 'sim-001-dev-secret';
const FIXED_TIME = '2026-01-01T00:00:00.000Z';
/** `node src/index.ts --fixed-time` prints this for seed 1; it is the regression anchor. */
const SEED_1_FINGERPRINT = '27b60516caf74574b21dc5ceda61acb3f0a46a29ef552a81a2e13b228a2c24b6';

function scenarioBatches(seed: number, ticks = 300): TelemetryEvent[][] {
  const rng = mulberry32(seed);
  return Array.from({ length: ticks }, (_, tick) => telemetryForTick(tick, config.tickMs, rng));
}

/** Snapshots cross a disk or a thread boundary, so tests always round trip them through JSON. */
function thaw(snapshot: EngineSnapshot): EngineSnapshot {
  return JSON.parse(JSON.stringify(snapshot)) as EngineSnapshot;
}

function heavyBatch(tick: number): TelemetryEvent[] {
  const t = tick * config.tickMs;
  const batch: TelemetryEvent[] = [];
  for (let i = 0; i < 40; i += 1) batch.push({ t, kind: 'keystroke' });
  for (let i = 0; i < 40; i += 1) batch.push({ t, kind: 'backspace' });
  return batch;
}

test('an engine snapshot round trips through JSON and continues identically', () => {
  const batches = scenarioBatches(3);
  const original = new SimulationEngine(config);
  for (let tick = 0; tick < 150; tick += 1) original.step(batches[tick]);

  const restored = SimulationEngine.restore(thaw(original.snapshot()), config);
  assert.equal(restored.tick, 150);

  for (let tick = 150; tick < 300; tick += 1) {
    const expected = original.step(batches[tick]);
    assert.deepEqual(restored.step(batches[tick]), expected, `diverged at tick ${tick}`);
  }
  assert.equal(original.directorLog.length, 1);
  assert.deepEqual(restored.directorLog, original.directorLog);
});

test('restoring before the trap keeps its tick, restoring after it never fires again', () => {
  const batches = scenarioBatches(5);
  const reference = new SimulationEngine(config);
  let trapTick = -1;
  for (let tick = 0; tick < 300; tick += 1) {
    if (reference.step(batches[tick]).events.length > 0) trapTick = tick;
  }
  assert.ok(trapTick >= 200 && trapTick < 300, `trap should trip in phase 3, got ${trapTick}`);

  const before = new SimulationEngine(config);
  for (let tick = 0; tick < trapTick; tick += 1) before.step(batches[tick]);
  const resumedBefore = SimulationEngine.restore(thaw(before.snapshot()), config);
  assert.equal(resumedBefore.step(batches[trapTick]).events.length, 1);

  const after = new SimulationEngine(config);
  for (let tick = 0; tick <= trapTick; tick += 1) after.step(batches[tick]);
  const resumedAfter = SimulationEngine.restore(thaw(after.snapshot()), config);
  let alerts = 0;
  for (let tick = trapTick + 1; tick < trapTick + 101; tick += 1) {
    alerts += resumedAfter.step(heavyBatch(tick)).events.length;
  }
  assert.equal(alerts, 0);
  assert.equal(resumedAfter.isTrapTripped, true);
  assert.equal(resumedAfter.state.codebaseHealth, 70);
  assert.equal(resumedAfter.directorLog.length, 1);
});

test('a Simulation resumed from a snapshot signs the reference fingerprint', () => {
  const batches = scenarioBatches(1);
  const first = new Simulation({ secret: DEV_SECRET, config });
  for (let tick = 0; tick < 180; tick += 1) first.step(batches[tick]);

  const resumed = new Simulation({ secret: DEV_SECRET, config, snapshot: thaw(first.engine.snapshot()) });
  for (let tick = 180; tick < 300; tick += 1) resumed.step(batches[tick]);

  assert.equal(resumed.complete(FIXED_TIME).signature, SEED_1_FINGERPRINT);
});

test('releaseTicks releases whole ticks and carries the remainder', () => {
  assert.deepEqual(releaseTicks(350, 100, Infinity), { ticks: 3, accumulatorMs: 50 });
  assert.deepEqual(releaseTicks(99, 100, Infinity), { ticks: 0, accumulatorMs: 99 });
  // A capped catch up keeps the unreleased time so the clock never drifts.
  assert.deepEqual(releaseTicks(350, 100, 2), { ticks: 2, accumulatorMs: 150 });
});
