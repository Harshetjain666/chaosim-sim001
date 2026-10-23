import { test } from 'node:test';
import assert from 'node:assert/strict';

import { createWorldState } from '../src/worldState.ts';
import { SimulationEngine } from '../src/engine.ts';
import { Simulation } from '../src/simulation.ts';
import { defaultConfig } from '../src/config.ts';
import { nextPanicIndex, isStressed } from '../src/stressModel.ts';
import { canonicalize, sign, verify, type AttestationPayload } from '../src/attestation.ts';
import { runChaosLoadTest } from '../src/chaosLoadTest.ts';
import type { TelemetryEvent } from '../src/types.ts';

const SECRET = 'test-secret';

function attestedBody() {
  return {
    finalState: createWorldState(),
    tickCount: 1,
    trapTripped: false,
    directorEvents: [],
    completedAt: '2026-01-01T00:00:00.000Z',
  };
}

test('initial world state matches the spec', () => {
  const s = createWorldState();
  assert.equal(s.panicIndex, 0);
  assert.equal(s.codebaseHealth, 100);
  assert.equal(s.ceoAnxietyIndex, 10);
  assert.equal(s.keystrokeVelocity, 0);
  assert.equal(s.backspaceCount, 0);
});

test('stress curve adds 2.5 when stressed and decays otherwise', () => {
  const stressed = { ...createWorldState(), panicIndex: 10, backspaceCount: 11, keystrokeVelocity: 121 };
  assert.equal(isStressed(stressed, defaultConfig), true);
  assert.equal(nextPanicIndex(stressed, defaultConfig), 12.5);

  const calm = { ...createWorldState(), panicIndex: 10, backspaceCount: 0, keystrokeVelocity: 0 };
  assert.equal(isStressed(calm, defaultConfig), false);
  assert.ok(nextPanicIndex(calm, defaultConfig) < 10);
});

test('panic is clamped at the ceiling', () => {
  const hot = { ...createWorldState(), panicIndex: 99, backspaceCount: 50, keystrokeVelocity: 400 };
  assert.equal(nextPanicIndex(hot, defaultConfig), defaultConfig.panicCeil);
});

test('the trap fires exactly once even under sustained pressure', () => {
  const engine = new SimulationEngine();
  const heavy: TelemetryEvent[] = [];
  for (let i = 0; i < 40; i += 1) heavy.push({ t: 0, kind: 'keystroke' });
  for (let i = 0; i < 40; i += 1) heavy.push({ t: 0, kind: 'backspace' });

  let alerts = 0;
  for (let tick = 0; tick < 200; tick += 1) {
    const result = engine.step(heavy.map((e) => ({ ...e, t: tick * defaultConfig.tickMs })));
    alerts += result.events.length;
  }

  assert.equal(alerts, 1);
  assert.equal(engine.isTrapTripped, true);
  assert.equal(engine.state.codebaseHealth, 70);
  assert.equal(engine.state.ceoAnxietyIndex, 95);
});

test('canonicalize is stable regardless of key insertion order', () => {
  const a = canonicalize({ b: 1, a: 2, nested: { y: 1, x: 2 } });
  const b = canonicalize({ a: 2, nested: { x: 2, y: 1 }, b: 1 });
  assert.equal(a, b);
});

test('attestation verifies and rejects any tampering', () => {
  const result = runChaosLoadTest({ secret: SECRET, seed: 7, completedAt: '2026-01-01T00:00:00.000Z' });
  assert.equal(verify(result.attestation, SECRET), true);

  const tampered = {
    ...result.attestation,
    payload: { ...result.attestation.payload, finalState: { ...result.attestation.payload.finalState, codebaseHealth: 100 } },
  };
  assert.equal(verify(tampered, SECRET), false);

  assert.equal(verify(result.attestation, 'wrong-secret'), false);
});

test('a re-signed payload uses key-sorted canonical bytes', () => {
  const att = sign(
    {
      schema: 'chaosim.sim001.attestation.v1',
      finalState: createWorldState(),
      tickCount: 1,
      trapTripped: false,
      directorEvents: [],
      completedAt: '2026-01-01T00:00:00.000Z',
    },
    SECRET,
  );
  assert.equal(att.canonical.indexOf('"ceoAnxietyIndex"') < att.canonical.indexOf('"codebaseHealth"'), true);
  assert.equal(verify(att, SECRET), true);
});

test('only schemas v1 and v2 verify, and a v2 payload must name its session', () => {
  const v1 = sign({ schema: 'chaosim.sim001.attestation.v1', ...attestedBody() }, SECRET);
  const v2 = sign({ schema: 'chaosim.sim001.attestation.v2', sessionId: 's1', ...attestedBody() }, SECRET);
  assert.equal(verify(v1, SECRET), true);
  assert.equal(verify(v2, SECRET), true);

  // Both are signed with the right secret, but neither is a payload a verifier can read.
  const unknown = { schema: 'chaosim.sim001.attestation.v3', sessionId: 's1', ...attestedBody() };
  assert.equal(verify(sign(unknown as unknown as AttestationPayload, SECRET), SECRET), false);
  const unnamed = { schema: 'chaosim.sim001.attestation.v2', ...attestedBody() };
  assert.equal(verify(sign(unnamed as unknown as AttestationPayload, SECRET), SECRET), false);
});

test('an attestation verifies for a session only when it is v2 and names that session', () => {
  const v2 = sign({ schema: 'chaosim.sim001.attestation.v2', sessionId: 's1', ...attestedBody() }, SECRET);
  assert.equal(verify(v2, SECRET, { sessionId: 's1' }), true);
  assert.equal(verify(v2, SECRET, { sessionId: 's2' }), false);

  // A v1 payload names no session, so it could be any session's.
  const v1 = sign({ schema: 'chaosim.sim001.attestation.v1', ...attestedBody() }, SECRET);
  assert.equal(verify(v1, SECRET, { sessionId: 's1' }), false);
});

test('the run is deterministic: same seed yields the same fingerprint', () => {
  const a = runChaosLoadTest({ secret: SECRET, seed: 42, completedAt: '2026-01-01T00:00:00.000Z' });
  const b = runChaosLoadTest({ secret: SECRET, seed: 42, completedAt: '2026-01-01T00:00:00.000Z' });
  assert.equal(a.attestation.signature, b.attestation.signature);
  assert.deepEqual(a.attestation.payload.finalState, b.attestation.payload.finalState);
});

test('the chaos load test passes every acceptance check', () => {
  const result = runChaosLoadTest({ secret: SECRET, seed: 1 });
  for (const check of result.checks) {
    assert.equal(check.ok, true, `${check.label} -> ${check.detail}`);
  }
});
