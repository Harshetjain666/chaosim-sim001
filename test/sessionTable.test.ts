import { test } from 'node:test';
import assert from 'node:assert/strict';

import { defaultConfig, defaultShardConfig, type EngineConfig } from '../src/config.ts';
import { SimulationEngine, type EngineSnapshot } from '../src/engine.ts';
import { mulberry32 } from '../src/rng.ts';
import type { DirectorEvent } from '../src/scenarioDirector.ts';
import { Simulation, attestCompletion } from '../src/simulation.ts';
import { TELEMETRY_KINDS } from '../src/types.ts';
import { CorruptionError } from '../src/cluster/errors.ts';
import { expandBatch, kindCode } from '../src/cluster/inbox.ts';
import { MAX_TICK_COUNT, SessionTable } from '../src/cluster/sessionTable.ts';
import type { TickInput } from '../src/cluster/wal.ts';

const SECRET = 'sim-001-dev-secret';
const AT = '2026-01-01T00:00:00.000Z';
const CAP = defaultShardConfig.inboxCapPerKind;

/**
 * The reference for one session: a plain engine handed the batch its counts
 * stand for, stamped at the session's own time. That is exactly how the shard
 * stepped its sessions before they were columns.
 */
function stepReference(engine: SimulationEngine, counts: readonly number[] | undefined, config: EngineConfig) {
  return engine.step(counts ? expandBatch(counts, engine.tick * config.tickMs) : []);
}

function byId(events: { id: string; event: DirectorEvent }[]): { id: string; event: DirectorEvent }[] {
  return [...events].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
}

function snapshotOf(table: SessionTable, id: string): EngineSnapshot {
  const snapshot = table.snapshot(id);
  assert.ok(snapshot, `${id} should be in the table`);
  return snapshot;
}

const configs: [string, EngineConfig][] = [
  ['the 2s window', defaultConfig],
  ['a window that is not a whole number of ticks', { ...defaultConfig, windowMs: 450 }],
];

for (const [label, config] of configs) {
  test(`the columns step every session exactly as its own engine would, with ${label}`, () => {
    const rng = mulberry32(label.length);
    // A tiny first capacity, so the table grows several times on the way.
    const table = new SessionTable(config, CAP, 2);
    const engines = new Map<string, SimulationEngine>();
    /** Most events of a kind a session sends a tick: some stay calm, some panic and trip the trap. */
    const heat = new Map<string, number>();
    let opened = 0;
    let trapped = 0;
    let completed = 0;

    for (let tick = 0; tick < 250; tick += 1) {
      if (engines.size < 3 || rng() < 0.2) {
        const id = `s${opened}`;
        opened += 1;
        table.open(id);
        engines.set(id, new SimulationEngine(config));
        heat.set(id, rng() < 0.4 ? 12 : 3);
      }
      if (engines.size > 3 && rng() < 0.1) {
        // Complete one from anywhere, so the last slot moves into the hole.
        const ids = [...engines.keys()];
        const id = ids[Math.floor(rng() * ids.length)]!;
        const engine = engines.get(id)!;
        const expected = new Simulation({ secret: SECRET, config, snapshot: engine.snapshot() }).complete(AT);
        const summary = table.summary(id);
        assert.ok(summary);
        assert.deepEqual(attestCompletion(summary, AT, SECRET), expected, `${id} attestation`);
        table.remove(id);
        engines.delete(id);
        completed += 1;
      }

      // Calm sessions skip some ticks; now and then one kind floods to the cap.
      const inputs: TickInput[] = [];
      for (const id of engines.keys()) {
        const most = heat.get(id)!;
        if (most === 3 && rng() < 0.25) continue;
        const counts = TELEMETRY_KINDS.map(() => Math.floor(rng() * most));
        if (rng() < 0.01) counts[Math.floor(rng() * counts.length)] = CAP;
        inputs.push([id, counts]);
      }
      // A replayed record lists its sessions in whatever order it was written.
      inputs.reverse();

      const events = table.step(inputs);
      const countsById = new Map(inputs);
      const expected: { id: string; event: DirectorEvent }[] = [];
      for (const [id, engine] of engines) {
        for (const event of stepReference(engine, countsById.get(id), config).events) expected.push({ id, event });
      }
      trapped += expected.length;
      assert.deepEqual(byId(events), byId(expected), `events on tick ${tick}`);
      for (const [id, engine] of engines) {
        assert.deepEqual(snapshotOf(table, id), engine.snapshot(), `${id} after tick ${tick}`);
      }
    }

    assert.ok(trapped >= 5, `the workload should trip traps, tripped ${trapped}`);
    assert.ok(completed >= 5, `the workload should complete sessions, completed ${completed}`);
    assert.equal(table.size, engines.size);
    assert.deepEqual(byId(table.trapEvents()), byId([...engines].flatMap(([id, e]) => e.directorLog.map((event) => ({ id, event })))));
  });
}

test('the inbox counts per kind, caps a burst, and drains in canonical order', () => {
  const table = new SessionTable(defaultConfig, 3);
  table.open('a');
  table.open('b');
  assert.deepEqual(table.drain(), []);
  assert.equal(table.ingest('a', kindCode('backspace'), 2), 2);
  assert.equal(table.ingest('a', kindCode('backspace'), 5), 1);
  assert.equal(table.ingest('a', kindCode('keystroke'), 1), 1);
  assert.equal(table.ingest('ghost', kindCode('keystroke'), 1), null);
  assert.equal(table.ingest('b', TELEMETRY_KINDS.length, 1), 0, 'a code past the kinds lands nowhere');
  assert.equal(table.ingest('b', -1, 1), 0, 'a negative code lands nowhere');
  assert.deepEqual(table.drain(), [['a', [1, 3, 0, 0, 0]]]);
  assert.deepEqual(table.drain(), []);

  assert.deepEqual(expandBatch([1, 2, 0, 1, 0], 700), [
    { t: 700, kind: 'keystroke' },
    { t: 700, kind: 'backspace' },
    { t: 700, kind: 'backspace' },
    { t: 700, kind: 'fileSave' },
  ]);
});

test('a session moved into a freed slot keeps its state and its pending telemetry', () => {
  const table = new SessionTable(defaultConfig, CAP);
  const engine = new SimulationEngine(defaultConfig);
  for (const id of ['a', 'b', 'c']) table.open(id);
  for (let tick = 0; tick < 30; tick += 1) {
    table.step([['c', [3, 2, 0, 1, 0]]]);
    stepReference(engine, [3, 2, 0, 1, 0], defaultConfig);
  }
  table.ingest('c', kindCode('backspace'), 7);

  table.remove('a'); // c, in the last slot, moves into a's
  assert.equal(table.has('a'), false);
  assert.equal(table.size, 2);
  assert.deepEqual(snapshotOf(table, 'c'), engine.snapshot());
  assert.deepEqual(table.drain(), [['c', [0, 7, 0, 0, 0]]]);

  table.open('d'); // reuses the slot c left behind, which must start clean
  assert.deepEqual(snapshotOf(table, 'd'), new SimulationEngine(defaultConfig).snapshot());
});

test('a tick that does not fit the sessions is refused before anything changes', () => {
  const table = new SessionTable(defaultConfig, CAP);
  table.open('a');
  table.open('b');
  table.step([['a', [1, 1, 0, 0, 0]]]);
  const before = [snapshotOf(table, 'a'), snapshotOf(table, 'b')];

  const refused: [string, TickInput[]][] = [
    ['an unknown session', [['a', [1, 0, 0, 0, 0]], ['ghost', [1, 0, 0, 0, 0]]]],
    ['a repeated session', [['b', [1, 0, 0, 0, 0]], ['b', [1, 0, 0, 0, 0]]]],
    ['too few counts', [['a', [1, 0, 0, 0]]]],
    ['a negative count', [['a', [1, -1, 0, 0, 0]]]],
    ['a fractional count', [['a', [1, 0.5, 0, 0, 0]]]],
    ['a count the columns cannot hold', [['a', [MAX_TICK_COUNT + 1, 0, 0, 0, 0]]]],
  ];
  for (const [what, inputs] of refused) {
    assert.throws(() => table.step(inputs), CorruptionError, what);
    assert.deepEqual([snapshotOf(table, 'a'), snapshotOf(table, 'b')], before, `${what} changed state`);
  }

  // The largest count the columns hold lands whole.
  table.step([['b', [0, MAX_TICK_COUNT, 0, 0, 0]]]);
  assert.equal(table.summary('b')?.state.backspaceCount, MAX_TICK_COUNT);
});

test('columns loaded into a new table carry on exactly where they left off', () => {
  const rng = mulberry32(99);
  const table = new SessionTable(defaultConfig, CAP);
  const ids = ['a', 'b', 'c', 'd', 'e'];
  const randomInputs = (live: readonly string[]): TickInput[] =>
    live.filter(() => rng() < 0.8).map((id) => [id, TELEMETRY_KINDS.map(() => Math.floor(rng() * 5))]);
  for (const id of ids) table.open(id);
  for (let tick = 0; tick < 45; tick += 1) table.step(randomInputs(ids));
  table.remove('b');
  const live = ['a', 'e', 'c', 'd'];

  // A checkpoint is a copy of the columns, so the copy must be enough, even
  // in a table whose slots other sessions used before.
  const restored = new SessionTable(defaultConfig, CAP);
  restored.open('used');
  restored.step([['used', [9, 9, 9, 9, 9]]]);
  restored.ingest('used', kindCode('keystroke'), 3);
  restored.remove('used');
  restored.load(structuredClone(table.columns()));
  assert.equal(restored.size, 4);
  for (let tick = 45; tick < 90; tick += 1) {
    const inputs = randomInputs(live);
    assert.deepEqual(byId(restored.step(inputs)), byId(table.step(inputs)));
  }
  for (const id of live) assert.deepEqual(snapshotOf(restored, id), snapshotOf(table, id), id);
  assert.deepEqual(restored.columns(), table.columns());
});

test('columns from a different window length are refused', () => {
  const table = new SessionTable(defaultConfig, CAP);
  table.open('a');
  const other = new SessionTable({ ...defaultConfig, windowMs: 1000 }, CAP);
  assert.throws(() => other.load(table.columns()), CorruptionError);
});

test('a layout the columns cannot hold exactly is refused up front', () => {
  assert.throws(() => new SessionTable({ ...defaultConfig, windowMs: 50 }, CAP), RangeError, 'window shorter than a tick');
  assert.throws(() => new SessionTable({ ...defaultConfig, tickMs: 0.5 }, CAP), RangeError, 'fractional tick');
  assert.throws(() => new SessionTable(defaultConfig, MAX_TICK_COUNT + 1), RangeError, 'cap above the column width');
  assert.throws(() => new SessionTable(defaultConfig, 2.5), RangeError, 'fractional cap');
});
