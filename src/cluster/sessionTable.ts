import type { EngineConfig } from '../config.ts';
import type { EngineSnapshot } from '../engine.ts';
import { ceoAlert, type DirectorEvent } from '../scenarioDirector.ts';
import { keystrokeVelocity, nextPanicIndex } from '../stressModel.ts';
import { TELEMETRY_KINDS, type TelemetryEvent, type WorldState } from '../types.ts';
import { createWorldState } from '../worldState.ts';
import { CorruptionError } from './errors.ts';
import { expandBatch, kindCode } from './inbox.ts';
import type { TickInput } from './wal.ts';

const KIND_COUNT = TELEMETRY_KINDS.length;
const KEYSTROKE = kindCode('keystroke');
const BACKSPACE = kindCode('backspace');

/** Most events of one kind a session can land in one tick: window counts are 16 bit. */
export const MAX_TICK_COUNT = 0xffff;

const WORLD_FIELDS = [
  'keystrokeVelocity',
  'backspaceCount',
  'panicIndex',
  'codebaseHealth',
  'ceoAnxietyIndex',
] as const satisfies readonly (keyof WorldState)[];

/**
 * The per session numbers, one Float64Array each, in the order a checkpoint
 * stores them: the session's tick, its WorldState, and the tick its trap
 * tripped on, or -1 while it is armed.
 */
export const NUMBER_COLUMNS = ['tick', ...WORLD_FIELDS, 'trapTick'] as const;

export type NumberColumn = (typeof NUMBER_COLUMNS)[number];

/**
 * A table's sessions in slot order, which is everything a checkpoint keeps
 * of them. Each number column has one entry per session, and `window` holds
 * `windowTicks` ticks of per kind counts per session.
 */
export type SessionColumns = Record<NumberColumn, Float64Array> & {
  ids: string[];
  windowTicks: number;
  window: Uint16Array;
};

export interface SessionEvent {
  id: string;
  event: DirectorEvent;
}

/**
 * Why session columns cannot hold sessions run with these settings, or null
 * when they can. The window must span whole ticks of whole milliseconds,
 * which keeps the arithmetic below exact, and the inbox cap must fit the
 * width of a window count.
 */
export function columnLayoutProblem(config: EngineConfig, capPerKind: number): string | null {
  const { tickMs, windowMs } = config;
  if (!Number.isSafeInteger(tickMs) || tickMs < 1 || !Number.isSafeInteger(windowMs) || windowMs < tickMs) {
    return `session columns need a whole millisecond tick and a window at least one tick long, got tickMs ${tickMs} and windowMs ${windowMs}`;
  }
  if (!Number.isInteger(capPerKind) || capPerKind < 0 || capPerKind > MAX_TICK_COUNT) {
    return `inboxCapPerKind must be a whole number from 0 to ${MAX_TICK_COUNT}, got ${capPerKind}`;
  }
  return null;
}

/**
 * A shard's sessions as columns: the structure of arrays layout from
 * Challenge 1.2 of the brief.
 *
 * Each session owns a dense slot, and every number it has lives in a typed
 * array indexed by that slot. Its sliding window is a ring of per tick kind
 * counts with running totals. That holds exactly what the engine's event list
 * holds, because a shard stamps every event at the start of its tick and the
 * window evicts a whole tick at a time: after tick T it holds ticks
 * T - windowTicks + 1 through T. So a tick costs the same whatever the
 * window holds, the table allocates nothing per session per tick, and a
 * checkpoint is a copy of the columns rather than a walk over objects.
 *
 * `step()` is SimulationEngine.step() for every session at once. It runs
 * the stress model's own functions and mirrors the director's latch, and
 * the tests hold it to the plain engine, snapshot for snapshot, window
 * included. Removing a session moves the last slot into its place, so slot
 * order is a pure function of the operations applied, and replay rebuilds
 * it exactly.
 */
export class SessionTable {
  readonly windowTicks: number;

  private readonly config: EngineConfig;
  private readonly capPerKind: number;
  /** Counts one session's window ring holds: windowTicks ticks of every kind. */
  private readonly ringLength: number;
  private readonly initial = createWorldState();
  /** Scratch world the stress model reads, so the columns run its code without allocating. */
  private readonly world = createWorldState();
  private readonly slotOf = new Map<string, number>();
  private readonly ids: string[] = [];
  private count = 0;
  private capacity: number;

  private numbers: Record<NumberColumn, Float64Array>;
  /** Per tick kind counts, by slot; within a slot, by tick % windowTicks. */
  private window: Uint16Array;
  /** Each session's window summed per kind, kept as ticks enter and leave it. */
  private windowTotals: Uint32Array;
  /** Telemetry counted since the last tick, by slot and kind. */
  private inbox: Uint16Array;
  private inboxUsed: Uint8Array;
  /** The step a session's input last landed in, to catch a record that repeats one. */
  private landed: Float64Array;
  private steps = 0;

  constructor(config: EngineConfig, capPerKind: number, capacity = 1024) {
    const problem = columnLayoutProblem(config, capPerKind);
    if (problem) throw new RangeError(problem);
    this.config = config;
    this.capPerKind = capPerKind;
    this.windowTicks = Math.floor(config.windowMs / config.tickMs);
    this.ringLength = this.windowTicks * KIND_COUNT;
    this.capacity = Math.max(1, capacity);
    this.numbers = numberColumns(() => new Float64Array(this.capacity));
    this.window = new Uint16Array(this.capacity * this.ringLength);
    this.windowTotals = new Uint32Array(this.capacity * KIND_COUNT);
    this.inbox = new Uint16Array(this.capacity * KIND_COUNT);
    this.inboxUsed = new Uint8Array(this.capacity);
    this.landed = new Float64Array(this.capacity);
  }

  get size(): number {
    return this.count;
  }

  has(id: string): boolean {
    return this.slotOf.has(id);
  }

  /** Add a session in the spec's initial state, in the next slot. */
  open(id: string): void {
    if (this.slotOf.has(id)) throw new Error(`session ${id} is already in the table`);
    if (this.count === this.capacity) this.grow(this.capacity * 2);
    const slot = this.count;
    this.count += 1;
    this.ids.push(id);
    this.slotOf.set(id, slot);

    const { numbers, initial } = this;
    numbers.tick[slot] = 0;
    for (const field of WORLD_FIELDS) numbers[field][slot] = initial[field];
    numbers.trapTick[slot] = -1;
    // A slot can be one a removed session left behind.
    this.window.fill(0, slot * this.ringLength, (slot + 1) * this.ringLength);
    this.windowTotals.fill(0, slot * KIND_COUNT, (slot + 1) * KIND_COUNT);
    this.inbox.fill(0, slot * KIND_COUNT, (slot + 1) * KIND_COUNT);
    this.inboxUsed[slot] = 0;
  }

  /** Drop a session. The last slot moves into its place, so the slots stay dense. */
  remove(id: string): void {
    const slot = this.slotOf.get(id);
    if (slot === undefined) throw new Error(`session ${id} is not in the table`);
    const last = this.count - 1;
    if (slot !== last) this.move(last, slot);
    this.slotOf.delete(id);
    this.ids.pop();
    this.count = last;
  }

  /**
   * Count telemetry into a session's inbox for its next tick. Returns how
   * many fit under the cap, or null when there is no such session.
   */
  ingest(id: string, code: number, count: number): number | null {
    const slot = this.slotOf.get(id);
    if (slot === undefined) return null;
    // The router only sends known kinds and whole counts. Anything else could
    // land in another session's counters, so it lands nowhere.
    if (!Number.isInteger(code) || code < 0 || code >= KIND_COUNT || !Number.isInteger(count)) return 0;
    const at = slot * KIND_COUNT + code;
    const current = this.inbox[at]!;
    const accepted = Math.max(0, Math.min(count, this.capPerKind - current));
    if (accepted > 0) {
      this.inbox[at] = current + accepted;
      this.inboxUsed[slot] = 1;
    }
    return accepted;
  }

  /** Empty every inbox into this tick's inputs: the sessions that had telemetry, in slot order. */
  drain(): TickInput[] {
    const { inbox, inboxUsed, ids } = this;
    const inputs: TickInput[] = [];
    for (let slot = 0; slot < this.count; slot += 1) {
      if (inboxUsed[slot] === 0) continue;
      const counts: number[] = [];
      for (let at = slot * KIND_COUNT; at < (slot + 1) * KIND_COUNT; at += 1) {
        counts.push(inbox[at]!);
        inbox[at] = 0;
      }
      inboxUsed[slot] = 0;
      inputs.push([ids[slot]!, counts]);
    }
    return inputs;
  }

  /**
   * Step every session one tick, each with the batch its counts stand for,
   * or an empty one if `inputs` does not list it. The inputs are checked
   * before anything moves, so a tick record that does not fit these sessions
   * changes nothing. Returns the director events, in slot order.
   */
  step(inputs: readonly TickInput[]): SessionEvent[] {
    this.steps += 1;
    const slots = new Int32Array(inputs.length);
    for (let i = 0; i < inputs.length; i += 1) {
      const [id, counts] = inputs[i]!;
      const slot = this.slotOf.get(id);
      if (slot === undefined || this.landed[slot] === this.steps) {
        throw new CorruptionError(`a tick record carries input for unknown or repeated session ${id}`);
      }
      if (!fitsColumns(counts)) {
        throw new CorruptionError(`a tick record gives session ${id} counts the columns cannot hold: ${JSON.stringify(counts)}`);
      }
      this.landed[slot] = this.steps;
      slots[i] = slot;
    }

    const { windowTicks, ringLength, window, windowTotals } = this;
    const { tick } = this.numbers;

    // 1. Every window slides one tick. The ring position a session is about
    //    to write holds the tick that falls out of its window now.
    for (let slot = 0; slot < this.count; slot += 1) {
      const at = slot * ringLength + (tick[slot]! % windowTicks) * KIND_COUNT;
      const totals = slot * KIND_COUNT;
      for (let k = 0; k < KIND_COUNT; k += 1) {
        windowTotals[totals + k] = windowTotals[totals + k]! - window[at + k]!;
        window[at + k] = 0;
      }
    }

    // 2. This tick's counts land in the positions just freed.
    for (let i = 0; i < inputs.length; i += 1) {
      const slot = slots[i]!;
      const counts = inputs[i]![1];
      const at = slot * ringLength + (tick[slot]! % windowTicks) * KIND_COUNT;
      const totals = slot * KIND_COUNT;
      for (let k = 0; k < KIND_COUNT; k += 1) {
        window[at + k] = counts[k]!;
        windowTotals[totals + k] = windowTotals[totals + k]! + counts[k]!;
      }
    }

    // 3. Metrics, stress and the director, in SimulationEngine.step's order.
    const { config, world } = this;
    const { backspaceCount, panicIndex, codebaseHealth, ceoAnxietyIndex, trapTick } = this.numbers;
    const velocity = this.numbers.keystrokeVelocity;
    const events: SessionEvent[] = [];
    for (let slot = 0; slot < this.count; slot += 1) {
      const totals = slot * KIND_COUNT;
      world.keystrokeVelocity = keystrokeVelocity(windowTotals[totals + KEYSTROKE]!, config);
      world.backspaceCount = windowTotals[totals + BACKSPACE]!;
      world.panicIndex = panicIndex[slot]!;
      const panic = nextPanicIndex(world, config);
      velocity[slot] = world.keystrokeVelocity;
      backspaceCount[slot] = world.backspaceCount;
      panicIndex[slot] = panic;
      const now = tick[slot]!;
      tick[slot] = now + 1;

      // ScenarioDirector.evaluate: once tripped the trap never fires again,
      // and it trips on the first tick panic reaches the threshold.
      if (trapTick[slot]! >= 0 || panic < config.trapPanicThreshold) continue;
      trapTick[slot] = now;
      codebaseHealth[slot] = Math.max(0, codebaseHealth[slot]! - config.codebaseHealthPenalty);
      ceoAnxietyIndex[slot] = config.ceoAnxietyOnTrap;
      events.push({ id: this.ids[slot]!, event: ceoAlert(now) });
    }
    return events;
  }

  /** A session's tick, world and director log: everything but its window. */
  summary(id: string): Omit<EngineSnapshot, 'window'> | null {
    const slot = this.slotOf.get(id);
    return slot === undefined ? null : this.summaryAt(slot);
  }

  /**
   * A session as SimulationEngine.snapshot() would show it, had its engine
   * been stepped with the same counts, window and all.
   */
  snapshot(id: string): EngineSnapshot | null {
    const slot = this.slotOf.get(id);
    if (slot === undefined) return null;
    const { tick, state, trapTripped, directorLog } = this.summaryAt(slot);
    return { tick, state, window: this.windowEvents(slot, tick), trapTripped, directorLog };
  }

  /** Every session's director log, in slot order. */
  trapEvents(): SessionEvent[] {
    const events: SessionEvent[] = [];
    const { trapTick } = this.numbers;
    for (let slot = 0; slot < this.count; slot += 1) {
      const at = trapTick[slot]!;
      if (at >= 0) events.push({ id: this.ids[slot]!, event: ceoAlert(at) });
    }
    return events;
  }

  /**
   * The live columns, trimmed to the sessions. They are views, not copies:
   * encoding them into a checkpoint before the next tick is the copy, and
   * that is the whole of a checkpoint's freeze.
   */
  columns(): SessionColumns {
    return {
      ids: this.ids.slice(),
      windowTicks: this.windowTicks,
      ...numberColumns((name) => this.numbers[name].subarray(0, this.count)),
      window: this.window.subarray(0, this.count * this.ringLength),
    };
  }

  /** Fill an empty table from a checkpoint's columns. */
  load(columns: SessionColumns): void {
    if (this.count !== 0) throw new Error('only an empty table can be loaded');
    if (columns.windowTicks !== this.windowTicks) {
      throw new CorruptionError(
        `checkpointed windows span ${columns.windowTicks} ticks, but this engine config spans ${this.windowTicks}`,
      );
    }
    const n = columns.ids.length;
    if (NUMBER_COLUMNS.some((name) => columns[name].length !== n) || columns.window.length !== n * this.ringLength) {
      throw new CorruptionError(`checkpointed columns do not all hold the same ${n} sessions`);
    }
    let capacity = this.capacity;
    while (capacity < n) capacity *= 2;
    if (capacity > this.capacity) this.grow(capacity);

    for (const [slot, id] of columns.ids.entries()) {
      if (this.slotOf.has(id)) throw new CorruptionError(`session ${id} is checkpointed twice`);
      this.slotOf.set(id, slot);
      this.ids.push(id);
    }
    for (const name of NUMBER_COLUMNS) this.numbers[name].set(columns[name]);
    this.window.set(columns.window);
    // Slots a removed session left behind can still hold its totals and inbox.
    this.windowTotals.fill(0);
    this.inbox.fill(0);
    this.inboxUsed.fill(0);
    for (let slot = 0; slot < n; slot += 1) {
      for (let at = slot * this.ringLength; at < (slot + 1) * this.ringLength; at += 1) {
        const total = slot * KIND_COUNT + (at % KIND_COUNT);
        this.windowTotals[total] = this.windowTotals[total]! + this.window[at]!;
      }
    }
    this.count = n;
  }

  private summaryAt(slot: number): Omit<EngineSnapshot, 'window'> {
    const { numbers } = this;
    const state = {} as WorldState;
    for (const field of WORLD_FIELDS) state[field] = numbers[field][slot]!;
    const trapTick = numbers.trapTick[slot]!;
    return {
      tick: numbers.tick[slot]!,
      state,
      trapTripped: trapTick >= 0,
      directorLog: trapTick >= 0 ? [ceoAlert(trapTick)] : [],
    };
  }

  /** The window as the engine's event list: oldest tick first, each in canonical kind order. */
  private windowEvents(slot: number, tick: number): TelemetryEvent[] {
    const events: TelemetryEvent[] = [];
    for (let past = Math.max(0, tick - this.windowTicks); past < tick; past += 1) {
      const at = slot * this.ringLength + (past % this.windowTicks) * KIND_COUNT;
      for (const event of expandBatch(this.window.subarray(at, at + KIND_COUNT), past * this.config.tickMs)) {
        events.push(event);
      }
    }
    return events;
  }

  private move(from: number, to: number): void {
    for (const name of NUMBER_COLUMNS) this.numbers[name][to] = this.numbers[name][from]!;
    this.window.copyWithin(to * this.ringLength, from * this.ringLength, (from + 1) * this.ringLength);
    this.windowTotals.copyWithin(to * KIND_COUNT, from * KIND_COUNT, (from + 1) * KIND_COUNT);
    this.inbox.copyWithin(to * KIND_COUNT, from * KIND_COUNT, (from + 1) * KIND_COUNT);
    this.inboxUsed[to] = this.inboxUsed[from]!;
    const id = this.ids[from]!;
    this.ids[to] = id;
    this.slotOf.set(id, to);
  }

  private grow(capacity: number): void {
    this.numbers = numberColumns((name) => resized(this.numbers[name], capacity));
    this.window = resized(this.window, capacity * this.ringLength);
    this.windowTotals = resized(this.windowTotals, capacity * KIND_COUNT);
    this.inbox = resized(this.inbox, capacity * KIND_COUNT);
    this.inboxUsed = resized(this.inboxUsed, capacity);
    this.landed = resized(this.landed, capacity);
    this.capacity = capacity;
  }
}

function numberColumns<T>(make: (name: NumberColumn) => T): Record<NumberColumn, T> {
  return Object.fromEntries(NUMBER_COLUMNS.map((name) => [name, make(name)])) as Record<NumberColumn, T>;
}

function resized<T extends Float64Array | Uint32Array | Uint16Array | Uint8Array>(column: T, length: number): T {
  const next = new (column.constructor as new (length: number) => T)(length);
  next.set(column);
  return next;
}

/** One count per kind, each a whole number a window count can hold. */
function fitsColumns(counts: readonly number[]): boolean {
  if (!Array.isArray(counts) || counts.length !== KIND_COUNT) return false;
  for (const count of counts) {
    if (!Number.isInteger(count) || count < 0 || count > MAX_TICK_COUNT) return false;
  }
  return true;
}
