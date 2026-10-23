# ChaoSim Architecture Brief

This document answers the three challenges in one place. Challenge 3 is the running prototype in this repository; Challenges 1 and 2 are answered here in prose and code, and I point back to the prototype wherever a concept already has a concrete implementation.

The thread running through all three is a single conviction: the server owns one authoritative, deterministic loop, the client is never trusted to compute anything, and every expensive thing (database writes, scoring, compression, signing) happens off the hot path on a controlled schedule rather than per event. Everything below is a consequence of that.

A note on scope. The brief deliberately avoids managed cloud primitives. There are no hosted queues, no managed databases, no autoscaling groups in the answers. The mechanisms are all software you can read: ring buffers, a fixed timestep loop, worker threads, an append only log, streaming compression, and an HMAC. That is the spirit of the challenge.

---

## Challenge 1: Architecture Questions

The system is 10,000 IDEs streaming continuous telemetry over persistent WebSockets into an authoritative loop that updates per candidate state every 100ms, injects environment mutations when state deteriorates, computes offline progress across a 1000 hour lifecycle, and signs the whole decision footprint for an immutable training lake. Four sub questions.

### 1.1 Authoritative game loop and backpressure

The single most important structural decision is to physically separate two things that beginners tend to fuse: socket I/O and simulation. The socket layer is event driven and runs per connection. The simulation is one fixed timestep loop that owns all state. They communicate only through a bounded per session inbox. A socket callback never runs game logic, and the game loop never awaits a socket.

A WebSocket handler does the cheapest possible work: validate, stamp a server side timestamp, and push the raw event into that session's ring buffer. If the buffer is full it applies a drop or coalesce policy and increments a counter. It returns immediately.

```ts
wss.on('connection', (socket, sessionId) => {
  const inbox = sessions.get(sessionId)!.inbox; // preallocated ring buffer

  socket.on('message', (raw) => {
    const event = decodeAndValidate(raw);          // O(1), no scoring here
    if (!event) return;                            // reject malformed input
    event.t = serverNowMs();                       // server is the clock authority
    if (!inbox.push(event)) {
      inbox.droppedCount++;                        // bounded: shed under burst
    }
  });
});
```

The loop drains those inboxes on a fixed cadence. Two properties matter: the work per tick is bounded (it is O(active sessions), each doing a small summarization over a bounded window, never O(events)), and the clock is driven by elapsed real time rather than by trusting the timer to fire at exactly 100ms. The prototype's [src/liveLoop.ts](../src/liveLoop.ts) is exactly this pattern:

```ts
let last = process.hrtime.bigint();
let accumulatorMs = 0;

setInterval(() => {
  const now = process.hrtime.bigint();
  accumulatorMs += Number(now - last) / 1_000_000;
  last = now;

  // Release as many fixed ticks as real time has earned. A late timer
  // catches up by stepping several ticks; it never drifts forward.
  while (accumulatorMs >= TICK_MS) {
    for (const session of shard.activeSessions) {
      session.step(session.inbox.drainWindow());  // pure, bounded work
    }
    accumulatorMs -= TICK_MS;
  }
}, TICK_MS / 2);
```

Why this prevents the two failure modes in the question:

* **High velocity bursts cannot block the main loop.** A panicking candidate spamming saves just produces more cheap `inbox.push()` calls on the I/O side. The simulation still does one bounded summarization per tick regardless of whether 5 or 5,000 events landed, because it reads a windowed aggregate, not each event. The burst is absorbed by the ring buffer and, if it exceeds capacity, coalesced. We only need the rate of backspacing and saving, not every byte, so lossy shedding under extreme load is the correct behavior, not a compromise.
* **Clock drift across parallel sessions cannot happen,** because no session has its own timer. Every session advances by the same integer tick count released by the one accumulator. If a tick runs long, the accumulator releases the next ones to catch up, and every session stays on the same logical clock. State is a function of tick index, never of `Date.now()`.

At 10,000 sessions you shard. One worker thread cannot and should not own all of them. Partition by `sessionId` (a stable hash) across `worker_threads`, each worker running its own accumulator loop over its slice, with sticky routing so a connection always lands on the worker that owns its state. Sessions are independent, so the shards never coordinate on the hot path. You scale by adding workers, then processes, then nodes. Event loop lag (measured with `monitorEventLoopDelay`) is the health signal: if a shard's lag climbs, it is over its budget and sessions migrate.

This part is built. [src/cluster/](../src/cluster/) places sessions on `worker_threads` shards by rendezvous hashing, runs each shard on the same accumulator clock as the live loop, and reports per shard event loop lag and tick time. Migration is the one piece not built yet. [docs/SCALE_OUT.md](SCALE_OUT.md) has the design and the measured numbers.

### 1.2 ECS state architecture vs database bottlenecks

The volatile state lives entirely in memory in a structure of arrays layout, which is the practical core of an ECS approach. Instead of 10,000 heap objects (which fragment memory and punish the garbage collector), each metric is one contiguous typed array indexed by a dense session slot, with a map from `sessionId` to slot:

```ts
const slotOf = new Map<string, number>();        // sessionId -> dense index
const panicIndex      = new Float64Array(CAPACITY);
const codebaseHealth  = new Float64Array(CAPACITY);
const ceoAnxietyIndex = new Float64Array(CAPACITY);
const backspaceCount  = new Float64Array(CAPACITY);
const keystrokeVel    = new Float64Array(CAPACITY);
```

The tick then walks these arrays in order, which is cache friendly, allocation free, and trivially vectorizable, and produces zero per entity garbage. The single session prototype keeps a readable object per session (see [src/types.ts](../src/types.ts)) because at one session clarity wins. The shards have made the flip: [src/cluster/sessionTable.ts](../src/cluster/sessionTable.ts) holds each shard's sessions in exactly these columns, plus the tick the trap tripped on, and the logic in [src/stressModel.ts](../src/stressModel.ts) runs on them unchanged. The tests hold the columns to the readable engine after every tick.

Nothing on this path touches a database. Writing per tick to a persistent store is what chokes connections, so we never do it. Persistence happens through a deterministic, time windowed batch flush plus a write ahead log, which together give crash safety:

* **Checkpoint flush.** Every N ticks (for example every 5 seconds) a flush job snapshots the dirty session slots and writes one compact batch to the persistent layer. To avoid blocking the loop, it snapshots under a copy on write or double buffer scheme: the loop keeps mutating the live arrays while the flusher serializes the frozen copy.
* **Write ahead log.** Between checkpoints, each tick appends its raw telemetry frame to an append only segment that is `fsync`ed in small batches. It has to be the input frame and not the resulting state deltas: the sliding window and the director's latch are state too, and only replaying inputs through the deterministic step rebuilds them exactly. This is ordinary file I/O on a local volume, replicated, not a managed service.

Now the crash question. If a container self destructs mid run, on restart the worker replays from the last checkpoint and then replays the WAL segment forward to reconstruct the exact in memory state. Each record carries a monotonic tick sequence number, so replay is idempotent: anything already folded into the checkpoint is skipped. The data loss window (the RPO) is bounded by the WAL `fsync` interval, which is sub second, not by the much larger checkpoint interval. You lose at most the last unsynced batch, and because replay is deterministic (Challenge 1.1), the reconstructed state is bit for bit identical to what was lost up to that boundary.

```ts
function flushCheckpoint(shard: Shard, tick: number) {
  const frozen = shard.snapshotDirtyColumns();   // copy-on-write: the only synchronous part
  void persist.writeBatch(tick, frozen);         // async fs, runs on the libuv thread pool
}

// On boot:
function recover(shard: Shard) {
  const cp = persist.loadLatestCheckpoint();
  shard.restore(cp.columns);
  for (const rec of wal.readFrom(cp.tick + 1)) {
    if (rec.tick > shard.tick) shard.applyDelta(rec); // idempotent replay
  }
}
```

The prototype's analog of this is small but real: [src/telemetryWindow.ts](../src/telemetryWindow.ts) is the in memory buffer that is summarized rather than persisted per event, and [src/attestation.ts](../src/attestation.ts) is the final durable checkpoint. Production adds the WAL and the periodic checkpoints between, and that is now built in [src/cluster/](../src/cluster/): a per shard log with group commit, atomic checkpoints every 50 ticks, and recovery that replays through the same `step()`.

Building it surfaced two things this section originally missed. A write handed to `queueMicrotask` does not leave the loop at all, since a microtask runs on the same thread before the next timer; the write has to go through the async file API. And an outbound effect such as the CEO alert must wait until the tick that produced it is durable. Otherwise a crash rolls the world back to before the trap, the trap is armed again, and the candidate can see the alert twice. [docs/SCALE_OUT.md](SCALE_OUT.md) covers both, and a crash drill that kills a cluster process with SIGKILL checks that recovery is bit identical.

### 1.3 Offline state simulation

A player gone 12 hours must not trigger a replay of 432,000 ticks at re-authentication. The trick is recognizing that offline time has no high frequency human telemetry. There are no keystrokes while you are logged out. The only thing evolving is the ambient environment, the moving market, which follows known rules. That makes the catch up cheap in two complementary ways.

First, any metric whose update rule is closed form is integrated in O(1). The prototype's panic decay is multiplicative, `panic *= f` each tick (see `nextPanicIndex` in [src/stressModel.ts](../src/stressModel.ts)), so after k offline ticks the value is simply `panic0 * f^k`, one `Math.pow` call rather than a loop. Linear drifts and bounded growth curves have the same treatment. You jump straight to the answer at `t = now`.

```ts
function catchUp(state: WorldState, elapsedMs: number, env: EnvSchedule): WorldState {
  const ticks = Math.floor(elapsedMs / TICK_MS);

  // Closed form for the metrics that have one. O(1).
  state.panicIndex = floorZero(state.panicIndex * Math.pow(DECAY, ticks));

  // Coarse replay for the ambient market: minute steps, not 100ms steps.
  const steps = Math.floor(elapsedMs / COARSE_STEP_MS); // 12h -> 720 steps
  for (let i = 0; i < steps; i++) {
    applyEnvironmentDrift(state, env, COARSE_STEP_MS);
  }
  return state;
}
```

Second, whatever is not closed form is replayed coarsely. Because there is no per keystroke detail to honor, we step the environment at a much larger dt, say one minute. Twelve hours becomes 720 steps, which runs in well under a millisecond. The whole catch up routine is a pure function of the last checkpoint, the elapsed wall time, and the deterministic environment schedule, so it is reproducible and itself signable.

This sits comfortably inside the 500ms re-auth budget: the closed form parts are O(1) and the coarse replay is a few hundred iterations of arithmetic. We run it during the handshake, reconcile the session to the present, then hand it to the live loop to resume normal 100ms ticks. The budget is spent on the network handshake, not the math.

### 1.4 High density telemetry serialization

The goal is to turn the raw behavioral firehose into something compact and un-gameable before it reaches the signing layer, without ever slowing the loop. The pipeline has four stages, all running off the hot path on a worker fed by a bounded queue.

1. **Frame.** Each tick produces a small fixed shape frame per session (the counts and deltas for that tick). The loop's only job is to append this frame to a ring; it does no encoding itself.
2. **Columnar encode.** A background flusher groups frames into a segment and encodes them column by column rather than as rows of JSON. Homogeneous numeric columns (timestamps, keystroke counts, backspace counts) compress far better when grouped, especially with delta encoding on the monotonic timestamp and counter columns.
3. **Compress.** The columnar buffer is run through a streaming compressor (zlib or zstd bindings, streamed so memory stays flat). This is a library call on a worker thread, not a managed service.
4. **Chain and sign.** Here is the un-gameable part. Each frame carries a monotonic sequence number and a running hash that folds in the previous frame's hash, a hash chain. You cannot reorder, drop, or splice a frame without breaking the chain. The final HMAC signs the terminal digest of that chain together with the final state, so the fingerprint commits to the entire ordered footprint, not just the ending numbers.

```ts
// Running digest chained across the whole session, folded into the final HMAC.
let chain = INITIAL_DIGEST;
for (const frame of segment) {
  chain = sha256(concat(chain, encodeFrame(frame)));   // tamper-evident order
}
const fingerprint = hmacSha256(secret, concat(chain, canonical(finalState)));
```

What makes it un-gameable, concretely:

* The server is the only authority. Client telemetry is raw input; scores are never accepted from the client. A candidate cannot submit a flattering Panic Index, only keystrokes that the server scores.
* The hash chain makes the footprint tamper evident. Editing any historical frame changes the terminal digest and invalidates the signature.
* Server side realism checks flag physically impossible streams (keystroke rates a human cannot produce, perfectly periodic timing), so injected synthetic telemetry is detectable before it is ever signed.

And it never slows the loop because the loop only appends a frame to a ring. Encoding, compression, and hashing happen on a separate worker draining a bounded queue. If that worker falls behind, the queue applies backpressure and, at the hard limit, sheds with a counter, exactly as on the ingestion side. The prototype shows the endpoint of this pipeline: canonical serialization and HMAC in [src/attestation.ts](../src/attestation.ts). The hash chain is the production extension that turns a single end of run signature into a signature over the full ordered history.

### 1.5 The Autonomous Event Engine

The System Requirements describe an "Invisible Brain" that watches per candidate state every tick and, when PanicIndex stays high while CodebaseHealth falls, injects an environment mutation such as a simulated production database crash or an automated Slack message from an anxious investor. That is the same Scenario Director the prototype implements in [src/scenarioDirector.ts](../src/scenarioDirector.ts), and it is worth two specific design notes, because "sustained" and "dynamically inject" are each doing real work in that sentence.

**Sustained, not instantaneous.** A single spiky tick must not trip the brain, or it would flap on and off. So the detector is a compound, hysteretic condition rather than a bare threshold: PanicIndex has to stay above a high mark for a dwell of D consecutive ticks (tracked with a small counter, or an exponential moving average compared against the mark) and CodebaseHealth has to be trending down over a short window (a sign check on its slope). Both are O(1) per tick and run inside the same tick right after the stress curve, so the brain adds nothing measurable to the hot path. The prototype uses the minimal instance of this, a single `panicIndex >= 75` latch that fires once, which is the right scope for Challenge 3. The production version keeps the same shape and swaps the bare compare for the dwell plus slope test.

**Dynamically inject means a registry, not a hardcoded branch.** Mutations live in a table, each entry pairing a guard, a synchronous effect on world state, and an outbound side effect:

```ts
interface Mutation {
  id: 'db_crash' | 'investor_slack' | 'ceo_alert';
  guard: (s: WorldState) => boolean;          // when this one is eligible
  apply: (s: WorldState) => void;             // authoritative state change
  emit: (s: WorldState) => OutboundEffect;    // the simulated crash / Slack message
  cooldownTicks: number;                       // per mutation latch
}
```

When the detector trips, the director selects an eligible mutation deterministically (a seeded function of the current state, so the choice is reproducible and therefore signable), applies its state effect synchronously inside the tick, and enqueues the outbound side effect to a worker rather than performing it inline. The simulated database crash flips a world flag and degrades CodebaseHealth; the investor Slack message is pushed to an outbound queue and delivered off the loop. Each mutation carries its own cooldown latch so the world degrades in deliberate steps instead of a single tick storm. The prototype's once only CEO alert is exactly one row of this table with an effectively infinite cooldown.

Two smaller points the System Requirements imply. The state is multi dimensional and open ended: the structure of arrays layout in 1.2 holds one column per index, so adding TechnicalDebtIndex, CofounderTension, or BurnRate is adding columns, not reworking the loop. And because the detector, the selection, and the effects are all pure functions of authoritative state, the entire decision footprint the brain produces stays deterministic and replayable, which is exactly what lets 1.4 sign it.

---

## Challenge 2: Company Brain Architecture Scenario

500 candidates, 45 minute sessions, every keystroke and terminal command and focus shift streamed to compute Panic Index and Velocity Alpha. When the AI director injects a Failure Cascade, backspace frequency spikes and the candidate spams file saves. Those raw deltas cannot hit a database on every tick. Three specifics.

### 2.1 In-memory state management

Each session owns three tiers of state, ordered from rawest to most refined:

1. **A bounded ring buffer of raw sub second events.** Preallocated, fixed capacity, O(1) push, eviction by advancing a head index. This is the prototype's [src/telemetryWindow.ts](../src/telemetryWindow.ts), backed in production by typed arrays for the timestamp and a small integer kind code rather than objects, so it never allocates during a burst. The shards go one step further. The window evicts a whole tick at a time, so a ring of per tick counts per kind holds exactly what a ring of events would, in a fixed 200 bytes per session for the 2 second window however hard a client bursts.
2. **A rolling aggregate updated incrementally.** Rather than rescanning the window every tick, the buffer maintains running counts per event kind and an exponential moving average for velocity, updated on push and on eviction. Summarizing the window then costs O(1), not O(window). The prototype recomputes from the window for clarity; the incremental aggregate is the throughput version of the same numbers, and the shards keep it as running totals of the per tick counts.
3. **The authoritative metrics.** `panicIndex`, `codebaseHealth`, `ceoAnxietyIndex`, and so on, held in the structure of arrays layout from Challenge 1.2.

A global `Map<sessionId, SessionState>` (or a dense slot index) holds them all. The point of the three tiers is that raw events are cheap to absorb, summarization is cheap to compute, and only the tiny refined tier is ever flushed onward.

### 2.2 Concurrency and backpressure

A burst above 150 events per minute during a panic spike must not let WebSocket worker threads block the main event loop. The structure that guarantees it is the same separation as Challenge 1.1, applied with worker threads.

A pool of socket workers owns the connections. Each one only parses and validates, then pushes a compact event into that session's bounded inbox (a `SharedArrayBuffer` backed ring shared with the compute thread, or batched message passing). The single compute thread runs the tick loop and is the only place summarization and scoring happen.

```ts
// On a socket worker. Hot, but trivial: never scores, never awaits.
function onMessage(raw: Buffer, session: SessionRef) {
  const ev = decode(raw);
  if (!ev) return;
  if (!session.inbox.push(ev)) session.inbox.dropped++;  // bounded
}
```

The backpressure policy is per session and bounded. When the inbox fills because the drain rate is temporarily below the burst rate, we coalesce rather than grow: a run of file saves collapses to a count, repeated backspaces increment a counter. This is lossless for the signals we actually compute, because Panic Index and Velocity Alpha care about frequency, not about preserving every individual keystroke object. Past a hard ceiling we drop oldest with a counter and, if a single client floods absurdly, we exercise WebSocket read backpressure on that connection (`pause`/`resume`). The main event loop never does heavy work per message, so a 150+/min spike is just more cheap enqueues and the loop stays responsive. Event loop lag is the SLO we watch.

The reason this holds under the Failure Cascade specifically: the cascade is the worst case by design, and the architecture treats the worst case as ordinary. More events simply means more O(1) pushes and the same one bounded summarization per tick.

### 2.3 The flush trigger

The buffered log transitions into `ai-scorer.ts` on the first of several triggers, and the transition is made crash safe by writing durably before handing off.

The triggers, whichever fires first:

* **Time window.** A per session interval, for example every one to two seconds.
* **Volume.** The buffer reaches K events or B bytes.
* **State transition.** A director event, the Failure Cascade or the trap, forces an immediate flush so the scorer sees a clean boundary at the moment conditions change. In the prototype this is the `panicIndex >= 75` trap in [src/scenarioDirector.ts](../src/scenarioDirector.ts).
* **Lifecycle.** Disconnect, timeout, or completion. In the prototype, completion is `Simulation.complete()` in [src/simulation.ts](../src/simulation.ts), which freezes, serializes, and signs.

Crash safety comes from ordering, not luck. The flush appends the summarized batch to a durable WAL segment and `fsync`s it before the scorer is invoked, and the batch is marked consumed only after the scorer acknowledges. If the container dies mid flush, on restart we replay the WAL and re-feed any batch that is present but not marked consumed. Each batch carries a monotonic sequence number, so the scorer deduplicates by sequence and re-feeding is idempotent: nothing is lost and nothing is double counted.

```ts
function maybeFlush(session: SessionState, now: number): void {
  const trigger =
    now - session.lastFlush >= FLUSH_INTERVAL_MS ||
    session.buffer.size() >= FLUSH_MAX_EVENTS ||
    session.directorFiredThisTick ||
    session.ended;
  if (!trigger) return;

  const batch = session.summarize();          // bounded payload, not the firehose
  wal.appendAndSync(batch);                    // durable BEFORE handoff
  aiScorer.enqueue(batch).then(() => wal.markConsumed(batch.seq));
  session.buffer.reset();
  session.lastFlush = now;
}
```

One deliberate point about feeding the scorer: `ai-scorer.ts` is the Claude evaluation engine, so it is fed the summarized rolling aggregate plus a compressed sample of raw frames, never the raw firehose. That keeps evaluation cost bounded and keeps the model's context focused on signal. The flush boundary is precisely where that summarization happens.

---

## Challenge 3: Design decisions, tradeoffs, and the path to production

The prototype in this repository implements the full SIM-001 specification: the authoritative `WorldState`, a deterministic 100ms tick engine, the stress curve, the once only Adaptive Scenario Director with the CEO alert, and the HMAC-SHA256 attestation. The Chaos Load Test drives all three phases and asserts the expected outcomes. What follows is why it is built the way it is and how it grows.

### Key design decisions

**A pure, deterministic `step()` is the spine.** Everything authoritative happens inside `SimulationEngine.step()` in [src/engine.ts](../src/engine.ts), and that method is a pure function of the previous state and the tick's telemetry batch. No wall clock and no randomness leak into it. This one choice pays for itself repeatedly: the unit tests, the deterministic chaos run, and the real time live loop all exercise the identical logic; a seed reproduces a trajectory exactly; crash recovery by replay is correct by construction; and the attestation actually means something because the history that produced the final state is reproducible. The cost is discipline. Every source of nondeterminism, the clock and the synthetic jitter, has to be injected rather than read ambiently, which is why time enters through the loop driver and randomness through a seeded generator in [src/rng.ts](../src/rng.ts).

**Metrics are derived every tick from a sliding window, not accumulated forever.** Treating `keystrokeVelocity` and `backspaceCount` as windowed readings (see [src/stressModel.ts](../src/stressModel.ts)) is what makes the stress rule meaningful: they rise and fall with recent behavior. The prototype recomputes from the window for readability; the production version keeps the same numbers as an incremental aggregate so the cost is O(1) per tick. The tradeoff is clarity now versus throughput later, and the interface between them does not change.

**Panic decays multiplicatively.** It eases off quickly then tapers, which reads like a person calming down rather than a flat ramp, and it has a useful side effect: multiplicative decay has a closed form, which is exactly what makes the 12 hour offline catch up in Challenge 1.3 an O(1) computation. The small price is that the value approaches zero without reaching it, so the model snaps a floor at a small epsilon.

**The director owns all environment mutations and the once only flag.** Routing the state changes through one component in [src/scenarioDirector.ts](../src/scenarioDirector.ts) keeps the authority in one obvious place, and the `isTrapTripped` flag is what guarantees the cascade fires exactly once even under sustained pressure (there is a dedicated test for this). The director is stateful, which is the tradeoff, but the state is tiny and isolated.

**Attestation is canonical JSON plus HMAC.** Canonical serialization (key sorted, in [src/attestation.ts](../src/attestation.ts)) means equal payloads always produce equal bytes and therefore equal signatures, so verification never depends on field ordering. Verification is constant time to avoid leaking through timing. The honest tradeoff is that HMAC is symmetric: the verifier needs the same secret, which is fine for an internal training lake but wrong for a public attestation. The production upgrade is an asymmetric signature (Ed25519) so anyone can verify a fingerprint without holding a key that could forge one, combined with the hash chain from Challenge 1.4 so the signature covers the whole ordered footprint rather than only the final state.

**Configuration is data.** Every threshold and rate lives in [src/config.ts](../src/config.ts), so the stress model, the trap point, and the metric window can be tuned or A/B tested without touching logic.

### How this becomes a production system for thousands of concurrent simulations

The prototype is one session in one process. Scaling it is mostly about replicating the spine safely, and the determinism is what makes that safe, because sessions are independent and shards are replayable.

* **Sharding.** Partition sessions by a stable hash of `sessionId` across worker threads, then across processes, then across nodes. Each shard runs its own fixed timestep loop over its slice. Sticky WebSocket routing keeps a connection on the worker that owns its state. No cross shard coordination exists on the hot path. Built for worker threads in [src/cluster/](../src/cluster/).
* **State layout.** The structure of arrays component store from Challenge 1.2: rings of per tick counts for the windows, running totals for the aggregates, contiguous typed arrays for the authoritative columns. Built for the shards in [src/cluster/sessionTable.ts](../src/cluster/sessionTable.ts), which turned the checkpoint freeze into a column copy.
* **Durability.** Per shard write ahead log plus periodic checkpoints, with idempotent replay on restart and a sub second RPO, as in Challenge 1.2. A container that dies comes back bit identical up to the last synced batch. Built, and checked by a SIGKILL crash drill (`node src/index.ts --crash-drill`).
* **Telemetry and attestation pipeline.** Columnar binary frames, delta plus streaming compression, a hash chain for tamper evidence, and asymmetric signing into the immutable lake, all running on worker threads off the loop, as in Challenge 1.4.
* **Offline lifecycle.** The closed form plus coarse replay catch up from Challenge 1.3 runs at re-authentication inside the 500ms budget.
* **Backpressure everywhere.** Bounded queues, coalescing, and load shedding with counters at every boundary, ingestion, scoring handoff, and serialization, with event loop lag as the SLO that triggers session migration.
* **The scorer off the loop.** `ai-scorer.ts`, the Claude evaluation engine, consumes summarized flushes triggered by the boundaries in Challenge 2.3. The Failure Cascade is a natural evaluation unit because the director already marks that boundary.

The throughput argument is straightforward, and it is now measured rather than estimated. On the built cluster a shard carried 10,000 sessions through the full failure cascade, so the estimate this paragraph used to make, a few hundred to a thousand sessions per shard, was conservative by an order of magnitude. The first wall was not CPU but the checkpoint freeze: turning 10,000 readable session objects into a checkpoint stopped that shard's loop for 45 to 74ms every five seconds, and the cost grew with every session. The structure of arrays layout from Challenge 1.2 removed that wall. The freeze is now a copy of 256 bytes of columns per session at the 2 second window, under a millisecond in process; on the running cluster it stayed under 6ms at 10,000 sessions per shard, and the busiest second of the cascade fell from 17 percent of one core to under 5. At 20,000 sessions per shard the largest share of the loop's time goes to taking in telemetry, not to checkpoints. You scale shards linearly. The reason linear scaling is actually achievable here, and not just hoped for, is the same property the prototype is built around: each session is an independent, deterministic, replayable function of its own inputs. That is the whole design, and everything else is an optimization of it.
