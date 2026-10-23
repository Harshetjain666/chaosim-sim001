# SIM-001: ChaoSim Core Simulation Engine

A lightweight prototype of ChaoSim's core runtime engine and Adaptive Scenario Director.

It ingests behavioral telemetry, maintains an authoritative in-memory world, computes operator stress on a fixed tick, mutates the environment when conditions deteriorate, and seals the final state with an HMAC-SHA256 fingerprint.

The written architecture answers for Challenge 1 and Challenge 2, plus the design rationale and the path to production, live in [docs/ARCHITECTURE_BRIEF.md](docs/ARCHITECTURE_BRIEF.md).

## Requirements

Node 23.6 or newer. That is the only dependency. The engine runs straight from TypeScript source (Node strips the types) and the cryptography uses the built in `node:crypto` module, so there is nothing to install and nothing to compile.

On Node 22.6 through 23.5, add the `--experimental-strip-types` flag, or run any command through `npx tsx` instead of `node`.

## Quick start

```bash
node src/index.ts        # run the deterministic chaos load test and print the report
node src/index.ts --trace  # same, with a per second metrics table
node src/index.ts --live   # drive the engine in real time for ~30 seconds
node src/index.ts --cluster      # thousands of sessions on worker shards in real time, one shard killed mid run
node src/index.ts --crash-drill  # SIGKILL a sharded cluster twice and prove recovery is exact
node --test                # run the unit suite
```

The default command runs the three phase Chaos Load Test, prints a phase by phase summary, emits the CEO alert when the trap trips, outputs the verified fingerprint, and finishes with a block of acceptance checks. It exits non zero if any check fails, so it doubles as a regression gate.

## What the run shows

* **Phase 1 (0s to 10s).** Stable typing, no backspacing. The stress curve stays dormant, panic sits at zero, the director never acts.
* **Phase 2 (10s to 20s).** Friction ramps in. Velocity climbs first, then backspacing spikes. Panic begins rising but stays under the trap threshold of 75.
* **Phase 3 (20s to 30s).** The failure cascade sustains the pressure, panic crosses 75, the Scenario Director fires exactly once: codebase health drops by 30, CEO anxiety jumps to 95, and the alert is emitted. On completion the final state is frozen, timestamped, and signed.

## Scale out

The engine also runs sharded. `src/cluster/` puts sessions on worker thread shards, each with its own fixed timestep loop, a write ahead log, and checkpoints every five seconds, so a shard that crashes comes back with exactly the state it lost. [docs/SCALE_OUT.md](docs/SCALE_OUT.md) is the design, the guarantees, and the measured numbers.

* `--cluster` runs it in real time. By default that is 4,000 sessions on 4 shards for 30 seconds, every session fed the chaos scenario, with shard 0 killed at 21 seconds, right as the traps start to trip. It prints shard health every second, then checks that every attestation verifies, that the trap fired at most once per session, and that every alert reached the consumer once after dedup. `--shards=N`, `--sessions=N`, `--seconds=N` and `--kill-at=S` change the run, and `--kill-at=0` leaves every shard alive.
* `--crash-drill` kills a cluster process with SIGKILL twice, once while it is writing and once at a known point, then recovers it in a fresh process. It checks that no acknowledged tick was lost, that every recovered session is bit identical to the engine that never crashed, and that every session still finishes on its reference fingerprint.

## Project layout

```
src/
  types.ts             WorldState and telemetry types
  config.ts            engine and shard tunables in one place
  worldState.ts        authoritative state factory (spec initial values)
  telemetryWindow.ts   per session sliding window over raw events
  stressModel.ts       metric summarization and the panic stress curve
  scenarioDirector.ts  the trap: fires once, mutates the world, emits the alert
  engine.ts            the authoritative fixed tick engine (pure step function, snapshot and restore)
  attestation.ts       canonical serialize, HMAC-SHA256 sign, constant time verify
  simulation.ts        lifecycle façade: step then complete()
  liveLoop.ts          fixed timestep clock (no clock drift), shared by the live run and every shard
  scenario.ts          synthetic telemetry generator for the load test
  chaosLoadTest.ts     the three phase scenario plus acceptance checks
  index.ts             CLI entry and report formatting
  cluster/
    cluster.ts         the router: placement, batching, requests, supervision and restart
    lock.ts            one process per data directory
    placement.ts       rendezvous hashing: which shard owns a session
    protocol.ts        messages between the router and a shard worker
    shardWorker.ts     worker thread entry: one shard, its clock, its health metrics
    shard.ts           sessions, ticks, output commit, checkpoints, recovery
    sessionTable.ts    every session as typed array columns: window ring, inbox, the tick in one pass
    inbox.ts           telemetry kind codes and the batch a tick's counts stand for
    wal.ts             write ahead log: segments, group commit, torn tail scan
    checkpoint.ts      atomic checkpoints: a JSON header and a copy of the columns
    frame.ts           length and crc32 framing, fsync helpers
    errors.ts          corruption and cluster errors
    workload.ts        scenario workloads, the reference engine, the lockstep driver
    crashDrill.ts      the SIGKILL drill
    crashDrillChild.ts the process the drill kills
    clusterDemo.ts     the real time demo
test/
  sim.test.ts          determinism, trap once, stress curve, attestation
  snapshot.test.ts     freezing and resuming the engine
  wal.test.ts          log format, group commit, torn and corrupt logs
  sessionTable.test.ts the session columns against the plain engine, tick by tick
  checkpoint.test.ts   checkpoint layout, byte order, format versions
  shard.test.ts        recovery, output commit, checkpoints, idempotent completion
  cluster.test.ts      placement, sharded runs, shard kill, restart, manifest, crash loops
  crashDrill.test.ts   the SIGKILL drill
  clusterDemo.test.ts  real time smoke test
docs/
  ARCHITECTURE_BRIEF.md  Challenge 1 and 2 answers, design notes, production path
  SCALE_OUT.md           sharding, the write ahead log, recovery, and what each failure costs
```

## Design in one paragraph

The whole engine reduces to one idea: the world only ever changes inside `SimulationEngine.step()`, and that method is a pure function of the previous state and the telemetry batch for the tick. No wall clock leaks into the math, no client is trusted to compute anything, and the only side channel out is the signed attestation. Because the tick is pure and deterministic, the live loop, the chaos test, and the unit tests all drive the identical logic, and a given seed always reproduces the same trajectory. The shards run that same step over their sessions held as typed array columns, and are tested against it tick for tick. Everything else (the sliding window, the stress curve, the director, the signer) hangs off that spine.

## Configuration

`CHAOSIM_SECRET` sets the HMAC key (defaults to a development key). Pass `--seed=N` to vary the synthetic telemetry while keeping the run reproducible. Pass `--fixed-time` to stamp a constant completion time so the fingerprint itself is reproducible across runs.

The crash drill takes `--shards=N`, `--sessions=N`, and `--kill-after=T` to pin the first kill to tick T instead of a random one. A cluster's data directory remembers its shard count, engine config and signing secret (as a keyed fingerprint) and refuses to start with different ones, because placement, replay and already issued attestations depend on them. Only one process can use a data directory at a time. Shard tunables (checkpoint interval, inbox cap, retained completions) live next to the engine's in `src/config.ts` and are free to change between runs. The inbox cap goes up to 65,535, the widest count a session column holds, and the engine's window must be whole milliseconds and at least one tick long; the cluster refuses anything else at start.
