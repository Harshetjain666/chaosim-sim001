# Scale Out: Sharded Workers and Crash Recovery

The brief describes two production pieces in Challenge 1.1 and 1.2 and lists them again in Challenge 3: sessions sharded across worker threads, and a per shard write ahead log with checkpoints so a shard that dies comes back with exactly the state it had. The brief explains why. This document pins down how, including three places where building it showed the sketch in the brief was not quite enough.

Everything here is Node built ins: `worker_threads`, `fs`, `zlib.crc32`, `perf_hooks`. No dependencies were added.

## The shape

```
 main thread                               one worker thread per shard
 +---------------------------------+       +------------------------------------------+
 | ShardCluster                    |       | fixed timestep clock (liveLoop.ts)       |
 |   rendezvous placement          | ----> | Shard                                    |
 |   batched ingest per shard      |       |   inbox per session: 5 counters          |
 |   request / reply               | <---- |   sessions as typed array columns        |
 |   supervision and restart       |       |   WAL writer: group commit, async fsync  |
 +---------------------------------+       |   checkpoint every 50 ticks              |
                                           +------------------------------------------+
                                             data/shard-N/wal-<seq>.log
                                             data/shard-N/checkpoint-<seq>.ckpt
```

The engine is untouched in its math. A shard holds its sessions as typed array columns, one slot per session, and steps every one of them once per shard tick in a single pass that runs the same stress model `SimulationEngine.step()` runs (see Session columns below). Sessions never talk to each other, so shards never coordinate on the hot path.

The code lives in `src/cluster/`.

## Placement

A session goes to the shard with the highest `hash(sessionId, shard)`, which is rendezvous hashing. Plain `hash % shards` would also be stable, but growing from 4 to 5 shards would move about 80 percent of sessions, while rendezvous moves about 20 percent, only the ones the new shard now wins. Moving a session means moving its state, so the cheap time to pick the right function is before there is any state to move.

Live migration is not built yet. Until it is, the shard count is fixed for the life of a data directory. `cluster.json` records its own version, the shard count, the engine config, and a keyed fingerprint of the signing secret (never the secret itself), and the cluster refuses to start against a directory written with different values. Starting with a different engine config is refused for a reason that is easy to miss: replay re-runs the engine, so a changed threshold would replay the log into a different state than the one that was lost. A different secret is refused for a similar one: replay re-signs every completion after the last checkpoint, and those attestations would no longer match the ones already handed out.

## Ingestion: the shard is the clock

Telemetry arrives at the router as `(sessionId, kind, count)`. The router batches it per shard and posts one message per shard per event loop turn rather than one per event.

Inside the shard, each session's inbox is five counters, one per telemetry kind. That is the coalescing the brief asks for in 2.2 ("a run of file saves collapses to a count") taken to its conclusion: the inbox is constant size no matter how hard a client bursts. Each counter is capped per tick (`inboxCapPerKind`, 255 by default) and anything above the cap is counted as dropped.

At each tick the counters become the tick's input. They stand for the batch the engine would be handed: every event stamped with the session's simulation time at the start of that tick, in a fixed kind order. Client timestamps are never used. Stamping at the start of the tick is exactly equivalent to the spread timestamps in the single session path, because the sliding window evicts a whole tick's events on the same later tick wherever inside the tick they landed. That equivalence is tested: a session run through a shard produces the same fingerprint as `runChaosLoadTest` with the same seed.

## Session columns

A shard keeps its sessions the way Challenge 1.2 of the brief describes: a structure of arrays, not an object per session. `src/cluster/sessionTable.ts` gives every session a dense slot, with a map from session id to slot, and keeps every number in a typed array indexed by that slot:

* Seven `Float64Array` columns: the session's tick, its five `WorldState` numbers, and the tick its trap tripped on, `-1` while it is armed. A director log holds at most one alert, so that one number is the whole of it.
* The sliding window, as a ring of per tick counts of each kind (`Uint16Array`, 20 ticks of 5 kinds for the 2 second window), with a running total per kind that each tick adds to and takes from.
* The inbox: five counters per slot.

The ring is not an approximation of the engine's event list. A shard stamps every event at the start of its tick, and the window evicts a whole tick at a time, so after tick T the 2 second window holds exactly ticks T minus 19 through T. The engine only ever counts what is in its window, so counts lose nothing. A tick is three passes over the slots: every window slides one tick, the tick's counts land in the positions just freed, and then metrics, stress and the director run for every slot. That last pass calls the same functions in `src/stressModel.ts` that `SimulationEngine.step()` calls, and checks the director's latch the same way. Stepping the columns allocates nothing per session, and costs the same however busy a window is.

Completing a session moves the last slot into its place, so the slots stay dense. Slot order is then a pure function of the opens, ticks and completions applied, and replaying them rebuilds it exactly. A checkpoint keeps the slots in order. Director events from one tick come out in slot order, which is deterministic and the same after recovery, but is not the order the sessions opened in once a completion has moved one; consumers key events by `(sessionId, tick, type)` and should not depend on their order within a tick.

`inspect` still returns an `EngineSnapshot`. The columns rebuild it, and the window comes back as the event list the engine would hold. That is how the columns are tested: for random workloads where sessions open, idle, flood and complete, under two window lengths, every session's snapshot, window included, must equal a plain `SimulationEngine` stepped with the same counts after every tick, and every completion must sign the same attestation. Fourteen deliberately broken versions, among them a trap that trips a tick late at exactly 75, a completion that moves a session without its window or its pending inbox, a reused slot that keeps the last session's counts, and a restore that forgets the running totals, each fail it.

The single session path (`runChaosLoadTest`, the live loop, the snapshot tests) keeps the readable engine. It is the reference the columns answer to.

The layout brings two limits, and the cluster refuses to start outside them: a session can land at most 65,535 events of one kind in one tick, so `inboxCapPerKind` goes up to 65,535, and the tick and the window must be whole milliseconds, with the window at least one tick long. A third is checked when a checkpoint is written. A session costs 56 bytes plus 10 per window tick (256 bytes with the 2 second window), and one checkpoint frame holds at most 256 MiB: about a million sessions per shard with the 2 second window, about 44,000 with a one minute window. A checkpoint that would not fit stops the shard instead of being written, so nothing is pruned and the log still recovers the state.

## The log records inputs, not state

The brief says each tick appends "its state deltas (or the raw telemetry frame)". Only the second one works. The engine's state is more than the five `WorldState` numbers: the sliding window decides the next tick's velocity and backspace count, and the director's latch decides whether the trap can fire again. Logging `WorldState` deltas would restore the numbers and lose the window, and the first tick after recovery would diverge.

So the log is a command log. Each record is one of:

* `open`: a session was created.
* `tick`: the shard ran tick T, with the inbox counters of every session that had input. Sessions not listed stepped with an empty batch.
* `complete`: a session was sealed, with the `completedAt` stamp that went into its attestation.

Recovery replays these through the same code paths the live shard uses, so the shard's own tick does the reconstruction. Because the tick is pure and deterministic, the result is identical to the lost state, not approximately equal to it.

Every record carries a monotonic sequence number. Replay skips anything at or below the checkpoint's sequence number and refuses to run across a gap.

### On disk

A segment file starts with an 8 byte magic that includes the format version. Each record is framed as `[u32 length][u32 crc32][JSON payload]`. JSON keeps the log readable when debugging, and at a thousand sessions a tick record is a few tens of kilobytes, which is cheap. The frame is where a binary codec would slot in if measurement ever says it matters.

### Group commit

Appending a record only touches memory. A flush writes everything buffered since the last flush in one write and one `fdatasync`, through the libuv thread pool, so the shard's loop never waits on the disk. At most one flush is in flight; records appended meanwhile ride the next one. The shard asks for a flush after every tick, so the data loss window is the last tick plus the fsync latency, plus whatever telemetry was still sitting in inboxes. That is the sub second RPO the brief promises, made concrete.

If a write or fsync fails, the shard stops. It does not retry. After a failed fsync the kernel may already have dropped the dirty pages, so a retry can report success for data that is gone. The only safe move is to crash and recover from what the log can prove.

## Outputs wait for durability

This is the part the brief does not cover, and it matters for the one invariant the prototype is built around: the CEO alert fires exactly once.

Suppose the alert went out the moment the trap tripped, and the process died before that tick reached disk. Recovery would rebuild the world from an earlier tick, the trap would be armed again, and the candidate could get a second alert. The fix is the standard output commit rule: a director event is held until the WAL batch containing its tick is durable, then released. Anything released can no longer be undone by a crash.

The reverse failure also needs covering: an event that was released but had not left the process yet when it died. That event can be one the log replay re-derives, or one already folded into a checkpoint, because a checkpoint is written right after the flush that releases its events. So recovery publishes every event the recovered state holds again, marked `replayed`: each live session's director log and the events inside every retained attestation. Delivery is therefore at least once, and each event carries a natural idempotency key, `(sessionId, tick, type)`, so a consumer that remembers keys sees each alert once. The trap fires at most once per session, so the republish is bounded by the number of sessions.

## Checkpoints

Every `checkpointEveryTicks` shard ticks (50, five seconds, as in the brief) the shard freezes all its sessions into one checkpoint. The freeze copies the session columns into the checkpoint's buffer at the end of a tick, behind a small JSON header that carries what is not a number per session: the ids in slot order, the retained attestations, and the forgotten ids. That copy is the copy on write: the live columns keep moving on the next tick while the frozen bytes go to disk. With the 2 second window a session costs 256 bytes of columns however busy that window is. The readable layout this replaced cost about 1.6 KB per session in the scenario, as JSON, and grew with every event in a window.

On disk a checkpoint is the 8 byte magic `CSCKP002`, then one frame like a log record's, whose payload is `[u32 header length][header JSON][number columns][window counts]`. The columns are copied as they sit in memory, so the header names their byte order, along with the order of the kinds and the columns; a file laid out differently from the build reading it, or written on a host of the other byte order, is reported as unsupported rather than misread. A version 1 checkpoint, the JSON layout this replaced, is unsupported too, so a data directory with a checkpoint from before the change does not open; one that holds only log segments replays as before, since the log format did not change.

The brief's sketch hands the write to `queueMicrotask`. That does not take anything off the loop: a microtask runs on the same thread before the next timer or I/O callback. The implementation does the write with the async `fs` API, which runs on the thread pool.

The order of operations is what makes it safe:

1. Freeze the snapshot at sequence S, and mark the WAL to start a new segment at S + 1.
2. Wait until the WAL is durable through S. This also releases any events up to S, so a checkpoint never contains an event that was not released first.
3. Write the checkpoint to a temp file, fsync it, rename it into place, fsync the directory.
4. Only now delete WAL segments that hold nothing newer than S, then older checkpoints.

A crash anywhere in that sequence leaves either the old checkpoint with its full log tail, or the new checkpoint. A checkpoint is framed with a CRC like a log record, and a checkpoint that fails its checksum stops the shard rather than being skipped, because the log it would need has already been pruned.

A checkpoint always freezes on schedule, even if the previous one is still writing; the writes are chained so they become durable in order. Skipping instead would make where checkpoints land depend on disk speed. Only when one is already waiting behind a running one, which means the disk is not keeping up, is the next one skipped and counted, so a slow disk costs replay length rather than unbounded memory.

## Recovery

When a shard starts it:

1. Loads the newest checkpoint, or starts empty if there is none.
2. Scans the WAL segments in order and replays every record above the checkpoint's sequence number.
3. Treats a short or checksum failing record at the very end of the last segment as a torn write from the crash, truncates the file there, and stops. A last segment that holds only part of its header, or zeros where the header should be (some filesystems leave a file that was extended but never written that way after a power loss), is a torn creation and is removed. The same damage anywhere else is corruption, and the shard refuses to start.
4. Opens a fresh segment and starts ticking.

A file that carries the magic of a segment or checkpoint but a format version this build does not know is reported as unsupported, not corrupt, and left untouched. An operator told "corrupt" might delete data a newer build wrote.

Time that passed while the shard was down is not simulated. The shard resumes from the recovered tick. Reconciling a long gap is the job of the offline catch up in Challenge 1.3.

## Supervision

The router watches every worker. If one exits unexpectedly it rejects the requests that were in flight on that shard with a retryable `SHARD_UNAVAILABLE` error and restarts the worker with exponential backoff. Meanwhile it holds new telemetry for that shard, up to `ingestBufferLimit` items, and new requests, up to `requestBufferLimit` (10,000); past either limit it drops telemetry and refuses requests as `SHARD_UNAVAILABLE` rather than queueing without bound. The new worker recovers from the shard's directory and the router hands it everything it held, in the order it arrived. A shard that crashes five times inside a minute is marked failed instead of being restarted forever. While the cluster is starting, a shard that never comes up fails `start()`, but one that comes up and then dies is restarted like any other.

A rejected request may or may not have been applied, and `openSession`, `complete` and `inspect` are all safe to send again. `openSession` and `complete` are idempotent at the shard. Completed attestations are retained per shard (`completedRetention`, the most recent 1000) and survive checkpoints, so a client that retries a completion after a crash gets the same signed attestation back.

Session ids must be unique. When a completed session's attestation ages out, the shard still remembers its id (`completedIdRetention`, the most recent 10,000), so reopening it answers `completed` instead of quietly starting a second session, and completing it again fails with `ATTESTATION_EXPIRED`. Past that the id is forgotten, which is why ids should be unique to begin with.

What `inspect` returns through the cluster is always durable. The worker takes the snapshot, then flushes before it answers, so nothing a caller has read can be rolled back by a crash. `Shard.inspect` itself says whether its snapshot is durable yet.

`advance`, the manual clock, is the exception. Ticks are not idempotent, so an advance cut short by a crash fails with `ADVANCE_INTERRUPTED`, which is not retryable: some of its ticks may already be durable, and sending it again could run them twice. The caller reads where each session stands with `inspect` and drives on from there, which is exactly what the crash drill's driver does.

A cluster takes an exclusive `LOCK` file in its data directory before it touches anything else. Two processes on one directory, say an old deploy still running when the new one starts, would each replace the other's log segments and go on acknowledging writes that are no longer on disk. That is the one way to lose acknowledged data that recovery cannot see, so the second process is refused. A lock left behind by a process that died is taken over.

`close()` stops every shard with a final checkpoint and rejects, naming the cause, if any shard could not. Nothing it acknowledged is lost in that case; its next start replays the log instead of starting from the final checkpoint.

## What survives what

| Failure | Recovered | Lost |
| --- | --- | --- |
| Worker thread crash | every tick whose WAL batch was fsynced, bit identical | the last tick at most, plus telemetry still in inboxes |
| Process killed with SIGKILL | the same, for every shard | the same |
| Write or fsync error | everything already acknowledged | the shard restarts from the log |
| Torn final record | everything before it | the torn record |
| Corrupt sealed segment or checkpoint | nothing automatically | the shard refuses to start |
| A second process started on the same data directory | the running process, untouched | nothing; the second one refuses to start |

## How it is verified

* **Same answer as one session.** Sessions run through shards produce byte identical fingerprints to `runChaosLoadTest` with the same seed.
* **The columns are the engine.** The session columns are checked against a plain `SimulationEngine` for every session after every tick, window included, as described under Session columns.
* **Recovery is exact.** Unit tests abandon a shard at arbitrary points, recover it, and compare every session against a reference engine stepped to the same tick, and against its own snapshot from before the crash after a completion has moved it to another slot.
* **Torn and corrupt logs.** Tests truncate and flip bytes in segments and check that a torn tail is repaired and corruption is refused.
* **A worker dies.** `ShardCluster.killShard()` terminates a worker mid run; the supervisor restarts it, a request made while it was down is answered by the recovered worker, and the run finishes with the reference fingerprints.
* **The contracts.** Tests start a second cluster on a locked directory, cut an advance short with a kill, crash a shard whose alerts are already folded into a checkpoint, and pull a shard's directory out from under `close()`, and check each is reported or recovered as described above.
* **The process dies.** The crash drill (`node src/index.ts --crash-drill`) runs a cluster in a child process and kills it with SIGKILL while it is writing. A second process recovers the directory, carries on, reports the exact tick of every session once they are durable, and is killed the same way. A third recovery, in the drill's own process, must find every session at exactly the reported tick and identical to the engine that never crashed, and must then finish every session on its reference fingerprint. One more clean restart must replay nothing.

The second kill is the one that proves nothing durable was lost. A process killed while it is running can always have finished work it never got to report, so the first kill can only show that recovery copes with a write cut in half. The drill was built that way after an early version, which only killed a running process, let a deliberately broken recovery that dropped its last log record pass every check. The broken version is caught now.

## Measured

Apple M5 Pro with 18 cores, macOS, Node 25.9, the internal SSD, with other work running on the machine (load average around 5). Four shards, 30 seconds in real time through all three phases of the scenario (`--cluster --sessions=N`), reporting the worst shard in any one second window. Each size ran two or three times: up to 10,000 sessions per shard one run killed shard 0 at 21 seconds and the others kept every shard alive (`--kill-at=0`); both 20,000 runs kept every shard alive. A range is the spread across the runs:

| Sessions per shard | Tick work | Loop lag p99 | Checkpoint | Checkpoint freeze | fsync p99 |
| --- | --- | --- | --- | --- | --- |
| 1,000 | 0.7% of one core | 1.8ms to 3.2ms | 268 KB | 0.4ms to 0.5ms | 68ms to 82ms |
| 2,500 | 1.7% to 1.8% of one core | 2.1ms to 2.2ms | 667 KB | 0.6ms to 1.2ms | 68ms to 78ms |
| 10,000 | 3.6% to 4.6% of one core | 4.3ms to 6.4ms | 2.7 MB | 1.4ms to 5.9ms | 63ms to 295ms |
| 20,000 | 6.5% to 8.9% of one core | 14.5ms to 14.7ms | 5.3 MB | 2.1ms to 10.4ms | 26ms to 348ms |

The same runs with one readable object per session, before the session columns:

| Sessions per shard | Tick work | Loop lag p99 | Checkpoint | Checkpoint freeze | fsync p99 |
| --- | --- | --- | --- | --- | --- |
| 1,000 | 3.2% of one core | 4.5ms | 1.6 MB | 7.8ms | 60ms |
| 2,500 | 5.0% of one core | 9.8ms | 4.1 MB | 13.0ms | 126ms |
| 10,000 | 14.6% to 16.7% of one core | 13.5ms to 13.7ms | 16.5 MB | 45.3ms to 73.8ms | 16ms to 75ms |

What the numbers say:

* **The checkpoint freeze is no longer the ceiling.** At 10,000 sessions per shard a checkpoint stopped the loop for at most 5.9ms across three runs, against 45 to 74ms before, and the file is a sixth of the size. Measured in process, the copy itself takes about 0.4ms at 10,000 sessions and 1ms at 20,000. The higher readings are the worker's garbage collector pausing in the middle of a freeze, not the copy: a run with `--trace-gc` at 20,000 sessions per shard showed scavenges of up to 13ms and full collections of up to 17ms in every shard's worker.
* **CPU fell by about two thirds.** At 10,000 sessions per shard the busiest second of the cascade took 3.6 to 4.6 percent of one core, against 15 to 17 before. Measured in process at that size, a tick costs about 1.4ms: 0.3ms emptying the inboxes into the tick's input, 0.4ms encoding the log record as JSON, and 0.7ms stepping the columns.
* **What the loop spends its time on now.** At 20,000 sessions per shard, taking in one 100ms round of batched telemetry (a message of about 130,000 values, then an inbox increment per item) and running the tick costs a worker 5 to 11ms, more than the freeze, and the garbage that the per tick JSON and the per message arrays leave behind is what the collector pauses for. That is where loop lag p99 comes from now. A log record that refers to sessions by slot in binary, and telemetry posted as a transferable buffer rather than an array, are the next steps if a shard needs to hold more.
* **fsync here is the drive flushing its cache,** 60 to 80ms at p99 with four shards writing, and up to 350ms in runs where other work on the machine was writing too. Group commit absorbs it: a flush slower than a tick simply carries more ticks, so the loss window is the slowest flush plus a tick. Raising the libuv thread pool from 4 to 16 threads changed nothing measurable at four shards. On Linux with an NVMe drive, fdatasync usually takes well under a millisecond.
* **Recovery is quick, and mostly the drive.** A shard holding 1,000 sessions, killed mid run, was back in 44ms after replaying 12 ticks; at 2,500 sessions it took 55ms for 13 ticks, and at 10,000 sessions 88ms for 22 ticks. Most of that is the syncs of reopening the log. The CPU side at 10,000 sessions, reading and decoding the checkpoint, loading its columns and replaying 22 ticks, measures about 23ms in process.
* **The log is cheap but not small.** As JSON it costs about 25 bytes per active session per tick, about 2.6 MB/s per shard at 10,000 sessions in the cascade. A binary codec that refers to sessions by slot would cut that roughly fourfold if bandwidth ever matters.

## Not built yet

* **Session migration.** The health signal exists (per shard event loop delay and tick busy time are reported), but nothing moves sessions yet. Moving one needs a handoff: fence the old owner, ship the snapshot, flip the route. The snapshot half exists.
* **Replication.** The log is on a local volume. Shipping it to a second node is the next durability step.
* **Hung worker detection.** A worker that crashes is restarted. A worker stuck in an infinite loop is not detected yet; that needs a heartbeat watchdog.
* **More than one process.** The same router pattern applies one level up, with processes or nodes in place of threads.
* **The socket layer.** `ShardCluster.ingest()` is where a WebSocket server plugs in. Telemetry reaches it as `(sessionId, kind, count)`, already validated and with no client timestamp.
* **Attestations that name their session.** The signed payload (schema v1, unchanged here so the single session fingerprint stays the same) does not include the session id. With many sessions in one system, one session's signature could be presented as another's. A v2 schema should sign the session id too.
