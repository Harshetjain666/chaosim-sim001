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
 |   request / reply               | <---- |   one SimulationEngine per session       |
 |   supervision and restart       |       |   WAL writer: group commit, async fsync  |
 +---------------------------------+       |   checkpoint every 50 ticks              |
                                           +------------------------------------------+
                                             data/shard-N/wal-<seq>.log
                                             data/shard-N/checkpoint-<seq>.ckpt
```

The engine is untouched in its math. A shard owns many `SimulationEngine` instances and steps every active one once per shard tick. Sessions never talk to each other, so shards never coordinate on the hot path.

The code lives in `src/cluster/`.

## Placement

A session goes to the shard with the highest `hash(sessionId, shard)`, which is rendezvous hashing. Plain `hash % shards` would also be stable, but growing from 4 to 5 shards would move about 80 percent of sessions, while rendezvous moves about 20 percent, only the ones the new shard now wins. Moving a session means moving its state, so the cheap time to pick the right function is before there is any state to move.

Live migration is not built yet. Until it is, the shard count is fixed for the life of a data directory. `cluster.json` records its own version, the shard count, the engine config, and a keyed fingerprint of the signing secret (never the secret itself), and the cluster refuses to start against a directory written with different values. Starting with a different engine config is refused for a reason that is easy to miss: replay re-runs the engine, so a changed threshold would replay the log into a different state than the one that was lost. A different secret is refused for a similar one: replay re-signs every completion after the last checkpoint, and those attestations would no longer match the ones already handed out. The version is 2 since [attestations name their session](#attestations-name-their-session), and a version 1 directory is refused for the same reason.

## Ingestion: the shard is the clock

Telemetry arrives at the router as `(sessionId, kind, count)`. The router batches it per shard and posts one message per shard per event loop turn rather than one per event.

Inside the shard, each session's inbox is five counters, one per telemetry kind. That is the coalescing the brief asks for in 2.2 ("a run of file saves collapses to a count") taken to its conclusion: the inbox is constant size no matter how hard a client bursts. Each counter is capped per tick (`inboxCapPerKind`, 255 by default) and anything above the cap is counted as dropped.

At each tick the shard turns the counters into the engine's batch, in a fixed kind order, every event stamped with the session's simulation time for that tick. Client timestamps are never used. Stamping at the start of the tick is exactly equivalent to the spread timestamps in the single session path, because the sliding window evicts a whole tick's events on the same later tick wherever inside the tick they landed. That equivalence is tested: a session run through a shard signs the same payload as `runChaosLoadTest` with the same seed, plus its own session id, and its fingerprint is checked against exactly that.

## The log records inputs, not state

The brief says each tick appends "its state deltas (or the raw telemetry frame)". Only the second one works. The engine's state is more than the five `WorldState` numbers: the sliding window decides the next tick's velocity and backspace count, and the director's latch decides whether the trap can fire again. Logging `WorldState` deltas would restore the numbers and lose the window, and the first tick after recovery would diverge.

So the log is a command log. Each record is one of:

* `open`: a session was created.
* `tick`: the shard ran tick T, with the inbox counters of every session that had input. Sessions not listed stepped with an empty batch.
* `complete`: a session was sealed, with the `completedAt` stamp that went into its attestation.

Recovery replays these through the same code paths the live shard uses, so `SimulationEngine.step()` does the reconstruction. Because `step()` is pure and deterministic, the result is identical to the lost state, not approximately equal to it.

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

Every `checkpointEveryTicks` shard ticks (50, five seconds, as in the brief) the shard freezes all its sessions into one snapshot. The freeze is a synchronous `JSON.stringify` at the end of a tick. With a readable object per session that is the copy on write: the live objects keep mutating on the next tick while the frozen string goes to disk.

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

## Attestations name their session

A standalone run signs schema v1: the final state, the tick count, the trap flag, the director events and `completedAt`. Nothing in it says which session it came from, which is fine while a process runs one session. In a cluster it would let one session's attestation be presented as another's.

So every completion that goes through a shard signs schema v2, the same fields plus `sessionId`. The shard gives each session's `Simulation` its id when the session opens and again when recovery restores it from a checkpoint, so a completion replayed from the log signs exactly what the live one signed. The single session path (`runChaosLoadTest`, `--fixed-time`, `--live`) still signs v1, and its regression fingerprint for seed 1 did not move.

`verify()` accepts v1 and v2 and nothing else, and it rejects a v2 payload that has no session id. A caller that knows which session an attestation should belong to passes `{ sessionId }`. Then only a v2 attestation naming that session verifies, since a v1 one names none and could be any session's. The cluster demo checks every attestation this way.

This is why `cluster.json` is at version 2. A version 1 directory was written by a build that signed v1: its checkpoints retain attestations that name no session, and replaying its logs here would sign their completions as v2, unlike the attestations it already issued. It is refused at start, for the same reason as a changed secret.

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

* **Same answer as one session.** Sessions run through shards sign the payload `runChaosLoadTest` signs for the same seed, plus their session id, and every fingerprint is checked against that. Seed 1 run as session `s1` is also pinned to a value computed with `openssl`, outside this code.
* **Session ids are signed.** A test renames the session in a shard's attestation and checks that it no longer verifies, and that the original does not verify as any other session's.
* **Recovery is exact.** Unit tests abandon a shard at arbitrary points, recover it, and compare every session against a reference engine stepped to the same tick.
* **Torn and corrupt logs.** Tests truncate and flip bytes in segments and check that a torn tail is repaired and corruption is refused.
* **A worker dies.** `ShardCluster.killShard()` terminates a worker mid run; the supervisor restarts it, a request made while it was down is answered by the recovered worker, and the run finishes with the reference fingerprints.
* **The contracts.** Tests start a second cluster on a locked directory, cut an advance short with a kill, crash a shard whose alerts are already folded into a checkpoint, and pull a shard's directory out from under `close()`, and check each is reported or recovered as described above.
* **The process dies.** The crash drill (`node src/index.ts --crash-drill`) runs a cluster in a child process and kills it with SIGKILL while it is writing. A second process recovers the directory, carries on, reports the exact tick of every session once they are durable, and is killed the same way. A third recovery, in the drill's own process, must find every session at exactly the reported tick and identical to the engine that never crashed, and must then finish every session on its reference fingerprint. One more clean restart must replay nothing.

The second kill is the one that proves nothing durable was lost. A process killed while it is running can always have finished work it never got to report, so the first kill can only show that recovery copes with a write cut in half. The drill was built that way after an early version, which only killed a running process, let a deliberately broken recovery that dropped its last log record pass every check. The broken version is caught now.

## Measured

Apple M5 Pro with 18 cores, macOS, Node 25.9, the internal SSD. Four shards, 30 seconds in real time through all three phases of the scenario (`--cluster --sessions=N`), reporting the worst shard in any one second window:

| Sessions per shard | Tick work | Loop lag p99 | Checkpoint | Checkpoint freeze | fsync p99 |
| --- | --- | --- | --- | --- | --- |
| 1,000 | 3.2% of one core | 4.5ms | 1.6 MB | 7.8ms | 60ms |
| 2,500 | 5.0% of one core | 9.8ms | 4.1 MB | 13.0ms | 126ms |
| 10,000 | 16.7% of one core | 13.7ms | 16.5 MB | 73.8ms | 75ms |

What the numbers say:

* **CPU is not the limit.** 10,000 sessions on one shard, through the failure cascade, which is the busiest stretch of the scenario, peaked at 17 percent of one core.
* **The checkpoint freeze is.** Turning readable session objects into a checkpoint costs 5 to 8ms per thousand sessions, on the loop. At 10,000 sessions per shard that is 74ms every five seconds, most of a tick. With the current layout, keep shards nearer 5,000 sessions. The structure of arrays layout from Challenge 1.2 turns the freeze into a column copy and removes this ceiling.
* **fsync here is the drive flushing its cache,** 40 to 60ms typically with four shards writing. Group commit absorbs it: a flush slower than a tick simply carries more ticks, which stretches the loss window toward 200ms and no further. Raising the libuv thread pool from 4 to 16 threads changed nothing measurable at four shards. On Linux with an NVMe drive, fdatasync usually takes well under a millisecond.
* **Recovery is fast.** A shard holding 1,000 sessions, killed mid run, was back in 20ms after replaying 11 ticks. At 2,500 sessions it took 43ms for 13 ticks.
* **The log is cheap but not small.** As JSON it costs about 25 bytes per active session per tick, about 2.6 MB/s per shard at 10,000 sessions in the cascade. A binary codec that refers to sessions by slot would cut that roughly fourfold if bandwidth ever matters.

## Not built yet

* **Session migration.** The health signal exists (per shard event loop delay and tick busy time are reported), but nothing moves sessions yet. Moving one needs a handoff: fence the old owner, ship the snapshot, flip the route. The snapshot half exists.
* **Structure of arrays.** Sessions are still one readable object each. Flipping to typed array columns from Challenge 1.2 would make checkpoints a column copy instead of a stringify.
* **Replication.** The log is on a local volume. Shipping it to a second node is the next durability step.
* **Hung worker detection.** A worker that crashes is restarted. A worker stuck in an infinite loop is not detected yet; that needs a heartbeat watchdog.
* **More than one process.** The same router pattern applies one level up, with processes or nodes in place of threads.
* **The socket layer.** `ShardCluster.ingest()` is where a WebSocket server plugs in. Telemetry reaches it as `(sessionId, kind, count)`, already validated and with no client timestamp.
