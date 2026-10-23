import { monitorEventLoopDelay } from 'node:perf_hooks';
import { parentPort, workerData } from 'node:worker_threads';
import { startFixedStep } from '../liveLoop.ts';
import { ClusterError } from './errors.ts';
import type { RouterMessage, ShardRequest, ShardWorkerData, WorkerMessage, WorkerStats } from './protocol.ts';
import { Shard } from './shard.ts';

/**
 * Worker thread entry: one shard per thread.
 *
 * The worker owns the shard's loop. With a realtime clock the shared fixed
 * timestep clock steps the shard and asks for a flush after every tick. With
 * a manual clock the router drives ticks through `advance`, which is what
 * makes a cluster run reproducible. Telemetry arrives in batches and only
 * ever increments inbox counters; all scoring happens on the tick.
 *
 * Any error the shard cannot continue past ends the thread. The supervisor
 * restarts it and the new thread recovers from disk.
 */
const data = workerData as ShardWorkerData;
const port = parentPort!;

function post(message: WorkerMessage): void {
  port.postMessage(message);
}

const { shard, recovery } = await Shard.open({
  dir: data.dir,
  shardId: data.shardId,
  secret: data.secret,
  engineConfig: data.engineConfig,
  shardConfig: data.shardConfig,
  onEvents: (events) => post({ type: 'events', events }),
  onFatal: (error) => {
    // Messages posted before exit are delivered before the router sees the exit.
    post({ type: 'fatal', message: error.message });
    process.exit(1);
  },
});

/**
 * The delay monitor samples on a timer, and every sample includes the timer's
 * own interval, so an idle loop reads about 10ms. Lag is what is left after
 * taking the interval off.
 */
const LAG_SAMPLE_MS = 10;
const lag = monitorEventLoopDelay({ resolution: LAG_SAMPLE_MS });
lag.enable();

function lagMs(sampleNs: number): number {
  return Math.max(0, sampleNs / 1e6 - LAG_SAMPLE_MS);
}
let busyMs = 0;
let windowStart = performance.now();

function tick(): void {
  const started = performance.now();
  shard.step();
  busyMs += performance.now() - started;
}

const clock =
  data.clock === 'realtime'
    ? startFixedStep({
        tickMs: data.engineConfig.tickMs,
        maxTicksPerPump: data.shardConfig.maxCatchUpTicks,
        onTick: () => {
          tick();
          shard.requestFlush();
        },
      })
    : null;

function stats(): WorkerStats {
  const now = performance.now();
  const elapsed = now - windowStart;
  const sampled = lag.count > 0;
  const loop = {
    lagP50Ms: sampled ? lagMs(lag.percentile(50)) : 0,
    lagP99Ms: sampled ? lagMs(lag.percentile(99)) : 0,
    lagMaxMs: sampled ? lagMs(lag.max) : 0,
    busyRatio: elapsed > 0 ? busyMs / elapsed : 0,
    backlogMs: clock?.backlogMs ?? 0,
  };
  lag.reset();
  busyMs = 0;
  windowStart = now;
  return { ...shard.takeStats(), loop };
}

async function handle(request: ShardRequest): Promise<unknown> {
  switch (request.op) {
    case 'open': {
      const result = shard.openSession(request.sessionId);
      // The session is durable before the caller hears it exists.
      await shard.flush();
      return result;
    }
    case 'complete':
      return shard.complete(request.sessionId, request.completedAt);
    case 'inspect': {
      // Snapshot first, then flush: once the flush resolves, everything the
      // snapshot shows is on disk, so a caller never sees state a crash can undo.
      const view = shard.inspect(request.sessionId);
      if (view?.status === 'active' && !view.durable) {
        await shard.flush();
        view.durable = true;
      }
      return view;
    }
    case 'advance': {
      if (clock) throw new ClusterError('BAD_REQUEST', 'advance needs a manual clock');
      for (let i = 0; i < request.ticks; i += 1) tick();
      await shard.flush();
      return shard.tick;
    }
    case 'stats':
      return stats();
    case 'shutdown':
      clock?.stop();
      lag.disable();
      await shard.close();
      return null;
  }
}

function describe(error: unknown): { message: string; code?: ClusterError['code'] } {
  if (error instanceof ClusterError) return { message: error.message, code: error.code };
  return { message: error instanceof Error ? error.message : String(error) };
}

port.on('message', (message: RouterMessage) => {
  if (message.type === 'ingest') {
    const items = message.data;
    for (let i = 0; i + 2 < items.length; i += 3) {
      shard.ingest(items[i] as string, items[i + 1] as number, items[i + 2] as number);
    }
    return;
  }

  const { id, request } = message;
  handle(request).then(
    (value) => {
      post({ type: 'reply', id, ok: true, value });
      if (request.op === 'shutdown') setImmediate(() => process.exit(0));
    },
    (error: unknown) => post({ type: 'reply', id, ok: false, error: describe(error) }),
  );
});

post({ type: 'ready', recovery });
