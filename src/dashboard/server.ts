import http from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  runClusterDemo,
  type ClusterDemoResult,
  type DemoControl,
  type DemoEvent,
} from '../cluster/clusterDemo.ts';
import { PAGE } from './page.ts';

export interface DashboardOptions {
  secret: string;
  shards: number;
  sessions: number;
  seconds: number;
  /** 0 picks a free port. Defaults to 3000. */
  port?: number;
}

export interface Dashboard {
  server: http.Server;
  port: number;
  url: string;
  /** Stops a live run, removes its data directory, drops every stream and releases the port. */
  close(): Promise<void>;
}

type RunStatus = 'idle' | 'running' | 'finished' | 'failed';

/** What the final checks looked like, kept after the run so a page opened later can still show them. */
export interface RunSummary {
  passed: boolean;
  checks: ClusterDemoResult['checks'];
  worst: ClusterDemoResult['worst'];
  alerts: ClusterDemoResult['alerts'];
  sessionsPerShard: number;
}

/** Entries of the event log. A page that connects late is given the log so far. */
type LogEntry = Exclude<DemoEvent, { type: 'stats' | 'started' }> | { type: 'run' } | { type: 'finished'; passed: boolean } | { type: 'failed'; error: string };

const LOG_LIMIT = 200;
const HEARTBEAT_MS = 15_000;
const HOST = '127.0.0.1';
const DEFAULT_PORT = 3000;

function whole(name: string, value: number, min: number, max = Number.MAX_SAFE_INTEGER): void {
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new RangeError(`${name} must be a whole number from ${min} to ${max}, got ${value}`);
  }
}

/**
 * A live view of the cluster demo. It runs `runClusterDemo` on request, one run
 * at a time, and streams what the demo reports to every connected page as
 * Server-Sent Events. It listens on loopback only and has no dependencies.
 */
export async function startDashboard(options: DashboardOptions): Promise<Dashboard> {
  const { secret, shards, sessions, seconds } = options;
  const wantedPort = options.port ?? DEFAULT_PORT;
  whole('shards', shards, 1);
  whole('sessions', sessions, 1);
  whole('seconds', seconds, 1);
  whole('port', wantedPort, 0, 65_535);
  const config = { shards, sessions, seconds };

  let status: RunStatus = 'idle';
  let latestStats: Extract<DemoEvent, { type: 'stats' }> | null = null;
  let log: LogEntry[] = [];
  let summary: RunSummary | null = null;
  let failure: string | null = null;
  let control: DemoControl | null = null;
  let aborter: AbortController | null = null;
  let running: Promise<void> = Promise.resolve();
  let closing = false;
  const streams = new Set<http.ServerResponse>();

  function publish(name: string, data: object): void {
    const frame = `event: ${name}\ndata: ${JSON.stringify(data)}\n\n`;
    for (const stream of streams) stream.write(frame);
  }

  function record(entry: LogEntry): void {
    log.push(entry);
    if (log.length > LOG_LIMIT) log = log.slice(-LOG_LIMIT);
    publish(entry.type, entry);
  }

  function onEvent(event: DemoEvent): void {
    if (event.type === 'stats') {
      latestStats = event;
      publish('stats', event);
    } else if (event.type === 'started') {
      publish('started', event);
    } else {
      record(event);
    }
  }

  function beginRun(): void {
    status = 'running';
    latestStats = null;
    summary = null;
    failure = null;
    log = [];
    const abort = new AbortController();
    aborter = abort;
    record({ type: 'run' });

    running = runClusterDemo({
      secret,
      shards,
      sessions,
      seconds,
      log: () => {},
      onEvent,
      onControl: (handle) => {
        control = handle;
      },
      signal: abort.signal,
    }).then(
      (result) => {
        summary = {
          passed: result.checks.every((check) => check.ok),
          checks: result.checks,
          worst: result.worst,
          alerts: result.alerts,
          sessionsPerShard: result.sessionsPerShard,
        };
        status = 'finished';
        log.push({ type: 'finished', passed: summary.passed });
        publish('finished', { type: 'finished', ...summary });
      },
      (error: unknown) => {
        if (abort.signal.aborted) return;
        failure = error instanceof Error ? error.message : String(error);
        status = 'failed';
        record({ type: 'failed', error: failure });
      },
    ).finally(() => {
      control = null;
      aborter = null;
    });
  }

  function snapshot(): object {
    return { status, config, stats: latestStats, events: log, result: summary, error: failure };
  }

  function send(res: http.ServerResponse, code: number, body: string, type = 'text/plain; charset=utf-8'): void {
    res.writeHead(code, { 'content-type': type, 'cache-control': 'no-store' });
    res.end(body);
  }

  function sendJson(res: http.ServerResponse, code: number, body: object): void {
    send(res, code, JSON.stringify(body), 'application/json; charset=utf-8');
  }

  let port = 0;
  /**
   * A page on this machine's loopback port is reachable by any web page in the
   * user's browser. The Host check stops DNS rebinding, and the Origin check
   * stops another site from posting a kill or a start to it.
   */
  function sameSite(req: http.IncomingMessage): boolean {
    const host = /^(127\.0\.0\.1|localhost|\[::1\])(?::(\d+))?$/.exec(req.headers.host ?? '');
    if (host === null || (host[2] === undefined ? port !== 80 : Number(host[2]) !== port)) return false;
    const origin = req.headers.origin;
    return origin === undefined || origin === `http://${req.headers.host}`;
  }

  async function kill(res: http.ServerResponse, raw: string): Promise<void> {
    const shardId = Number(raw);
    if (!/^\d+$/.test(raw) || shardId >= shards) {
      sendJson(res, 400, { error: `shard must be a whole number from 0 to ${shards - 1}` });
      return;
    }
    if (status !== 'running') {
      sendJson(res, 409, { error: 'no run is active' });
      return;
    }
    if (control === null) {
      sendJson(res, 409, { error: 'the run is still starting' });
      return;
    }
    try {
      await control.killShard(shardId);
    } catch (error) {
      sendJson(res, 409, { error: error instanceof Error ? error.message : String(error) });
      return;
    }
    sendJson(res, 202, { ok: true, shardId });
  }

  function events(res: http.ServerResponse): void {
    res.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-store',
      connection: 'keep-alive',
    });
    res.write(`retry: 2000\nevent: snapshot\ndata: ${JSON.stringify(snapshot())}\n\n`);
    streams.add(res);
    // The response closes with the connection; the request closes as soon as its (empty) body is read.
    res.on('close', () => streams.delete(res));
  }

  const server = http.createServer((req, res) => {
    req.resume();
    const path = (req.url ?? '/').split('?')[0]!;
    const method = req.method ?? 'GET';
    const killMatch = /^\/kill\/([^/]+)$/.exec(path);
    const known = path === '/' || path === '/events' || path === '/start' || killMatch !== null;

    if (!sameSite(req)) {
      send(res, 403, 'forbidden');
      return;
    }
    if (!known) {
      send(res, 404, 'not found');
      return;
    }
    const wanted = path === '/' || path === '/events' ? 'GET' : 'POST';
    if (method !== wanted) {
      res.setHeader('allow', wanted);
      send(res, 405, 'method not allowed');
      return;
    }

    if (path === '/') {
      res.writeHead(200, {
        'content-type': 'text/html; charset=utf-8',
        'cache-control': 'no-store',
        'content-security-policy':
          "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'",
      });
      res.end(PAGE);
    } else if (path === '/events') {
      events(res);
    } else if (closing) {
      sendJson(res, 503, { error: 'the dashboard is shutting down' });
    } else if (path === '/start') {
      if (status === 'running') {
        sendJson(res, 409, { error: 'a run is already in progress' });
        return;
      }
      beginRun();
      sendJson(res, 202, { ok: true });
    } else {
      void kill(res, killMatch![1]!);
    }
  });

  const heartbeat = setInterval(() => {
    for (const stream of streams) stream.write(': keep-alive\n\n');
  }, HEARTBEAT_MS);
  heartbeat.unref();

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(wantedPort, HOST, () => {
      server.off('error', reject);
      resolve();
    });
  });
  port = (server.address() as AddressInfo).port;

  let closed: Promise<void> | null = null;
  return {
    server,
    port,
    url: `http://${HOST}:${port}`,
    close() {
      closed ??= (async () => {
        closing = true;
        clearInterval(heartbeat);
        const stopped = new Promise<void>((resolve) => server.close(() => resolve()));
        aborter?.abort();
        await running;
        for (const stream of streams) stream.end();
        server.closeAllConnections();
        await stopped;
      })();
      return closed;
    },
  };
}
