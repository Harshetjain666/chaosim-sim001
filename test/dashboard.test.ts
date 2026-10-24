import { describe, test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import http from 'node:http';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { startDashboard, type Dashboard, type DashboardOptions } from '../src/dashboard/server.ts';

const SECRET = 'sim-001-dev-secret';

interface Reply {
  status: number;
  headers: http.IncomingHttpHeaders;
  body: string;
}

function request(
  port: number,
  method: string,
  path: string,
  headers: Record<string, string> = {},
): Promise<Reply> {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, method, path, headers }, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk: string) => (body += chunk));
      res.on('end', () => resolve({ status: res.statusCode ?? 0, headers: res.headers, body }));
    });
    req.on('error', reject);
    req.end();
  });
}

interface Frame {
  event: string;
  data: any;
}

/** A Server-Sent Events subscriber that keeps every frame it has seen. */
interface Feed {
  frames: Frame[];
  /** The first frame, at or after `from`, that satisfies `match`. */
  next(match: (frame: Frame) => boolean, timeoutMs?: number): Promise<Frame>;
  close(): void;
}

function subscribe(port: number): Promise<Feed> {
  return new Promise((resolve, reject) => {
    const frames: Frame[] = [];
    const waiters = new Set<() => void>();
    let buffer = '';
    const req = http.get({ host: '127.0.0.1', port, path: '/events' }, (res) => {
      assert.equal(res.statusCode, 200);
      assert.match(String(res.headers['content-type']), /^text\/event-stream/);
      res.setEncoding('utf8');
      res.on('data', (chunk: string) => {
        buffer += chunk;
        for (let end = buffer.indexOf('\n\n'); end >= 0; end = buffer.indexOf('\n\n')) {
          const block = buffer.slice(0, end);
          buffer = buffer.slice(end + 2);
          let event = 'message';
          let data = '';
          for (const line of block.split('\n')) {
            if (line.startsWith('event: ')) event = line.slice(7);
            else if (line.startsWith('data: ')) data += line.slice(6);
          }
          if (data === '') continue;
          frames.push({ event, data: JSON.parse(data) });
        }
        for (const wake of [...waiters]) wake();
      });
      resolve({
        frames,
        next(match, timeoutMs = 15_000) {
          return new Promise((found, fail) => {
            const check = (): boolean => {
              const hit = frames.find(match);
              if (hit === undefined) return false;
              waiters.delete(check);
              clearTimeout(timer);
              found(hit);
              return true;
            };
            const timer = setTimeout(() => {
              waiters.delete(check);
              fail(new Error(`no matching frame in ${timeoutMs}ms; saw ${frames.map((f) => f.event).join(',')}`));
            }, timeoutMs);
            if (!check()) waiters.add(check);
          });
        },
        close: () => req.destroy(),
      });
    });
    req.on('error', (error) => {
      // Closing the feed on purpose destroys the socket.
      if ((error as NodeJS.ErrnoException).code !== 'ECONNRESET') reject(error);
    });
  });
}

function is(event: string, extra?: (data: any) => boolean): (frame: Frame) => boolean {
  return (frame) => frame.event === event && (extra === undefined || extra(frame.data));
}

const SHORT: Omit<DashboardOptions, 'port'> = { secret: SECRET, shards: 2, sessions: 20, seconds: 3 };

/** A dashboard on a free port, closed when the test ends. */
async function dashboardFor(t: TestContext, overrides: Partial<DashboardOptions> = {}): Promise<Dashboard> {
  const dashboard = await startDashboard({ ...SHORT, port: 0, ...overrides });
  t.after(() => dashboard.close());
  return dashboard;
}

async function feedFor(t: TestContext, dashboard: Dashboard): Promise<Feed> {
  const feed = await subscribe(dashboard.port);
  t.after(() => feed.close());
  return feed;
}

describe('the page', () => {
  test('GET / serves one self contained HTML page', async (t) => {
    const dashboard = await dashboardFor(t);
    const reply = await request(dashboard.port, 'GET', '/');

    assert.equal(reply.status, 200);
    assert.match(String(reply.headers['content-type']), /^text\/html/);
    assert.match(reply.body, /^<!doctype html>/i);
    assert.match(reply.body, /<meta name="viewport"/, 'phone width');
    assert.match(reply.body, /prefers-color-scheme: dark/, 'dark theme');
    assert.match(reply.body, /EventSource\(['"]\/events['"]\)/, 'listens to /events');
    assert.match(reply.body, /Start run/);
    assert.doesNotMatch(reply.body, /(?:src|href)\s*=\s*["'](?:https?:)?\/\//i, 'no external assets');
    assert.doesNotMatch(reply.body, /@import|url\(\s*["']?https?:/i, 'no external CSS');
  });

  test('it listens on loopback only', async (t) => {
    const dashboard = await dashboardFor(t);
    const address = dashboard.server.address();
    assert.ok(address !== null && typeof address === 'object');
    assert.equal(address.address, '127.0.0.1');
    assert.equal(dashboard.url, `http://127.0.0.1:${dashboard.port}`);
  });

  test('unknown paths are 404 and known paths refuse the wrong method', async (t) => {
    const dashboard = await dashboardFor(t);
    assert.equal((await request(dashboard.port, 'GET', '/nope')).status, 404);
    assert.equal((await request(dashboard.port, 'POST', '/')).status, 405);
    assert.equal((await request(dashboard.port, 'GET', '/start')).status, 405);
    assert.equal((await request(dashboard.port, 'GET', '/kill/0')).status, 405);
  });
});

describe('POST /kill/:shard', () => {
  test('a shard that is not a whole number inside the cluster is a 400', async (t) => {
    const dashboard = await dashboardFor(t);
    for (const shard of ['abc', '-1', '1.5', '2', '99', '1e0', '0x1', '%20', '99999999999999999999']) {
      const reply = await request(dashboard.port, 'POST', `/kill/${shard}`);
      assert.equal(reply.status, 400, `/kill/${shard}`);
      assert.match(reply.body, /shard/i);
    }
    assert.equal((await request(dashboard.port, 'POST', '/kill/')).status, 404);
  });

  test('a valid shard with no run active is a 409', async (t) => {
    const dashboard = await dashboardFor(t);
    const reply = await request(dashboard.port, 'POST', '/kill/0');
    assert.equal(reply.status, 409);
    assert.match(reply.body, /no run/i);
  });

  test('a bad shard is a 400 even when a run is active, and a 409 never hides it', async (t) => {
    const dashboard = await dashboardFor(t);
    const feed = await feedFor(t, dashboard);
    assert.equal((await request(dashboard.port, 'POST', '/start')).status, 202);
    await feed.next(is('started'));
    assert.equal((await request(dashboard.port, 'POST', '/kill/7')).status, 400);
    assert.equal((await request(dashboard.port, 'POST', '/kill/x')).status, 400);
  });
});

describe('a run', () => {
  test('streams stats that cover every shard, and ends with every check passing', async (t) => {
    const dashboard = await dashboardFor(t);
    const feed = await feedFor(t, dashboard);

    const hello = await feed.next(is('snapshot'));
    assert.equal(hello.data.status, 'idle');
    assert.deepEqual(hello.data.config, { shards: 2, sessions: 20, seconds: 3 });

    const started = await request(dashboard.port, 'POST', '/start');
    assert.equal(started.status, 202);

    // One run at a time.
    const again = await request(dashboard.port, 'POST', '/start');
    assert.equal(again.status, 409);
    assert.match(again.body, /already/i);

    const stats = await feed.next(is('stats', (d) => d.second === 2));
    assert.equal(stats.data.seconds, 3);
    assert.equal(typeof stats.data.alerts, 'number');
    for (const frame of feed.frames.filter(is('stats'))) {
      assert.deepEqual(
        frame.data.shards.map((s: any) => s.shardId),
        [0, 1],
        'every frame covers every shard',
      );
      for (const shard of frame.data.shards) {
        assert.equal(shard.state, 'up');
        for (const key of ['ticksPerSec', 'busyRatio', 'lagP99Ms', 'fsyncP99Ms', 'walBytesPerSec', 'restarts']) {
          assert.equal(typeof shard[key], 'number', `${key} on shard ${shard.shardId}`);
        }
      }
    }
    // At 10 ticks a second a live shard cannot be idle.
    assert.ok(stats.data.shards.every((s: any) => s.ticksPerSec > 0));

    // A browser that connects mid run is brought up to date.
    const late = await subscribe(dashboard.port);
    t.after(() => late.close());
    const snapshot = await late.next(is('snapshot'));
    assert.equal(snapshot.data.status, 'running');
    assert.ok(snapshot.data.stats.shards.length === 2, 'the latest stats frame');

    const finished = await feed.next(is('finished'));
    assert.ok(finished.data.checks.length >= 4);
    for (const check of finished.data.checks) assert.equal(check.ok, true, `${check.label}: ${check.detail}`);
    assert.equal(finished.data.passed, true);

    const after = await subscribe(dashboard.port);
    t.after(() => after.close());
    const done = await after.next(is('snapshot'));
    assert.equal(done.data.status, 'finished');
    assert.equal(done.data.result.passed, true, 'the checks stay up after the run');
  });

  test('a kill is acknowledged at once and the shard comes back with a recovered event', async (t) => {
    const dashboard = await dashboardFor(t);
    const feed = await feedFor(t, dashboard);
    await request(dashboard.port, 'POST', '/start');
    await feed.next(is('started'));

    const kill = await request(dashboard.port, 'POST', '/kill/0');
    assert.equal(kill.status, 202);
    assert.deepEqual(JSON.parse(kill.body), { ok: true, shardId: 0 });

    // The shard is down until its supervisor has restarted it, and cannot be killed twice.
    const twice = await request(dashboard.port, 'POST', '/kill/0');
    assert.equal(twice.status, 409);
    assert.match(twice.body, /not up/i);

    await feed.next(is('killed', (d) => d.shardId === 0));
    const recovered = await feed.next(is('recovered', (d) => d.shardId === 0));
    assert.equal(typeof recovered.data.report.ticksReplayed, 'number');
    assert.equal(typeof recovered.data.report.durationMs, 'number');

    const finished = await feed.next(is('finished'));
    for (const check of finished.data.checks) assert.equal(check.ok, true, `${check.label}: ${check.detail}`);
    assert.ok(
      finished.data.checks.some((c: any) => /killed shard recovered/i.test(c.label)),
      'a manual kill is held to the same recovery check as a scheduled one',
    );
  });

  test('a run can follow a run, and its data directory is gone each time', async (t) => {
    const scratch = await mkdtemp(join(tmpdir(), 'dashboard-test-'));
    const saved = process.env.TMPDIR;
    process.env.TMPDIR = scratch;
    t.after(async () => {
      if (saved === undefined) delete process.env.TMPDIR;
      else process.env.TMPDIR = saved;
      await rm(scratch, { recursive: true, force: true });
    });

    const dashboard = await dashboardFor(t, { seconds: 1, sessions: 8 });
    const feed = await feedFor(t, dashboard);
    for (let run = 1; run <= 2; run += 1) {
      assert.equal((await request(dashboard.port, 'POST', '/start')).status, 202, `run ${run}`);
      await feed.next(is('started'), 15_000);
      assert.deepEqual(
        (await readdir(scratch)).filter((name) => name.startsWith('chaosim-cluster-')).length,
        1,
        'one fresh data directory while the run is live',
      );
      await feed.next((f) => f.event === 'finished' && feed.frames.filter(is('finished')).length === run);
      assert.deepEqual(await readdir(scratch), [], 'removed afterwards');
    }
  });

  test('closing the dashboard mid run stops it and removes the data directory', async (t) => {
    const scratch = await mkdtemp(join(tmpdir(), 'dashboard-test-'));
    const saved = process.env.TMPDIR;
    process.env.TMPDIR = scratch;
    t.after(async () => {
      if (saved === undefined) delete process.env.TMPDIR;
      else process.env.TMPDIR = saved;
      await rm(scratch, { recursive: true, force: true });
    });

    const dashboard = await startDashboard({ ...SHORT, port: 0, seconds: 30 });
    const feed = await subscribe(dashboard.port);
    t.after(() => feed.close());
    await request(dashboard.port, 'POST', '/start');
    await feed.next(is('stats'));

    await dashboard.close();
    assert.deepEqual(await readdir(scratch), [], 'no data directory, so no LOCK either');
    await assert.rejects(request(dashboard.port, 'GET', '/'), 'the port is released');
  });
});

describe('a page you did not open', () => {
  test('refuses a foreign Host header, which is how DNS rebinding reaches a loopback server', async (t) => {
    const dashboard = await dashboardFor(t);
    const reply = await request(dashboard.port, 'GET', '/', { host: 'evil.example' });
    assert.equal(reply.status, 403);
  });

  test('refuses a POST from another origin, and accepts one from its own page', async (t) => {
    const dashboard = await dashboardFor(t);
    const foreign = await request(dashboard.port, 'POST', '/start', { origin: 'https://evil.example' });
    assert.equal(foreign.status, 403);
    // The refused request must not have started a run.
    const probe = await subscribe(dashboard.port);
    t.after(() => probe.close());
    assert.equal((await probe.next(is('snapshot'))).data.status, 'idle');

    const own = await request(dashboard.port, 'POST', '/start', { origin: dashboard.url });
    assert.equal(own.status, 202);
  });
});

describe('the serve mode', () => {
  test('SIGINT closes the cluster, removes the data directory and exits 0', async (t) => {
    const scratch = await mkdtemp(join(tmpdir(), 'dashboard-test-'));
    t.after(() => rm(scratch, { recursive: true, force: true }));

    const child = spawn(
      process.execPath,
      ['src/index.ts', '--serve', '--port=0', '--shards=2', '--sessions=20', '--seconds=30'],
      { env: { ...process.env, TMPDIR: scratch }, stdio: ['ignore', 'pipe', 'inherit'] },
    );
    t.after(() => child.kill('SIGKILL'));
    let output = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk: string) => (output += chunk));
    const exited = new Promise<{ code: number | null; signal: string | null }>((resolve) =>
      child.once('exit', (code, signal) => resolve({ code, signal })),
    );

    const until = async (predicate: () => boolean | Promise<boolean>, what: string): Promise<void> => {
      const deadline = Date.now() + 20_000;
      while (!(await predicate())) {
        if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}; output so far:\n${output}`);
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    };

    await until(() => /http:\/\/127\.0\.0\.1:\d+/.test(output), 'the listening line');
    const port = Number(/http:\/\/127\.0\.0\.1:(\d+)/.exec(output)![1]);
    assert.equal((await request(port, 'POST', '/start')).status, 202);
    await until(
      async () => (await readdir(scratch)).some((name) => name.startsWith('chaosim-cluster-')),
      'a live data directory',
    );

    child.kill('SIGINT');
    const { code, signal } = await exited;
    assert.deepEqual({ code, signal }, { code: 0, signal: null });
    assert.deepEqual(await readdir(scratch), [], 'the run directory, and its LOCK, are gone');
  });
});
