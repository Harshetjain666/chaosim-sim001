/**
 * The dashboard page: one HTML document with its CSS and JS inline, so the
 * server has nothing else to serve and the page loads nothing from anywhere
 * else. It draws only from the Server-Sent Events at `/events` and posts to
 * `/start` and `/kill/:shard`. There are no template placeholders below: the
 * run's settings arrive in the first `snapshot` frame, so this text never
 * changes between runs.
 */
export const PAGE = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SIM-001 cluster</title>
<style>
  :root {
    color-scheme: light dark;
    --bg: #f5f6f8;
    --surface: #ffffff;
    --surface-2: #eef0f4;
    --text: #15181d;
    --muted: #5b6573;
    --line: #d9dde4;
    --accent: #2457d6;
    --accent-text: #ffffff;
    --ok: #17803d;
    --ok-bg: #dcf3e4;
    --warn: #8a5a00;
    --warn-bg: #fdecc4;
    --bad: #b3261e;
    --bad-bg: #fbdcd9;
    --idle-bg: #e4e7ec;
    --shadow: 0 1px 2px rgba(20, 24, 29, 0.06);
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0f1216;
      --surface: #171b21;
      --surface-2: #1f242c;
      --text: #e8ebf0;
      --muted: #9aa5b4;
      --line: #2c333d;
      --accent: #7ba3ff;
      --accent-text: #0b1220;
      --ok: #6fd392;
      --ok-bg: #12301e;
      --warn: #f0c565;
      --warn-bg: #3a2c0a;
      --bad: #ff8e86;
      --bad-bg: #3d1714;
      --idle-bg: #262c35;
      --shadow: none;
    }
  }
  * { box-sizing: border-box; }
  html { -webkit-text-size-adjust: 100%; }
  body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font: 15px/1.45 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
  }
  .wrap { max-width: 1100px; margin: 0 auto; padding: 16px; }
  header { display: flex; flex-wrap: wrap; align-items: center; gap: 12px 16px; margin-bottom: 16px; }
  h1 { margin: 0; font-size: 20px; letter-spacing: 0.01em; }
  h1 span { color: var(--muted); font-weight: 400; }
  h2 { margin: 0 0 10px; font-size: 13px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }
  .spacer { flex: 1 1 auto; }
  .pill {
    display: inline-block; padding: 2px 10px; border-radius: 999px;
    font-size: 12px; font-weight: 600; background: var(--idle-bg); color: var(--muted);
  }
  .pill.up, .pill.finished { background: var(--ok-bg); color: var(--ok); }
  .pill.running { background: var(--ok-bg); color: var(--ok); }
  .pill.down, .pill.starting { background: var(--warn-bg); color: var(--warn); }
  .pill.failed { background: var(--bad-bg); color: var(--bad); }
  button {
    font: inherit; font-weight: 600; min-height: 44px; padding: 0 18px;
    border-radius: 8px; border: 1px solid var(--line); background: var(--surface); color: var(--text);
    cursor: pointer;
  }
  button:hover:not(:disabled) { border-color: var(--accent); }
  button:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
  button:disabled { opacity: 0.45; cursor: not-allowed; }
  button.primary { background: var(--accent); border-color: var(--accent); color: var(--accent-text); }
  .panel {
    background: var(--surface); border: 1px solid var(--line); border-radius: 12px;
    padding: 14px; box-shadow: var(--shadow);
  }
  .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-bottom: 16px; }
  .stat .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; }
  .stat .value { font-size: 28px; font-weight: 650; font-variant-numeric: tabular-nums; }
  .bar { height: 6px; border-radius: 3px; background: var(--surface-2); overflow: hidden; margin-top: 6px; }
  .bar > div { height: 100%; width: 0; background: var(--accent); transition: width 0.4s linear; }
  .shards { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 12px; margin-bottom: 16px; }
  .card-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
  .card-head strong { font-size: 16px; }
  dl.metrics { display: grid; grid-template-columns: 1fr auto; gap: 4px 12px; margin: 0 0 12px; }
  dl.metrics dt { color: var(--muted); }
  dl.metrics dd { margin: 0; text-align: right; font-variant-numeric: tabular-nums; font-weight: 600; }
  .card button { width: 100%; }
  .lower { display: grid; grid-template-columns: 1fr; gap: 12px; }
  @media (min-width: 800px) { .lower { grid-template-columns: 3fr 2fr; align-items: start; } }
  ol.log { list-style: none; margin: 0; padding: 0; max-height: 320px; overflow-y: auto; }
  ol.log li { padding: 6px 0; border-top: 1px solid var(--line); overflow-wrap: anywhere; }
  ol.log li:first-child { border-top: 0; }
  ol.log .t { color: var(--muted); font-variant-numeric: tabular-nums; margin-right: 8px; }
  ol.log .kill, ol.log .failed { color: var(--bad); }
  ol.log .back { color: var(--ok); }
  ul.checks { list-style: none; margin: 0; padding: 0; }
  ul.checks li { padding: 8px 0; border-top: 1px solid var(--line); }
  ul.checks li:first-child { border-top: 0; }
  ul.checks .mark { display: inline-block; min-width: 3.2em; font-weight: 700; }
  ul.checks .pass .mark { color: var(--ok); }
  ul.checks .fail .mark { color: var(--bad); }
  ul.checks .detail { display: block; color: var(--muted); font-size: 13px; }
  .empty { color: var(--muted); margin: 0; }
  .verdict { font-weight: 700; margin: 0 0 8px; }
  .verdict.pass { color: var(--ok); }
  .verdict.fail { color: var(--bad); }
  [hidden] { display: none !important; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>SIM-001 <span>cluster</span></h1>
    <span id="status" class="pill">connecting</span>
    <span class="spacer"></span>
    <button id="start" class="primary" type="button" disabled>Start run</button>
  </header>

  <section class="summary">
    <div class="panel stat"><div class="label">Run</div><div class="value" id="clock">0 / 0 s</div><div class="bar"><div id="progress"></div></div></div>
    <div class="panel stat"><div class="label">Alerts</div><div class="value" id="alerts">0</div></div>
    <div class="panel stat"><div class="label">Shards up</div><div class="value" id="up">0 / 0</div></div>
    <div class="panel stat"><div class="label">Sessions</div><div class="value" id="sessions">0</div></div>
  </section>

  <section class="shards" id="shards" aria-label="Shards"></section>

  <section class="lower">
    <div class="panel">
      <h2>Events</h2>
      <ol class="log" id="log"></ol>
      <p class="empty" id="log-empty">Nothing has happened yet. Start a run.</p>
    </div>
    <div class="panel">
      <h2>Acceptance checks</h2>
      <p class="verdict" id="verdict" hidden></p>
      <ul class="checks" id="checks"></ul>
      <p class="empty" id="checks-empty">The checks run when the run ends.</p>
    </div>
  </section>
</div>

<script>
(function () {
  'use strict';

  var $ = function (id) { return document.getElementById(id); };
  var model = { status: 'idle', config: { shards: 0, sessions: 0, seconds: 0 }, stats: null, running: false };
  var cards = [];

  function el(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function num(value, digits, unit) {
    return value === null || value === undefined ? '-' : value.toFixed(digits) + unit;
  }

  function buildCards(count) {
    var holder = $('shards');
    if (cards.length === count) return;
    holder.textContent = '';
    cards = [];
    for (var i = 0; i < count; i += 1) {
      (function (shardId) {
        var card = el('article', 'panel card');
        var head = el('div', 'card-head');
        head.appendChild(el('strong', '', 'Shard ' + shardId));
        var pill = el('span', 'pill', 'idle');
        head.appendChild(pill);
        card.appendChild(head);
        var list = el('dl', 'metrics');
        var fields = {};
        [
          ['ticks', 'Ticks per second'], ['busy', 'Busy'], ['lag', 'Loop lag p99'],
          ['fsync', 'Fsync p99'], ['wal', 'WAL'], ['restarts', 'Restarts']
        ].forEach(function (pair) {
          list.appendChild(el('dt', '', pair[1]));
          fields[pair[0]] = el('dd', '', '-');
          list.appendChild(fields[pair[0]]);
        });
        card.appendChild(list);
        var kill = el('button', '', 'Kill shard ' + shardId);
        kill.type = 'button';
        kill.disabled = true;
        kill.addEventListener('click', function () { killShard(shardId); });
        card.appendChild(kill);
        holder.appendChild(card);
        cards.push({ pill: pill, fields: fields, kill: kill });
      })(i);
    }
  }

  function setStatus(status) {
    model.status = status;
    model.running = status === 'running';
    var pill = $('status');
    pill.textContent = status;
    pill.className = 'pill ' + status;
    $('start').disabled = model.running;
    refreshKillButtons();
  }

  function refreshKillButtons() {
    cards.forEach(function (card, i) {
      var line = model.stats && model.stats.shards[i];
      card.kill.disabled = !(model.running && line && line.state === 'up');
    });
  }

  function drawStats(stats) {
    model.stats = stats;
    var cfg = model.config;
    var up = 0;
    stats.shards.forEach(function (line, i) {
      var card = cards[i];
      if (!card) return;
      if (line.state === 'up') up += 1;
      card.pill.textContent = line.state;
      card.pill.className = 'pill ' + line.state;
      card.fields.ticks.textContent = num(line.ticksPerSec, 0, '');
      card.fields.busy.textContent = line.busyRatio === null ? '-' : (line.busyRatio * 100).toFixed(1) + '%';
      card.fields.lag.textContent = num(line.lagP99Ms, 1, ' ms');
      card.fields.fsync.textContent = num(line.fsyncP99Ms, 1, ' ms');
      card.fields.wal.textContent = line.walBytesPerSec === null ? '-' : (line.walBytesPerSec / 1024).toFixed(0) + ' KB/s';
      card.fields.restarts.textContent = String(line.restarts);
    });
    $('up').textContent = up + ' / ' + cfg.shards;
    $('alerts').textContent = String(stats.alerts);
    $('clock').textContent = stats.second + ' / ' + stats.seconds + ' s';
    $('progress').style.width = (100 * stats.second / Math.max(1, stats.seconds)) + '%';
    refreshKillButtons();
  }

  function resetCards() {
    model.stats = null;
    cards.forEach(function (card) {
      card.pill.textContent = 'idle';
      card.pill.className = 'pill';
      Object.keys(card.fields).forEach(function (key) { card.fields[key].textContent = '-'; });
    });
    $('up').textContent = '0 / ' + model.config.shards;
    $('alerts').textContent = '0';
    $('clock').textContent = '0 / ' + model.config.seconds + ' s';
    $('progress').style.width = '0';
    refreshKillButtons();
  }

  function setConfig(config) {
    model.config = config;
    $('sessions').textContent = String(config.sessions);
    buildCards(config.shards);
    if (!model.stats) {
      $('up').textContent = '0 / ' + config.shards;
      $('clock').textContent = '0 / ' + config.seconds + ' s';
    }
  }

  function describe(entry) {
    var at = entry.second === undefined ? '' : 't=' + entry.second + 's';
    switch (entry.type) {
      case 'run':
        return { at: '', cls: '', text: 'Run started: ' + model.config.sessions + ' sessions on ' + model.config.shards + ' shards for ' + model.config.seconds + ' s' };
      case 'killed':
        return { at: at, cls: 'kill', text: 'Shard ' + entry.shardId + ' killed: worker terminated, no flush, no checkpoint' };
      case 'recovered':
        return { at: at, cls: 'back', text: 'Shard ' + entry.shardId + ' back: replayed ' + entry.report.ticksReplayed + ' ticks after checkpoint seq ' + (entry.report.checkpointSeq === null ? 'none' : entry.report.checkpointSeq) + ' in ' + entry.report.durationMs.toFixed(1) + ' ms' };
      case 'kill-failed':
        return { at: at, cls: 'failed', text: 'Shard ' + entry.shardId + ' did not come back: ' + entry.error };
      case 'finished':
        return { at: '', cls: entry.passed ? 'back' : 'failed', text: entry.passed ? 'Run finished: all checks passed' : 'Run finished: some checks failed' };
      case 'failed':
        return { at: '', cls: 'failed', text: 'Run failed: ' + entry.error };
      case 'note':
        return { at: '', cls: 'failed', text: entry.text };
      default:
        return null;
    }
  }

  function addLog(entry) {
    var line = describe(entry);
    if (!line) return;
    var item = el('li', line.cls);
    if (line.at) item.appendChild(el('span', 't', line.at));
    item.appendChild(document.createTextNode(line.text));
    var log = $('log');
    log.insertBefore(item, log.firstChild);
    $('log-empty').hidden = true;
  }

  function clearLog() {
    $('log').textContent = '';
    $('log-empty').hidden = false;
  }

  function drawResult(result) {
    var list = $('checks');
    list.textContent = '';
    $('checks-empty').hidden = !!result;
    $('verdict').hidden = !result;
    if (!result) return;
    var verdict = $('verdict');
    verdict.textContent = result.passed ? 'ALL CHECKS PASSED' : 'SOME CHECKS FAILED';
    verdict.className = 'verdict ' + (result.passed ? 'pass' : 'fail');
    result.checks.forEach(function (check) {
      var item = el('li', check.ok ? 'pass' : 'fail');
      var head = el('span', '', check.label);
      item.appendChild(el('span', 'mark', check.ok ? 'PASS' : 'FAIL'));
      item.appendChild(head);
      item.appendChild(el('span', 'detail', check.detail));
      list.appendChild(item);
    });
    var note = el('li', '');
    note.appendChild(el('span', 'detail', result.alerts.delivered + ' alerts delivered, ' + result.alerts.duplicatesDropped + ' replayed duplicates dropped'));
    list.appendChild(note);
  }

  function post(path) {
    return fetch(path, { method: 'POST' }).then(function (reply) {
      return reply.text().then(function (body) { return { ok: reply.ok, status: reply.status, body: body }; });
    });
  }

  function refuse(what, outcome) {
    var message = outcome.body;
    try { message = JSON.parse(outcome.body).error || outcome.body; } catch (ignore) {}
    addLog({ type: 'note', text: what + ' refused (' + outcome.status + '): ' + message });
  }

  function killShard(shardId) {
    post('/kill/' + shardId).then(function (outcome) {
      if (!outcome.ok) refuse('Kill shard ' + shardId, outcome);
    });
  }

  $('start').addEventListener('click', function () {
    $('start').disabled = true;
    post('/start').then(function (outcome) {
      if (!outcome.ok) {
        refuse('Start', outcome);
        $('start').disabled = model.running;
      }
    });
  });

  var source = new EventSource('/events');
  source.addEventListener('open', function () { if (model.status === 'connecting') setStatus('idle'); });
  source.addEventListener('error', function () { $('status').textContent = 'reconnecting'; $('status').className = 'pill down'; $('start').disabled = true; refreshKillButtons(); });

  function on(name, handler) {
    source.addEventListener(name, function (event) { handler(JSON.parse(event.data)); });
  }

  on('snapshot', function (snap) {
    setConfig(snap.config);
    clearLog();
    snap.events.slice().forEach(addLog);
    if (snap.stats) drawStats(snap.stats); else resetCards();
    drawResult(snap.result);
    if (snap.status === 'failed') addLog({ type: 'failed', error: snap.error });
    setStatus(snap.status);
  });
  on('run', function (entry) {
    clearLog();
    resetCards();
    drawResult(null);
    addLog(entry);
    setStatus('running');
  });
  on('started', function () { refreshKillButtons(); });
  on('stats', drawStats);
  ['killed', 'recovered', 'kill-failed'].forEach(function (name) { on(name, addLog); });
  on('finished', function (result) {
    addLog(result);
    drawResult(result);
    setStatus('finished');
  });
  on('failed', function (entry) {
    addLog(entry);
    setStatus('failed');
  });
})();
</script>
</body>
</html>
`;
