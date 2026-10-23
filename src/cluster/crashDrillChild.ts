/**
 * The process the crash drill kills.
 *
 *   node src/cluster/crashDrillChild.ts <dataDir> <shards> <sessions> [pauseAfterRounds]
 *
 * Recovers whatever is in `dataDir`, picks every session up where its shard
 * left it, and keeps running the chaos scenario. It reports on stdout:
 *
 *   resumed <minTick> <maxTick>   after recovery
 *   acked <minTick>               after each round, once it is durable on every shard
 *   paused <positions json>       after `pauseAfterRounds` rounds, then it stops and waits
 *
 * It is not meant to finish: the drill kills it with SIGKILL first.
 */
import { ShardCluster } from './cluster.ts';
import { DRILL_COMPLETED_AT, drillSessions } from './crashDrill.ts';
import { ScenarioDriver } from './workload.ts';

const [dataDir, shards, sessions, pauseAfter] = process.argv.slice(2);
if (dataDir === undefined || shards === undefined || sessions === undefined) {
  console.error('usage: node src/cluster/crashDrillChild.ts <dataDir> <shards> <sessions> [pauseAfterRounds]');
  process.exit(2);
}

const ids = drillSessions(Number(sessions));
const cluster = await ShardCluster.start({
  dataDir,
  shards: Number(shards),
  secret: process.env.CHAOSIM_SECRET ?? 'sim-001-dev-secret',
  clock: 'manual',
});
const driver = new ScenarioDriver(cluster, ids, DRILL_COMPLETED_AT);
await driver.resync();

function positions(): number[] {
  return ids.map(({ id }) => driver.localTick(id));
}
process.stdout.write(`resumed ${Math.min(...positions())} ${Math.max(...positions())}\n`);

let rounds = 0;
let paused = false;
while (!paused && (await driver.round())) {
  rounds += 1;
  process.stdout.write(`acked ${Math.min(...positions())}\n`);
  if (rounds === Number(pauseAfter)) {
    const exact = Object.fromEntries(ids.map(({ id }) => [id, driver.localTick(id)]));
    process.stdout.write(`paused ${JSON.stringify(exact)}\n`);
    // Stop issuing rounds. The shard workers keep this process alive until the drill kills it.
    paused = true;
  }
}
if (!paused) await cluster.close();
