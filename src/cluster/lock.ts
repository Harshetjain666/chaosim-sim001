import { open, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { ClusterError } from './errors.ts';

const LOCK_FILE = 'LOCK';

/**
 * Claim a data directory for this process.
 *
 * Two processes writing one directory, say an old deploy still running when
 * the new one starts, would each replace the other's log segments and keep
 * acknowledging writes that no longer exist on disk. That is the one way to
 * lose acknowledged data that recovery cannot see, so a cluster takes an
 * exclusive lock file holding its pid before it touches anything else.
 *
 * A lock whose owner is no longer running (it crashed, or was killed with
 * SIGKILL and never cleaned up) is taken over. One whose owner is alive is
 * refused, including this same process. If the pid has since been reused by
 * an unrelated process the start is refused too, which errs on the safe side.
 *
 * Resolves with the function that releases the lock.
 */
export async function lockDataDir(dir: string): Promise<() => Promise<void>> {
  const path = join(dir, LOCK_FILE);
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const handle = await open(path, 'wx');
      try {
        await handle.writeFile(`${process.pid}\n`);
      } finally {
        await handle.close();
      }
      return () => rm(path, { force: true });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
      const owner = await lockOwner(path);
      if (owner !== null && isRunning(owner)) {
        throw new ClusterError('DATA_DIR_LOCKED', `${dir} is in use by process ${owner}`);
      }
      await rm(path, { force: true });
    }
  }
  throw new ClusterError('DATA_DIR_LOCKED', `${dir} is being claimed by another process`);
}

async function lockOwner(path: string): Promise<number | null> {
  const text = await readFile(path, 'utf8').catch(() => '');
  const pid = Number(text.trim());
  return Number.isInteger(pid) && pid > 0 ? pid : null;
}

function isRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM: it exists but belongs to someone else, so it is running.
    return (error as NodeJS.ErrnoException).code === 'EPERM';
  }
}
