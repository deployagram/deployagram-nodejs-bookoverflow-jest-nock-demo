import { execFile } from 'node:child_process';
import { resolve } from 'node:path';
import { promisify } from 'node:util';

const run = promisify(execFile);

/**
 * Jest runs this once, after the last test file. The containers are stopped by a child process, which has a fresh
 * Docker client. See stop-stack.ts.
 */
export default async function globalTeardown(_globalConfig: unknown, projectConfig: { rootDir: string }): Promise<void> {
  const ids = globalThis.testStackIds;
  if (!ids) return;

  // Give Testcontainers a fresh Docker client outside the Jest hook loader.
  await run(
    process.execPath,
    ['--require', require.resolve('tsx/cjs'), resolve(projectConfig.rootDir, 'test/support/stop-stack.ts'), JSON.stringify(ids)],
    { cwd: projectConfig.rootDir },
  );

  globalThis.testStackIds = undefined;
}
