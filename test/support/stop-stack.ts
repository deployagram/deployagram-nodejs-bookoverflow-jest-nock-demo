import { getContainerRuntimeClient } from 'testcontainers';
import type { TestStackIds } from './stack.js';

/** Stops and removes what the run started. Runs in a child process, from global-teardown.ts. */
async function stopStack(ids: TestStackIds): Promise<void> {
  const client = await getContainerRuntimeClient();
  const containerIds = [ids.schemaRegistry, ids.kafka, ids.collector].flatMap((id) => (id ? [id] : []));
  const results = await Promise.allSettled(
    containerIds.map(async (id) => {
      const container = client.container.getById(id);
      await client.container.stop(container, { timeout: 0 });
      await client.container.remove(container, { removeVolumes: true });
    }),
  );
  const errors = results.flatMap((result) => (result.status === 'rejected' ? [result.reason] : []));
  if (errors.length) throw new AggregateError(errors, 'Failed to clean up the BookOverflow test stack');
}

const serializedIds = process.argv[2];
if (!serializedIds) throw new Error('Expected the BookOverflow test stack IDs');
const ids: TestStackIds = JSON.parse(serializedIds);
void stopStack(ids).catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
