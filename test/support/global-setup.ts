import { startTestStack, type TestStackIds } from './stack.js';

declare global {
  var testStackIds: TestStackIds | undefined;
}

/**
 * Jest runs this once, before any test file. It starts the Collector, Kafka and the Schema Registry for the whole run, and puts the broker and registry
 * addresses in the environment, where the test files read them. global-teardown stops what this run started.
 */
export default async function globalSetup(): Promise<void> {
  const stack = await startTestStack();
  globalThis.testStackIds = stack.ids;
  process.env.TEST_KAFKA_BROKERS = stack.brokers.join(',');
  process.env.TEST_SCHEMA_REGISTRY_URL = stack.schemaRegistryUrl;
  process.env.TEST_COLLECTOR_URL = stack.collectorUrl;
}
