import { startTestStack, type TestStackIds } from './stack.js';

declare global {
  var testStackIds: TestStackIds | undefined;
}

/**
 * Jest runs this once, before any test file. It starts Kafka and the Schema Registry for the whole run, and puts their
 * addresses in the environment, where the test files read them. global-teardown stops them.
 */
export default async function globalSetup(): Promise<void> {
  const stack = await startTestStack();
  globalThis.testStackIds = stack.ids;
  process.env.TEST_KAFKA_BROKERS = stack.brokers.join(',');
  process.env.TEST_SCHEMA_REGISTRY_URL = stack.schemaRegistryUrl;
}
