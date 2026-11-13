import { randomUUID } from 'node:crypto';
import type { StartedKafkaContainer } from '@testcontainers/kafka';
import { Network, type StartedNetwork, type StartedTestContainer } from 'testcontainers';
import { brokersOf, createTopic, startKafka, SUGGESTIONS_TOPIC, waitUntilBrokerReady } from './kafka.js';
import { schemaRegistryUrlOf, startSchemaRegistry } from './schema-registry.js';

export interface TestStackIds {
  readonly kafka: string;
  readonly schemaRegistry: string;
  readonly network: string;
}

/** The addresses and Docker resource IDs for one test run. */
export interface TestStack {
  readonly brokers: string[];
  readonly schemaRegistryUrl: string;
  readonly ids: TestStackIds;
}

/** Starts Kafka and the Schema Registry, on a network of their own, and creates the suggestions topic. */
export async function startTestStack(): Promise<TestStack> {
  let network: StartedNetwork | undefined;
  let kafka: StartedKafkaContainer | undefined;
  let schemaRegistry: StartedTestContainer | undefined;
  const stop = async () => {
    await schemaRegistry?.stop();
    await kafka?.stop();
    await network?.stop();
  };

  try {
    // Unique names per run, so a run never reaches another run's containers.
    const suffix = randomUUID().slice(0, 8);
    const kafkaAlias = `bookoverflow-kafka-${suffix}`;
    const registryAlias = `bookoverflow-schema-registry-${suffix}`;

    network = await new Network().start();
    kafka = await startKafka(network, kafkaAlias);
    const brokers = brokersOf(kafka);
    await waitUntilBrokerReady(brokers);
    await createTopic(brokers, SUGGESTIONS_TOPIC);

    schemaRegistry = await startSchemaRegistry(network, registryAlias, kafkaAlias);
    return {
      brokers,
      schemaRegistryUrl: schemaRegistryUrlOf(schemaRegistry),
      ids: { kafka: kafka.getId(), schemaRegistry: schemaRegistry.getId(), network: network.getId() },
    };
  } catch (error) {
    await stop();
    throw error;
  }
}

/** The broker addresses the run started. Throws if global setup has not provided it. */
export function brokersFromEnvironment(): string[] {
  return requiredEnv('TEST_KAFKA_BROKERS').split(',');
}

/** The Schema Registry address the run started. Throws if global setup has not provided it. */
export function schemaRegistryUrlFromEnvironment(): string {
  return requiredEnv('TEST_SCHEMA_REGISTRY_URL');
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is not set: run the tests with npm test, which starts the containers first.`);
  }
  return value;
}
