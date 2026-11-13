import { randomUUID } from 'node:crypto';
import type { StartedKafkaContainer } from '@testcontainers/kafka';
import { startDeployagramEnvironment, stopDeployagramEnvironment } from '@deployagram/environment';
import { DiagramLoggerClient } from '@deployagram/logger-client';
import type { StartedTestContainer } from 'testcontainers';
import {
  brokersOf,
  createTopic,
  networkBootstrapServersOf,
  startKafka,
  SUGGESTIONS_TOPIC,
  waitUntilBrokerReady,
} from './kafka.js';
import { networkSchemaRegistryUrlOf, schemaRegistryUrlOf, startSchemaRegistry } from './schema-registry.js';

/** The Docker IDs of what one run started, so the teardown can stop them. */
export interface TestStackIds {
  readonly kafka: string;
  readonly schemaRegistry: string;
  /** The Collector, only when this run started it. A Collector that was already running is left alone. */
  readonly collector?: string;
}

/** The addresses and the IDs for one test run. */
export interface TestStack {
  readonly brokers: string[];
  readonly schemaRegistryUrl: string;
  readonly collectorUrl: string;
  readonly ids: TestStackIds;
}

/**
 * Starts the Collector, then Kafka and the Schema Registry on the Deployagram network, and points the Collector at
 * them. Services that were already running are reused. If setup fails, what this run started is stopped.
 */
export async function startTestStack(): Promise<TestStack> {
  // Runtime mode lets the run point the Collector at its broker and registry.
  const deployagram = await startDeployagramEnvironment({
    proxy: false,
    loggerEnvironment: { KAFKA_BROKER_LOCATION_MODE: 'RUNTIME', SCHEMA_REGISTRY_LOCATION_MODE: 'RUNTIME' },
  });
  let kafka: StartedKafkaContainer | undefined;
  let schemaRegistry: StartedTestContainer | undefined;

  try {
    // Unique names per run, so a run never reaches another run's containers.
    const suffix = randomUUID().slice(0, 8);
    const kafkaAlias = `bookoverflow-kafka-${suffix}`;
    const registryAlias = `bookoverflow-schema-registry-${suffix}`;

    kafka = await startKafka(kafkaAlias);
    const brokers = brokersOf(kafka);
    await waitUntilBrokerReady(brokers);
    await createTopic(brokers, SUGGESTIONS_TOPIC);

    schemaRegistry = await startSchemaRegistry(registryAlias, kafkaAlias);
    const schemaRegistryUrl = schemaRegistryUrlOf(schemaRegistry);

    // The Collector reads each produced record from the broker, and decodes it with the registry.
    const logger = new DiagramLoggerClient({ baseUrl: deployagram.logger.url });
    await logger.configureKafkaBroker(networkBootstrapServersOf(kafkaAlias));
    await logger.configureSchemaRegistry(networkSchemaRegistryUrlOf(registryAlias));

    return {
      brokers,
      schemaRegistryUrl,
      collectorUrl: deployagram.logger.url,
      ids: {
        kafka: kafka.getId(),
        schemaRegistry: schemaRegistry.getId(),
        collector: deployagram.logger.source === 'started' ? deployagram.logger.containerId : undefined,
      },
    };
  } catch (error) {
    await schemaRegistry?.stop();
    await kafka?.stop();
    await stopDeployagramEnvironment();
    throw error;
  }
}

/** The broker addresses the run started. Throws if global setup has not provided it. */
export function brokersFromEnvironment(): string[] {
  return requiredEnv('TEST_KAFKA_BROKERS').split(',');
}

/** The Collector URL the run found or started. Throws if global setup has not provided it. */
export function collectorUrlFromEnvironment(): string {
  return requiredEnv('TEST_COLLECTOR_URL');
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
