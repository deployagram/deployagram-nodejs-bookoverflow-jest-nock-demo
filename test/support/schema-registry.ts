import { GenericContainer, Wait, type StartedNetwork, type StartedTestContainer } from 'testcontainers';
import { networkBootstrapServersOf } from './kafka.js';

/** The Confluent Schema Registry image, matching the Kafka image's version. */
export const SCHEMA_REGISTRY_IMAGE = 'confluentinc/cp-schema-registry:7.6.1';

const SCHEMA_REGISTRY_PORT = 8081;

/**
 * Starts a Schema Registry on the network, reachable there as `alias`, and talking to the broker that is reachable
 * there as `kafkaAlias`.
 */
export async function startSchemaRegistry(
  network: StartedNetwork,
  alias: string,
  kafkaAlias: string,
): Promise<StartedTestContainer> {
  return new GenericContainer(SCHEMA_REGISTRY_IMAGE)
    .withNetwork(network)
    .withNetworkAliases(alias)
    .withExposedPorts(SCHEMA_REGISTRY_PORT)
    .withEnvironment({
      SCHEMA_REGISTRY_HOST_NAME: alias,
      SCHEMA_REGISTRY_LISTENERS: `http://0.0.0.0:${SCHEMA_REGISTRY_PORT}`,
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: networkBootstrapServersOf(kafkaAlias),
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: '1',
    })
    .withWaitStrategy(Wait.forHttp('/subjects', SCHEMA_REGISTRY_PORT).forStatusCode(200))
    .withStartupTimeout(60_000)
    .start();
}

/** The Schema Registry's URL as seen from the test process. */
export function schemaRegistryUrlOf(container: StartedTestContainer): string {
  return `http://${container.getHost()}:${container.getMappedPort(SCHEMA_REGISTRY_PORT)}`;
}
