import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import type { StartedNetwork } from 'testcontainers';
import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';

/**
 * The Kafka image. apache/kafka-native does not work with the testcontainers Kafka module, which
 * starts the broker with /etc/confluent/docker/run, a script only Confluent's images provide.
 */
export const KAFKA_IMAGE = 'confluentinc/cp-kafka:7.6.1';
// BookOverflow publishes to the plain topic, as the Java BookOverflow demo does.
export const SUGGESTIONS_TOPIC = 'SuppliedSuggestionsTopic';

// The port testcontainers' Kafka image listens on inside the container.
const KAFKA_INTERNAL_PORT = 9093;
// The broker's listener for other containers on the same network. The module advertises it under the network alias.
const KAFKA_NETWORK_PORT = 9092;

/** Starts a broker on the network, reachable there as `alias`. Other containers on the network can reach it by that name. */
export async function startKafka(network: StartedNetwork, alias: string): Promise<StartedKafkaContainer> {
  return new KafkaContainer(KAFKA_IMAGE).withKraft().withNetwork(network).withNetworkAliases(alias).start();
}

/** The broker's address on the network, as other containers on it use it. */
export function networkBootstrapServersOf(alias: string): string {
  return `PLAINTEXT://${alias}:${KAFKA_NETWORK_PORT}`;
}

/** Bootstrap servers for a started Kafka container, as seen from the test process. */
export function brokersOf(container: StartedKafkaContainer): string[] {
  return [`${container.getHost()}:${container.getMappedPort(KAFKA_INTERNAL_PORT)}`];
}

export async function createTopic(brokers: string[], topic: string): Promise<void> {
  const admin = new Kafka({ clientId: 'test-admin', brokers }).admin();
  await admin.connect();
  try {
    await admin.createTopics({ topics: [{ topic }], waitForLeaders: true });
  } finally {
    await admin.disconnect();
  }
}

/** Sends one message, with the given bytes as its value, to the topic. */
export async function produce(brokers: string[], topic: string, value: Buffer): Promise<void> {
  const producer = new Kafka({ clientId: 'test-producer', brokers }).producer();
  await producer.connect();
  try {
    await producer.send({ topic, messages: [{ value }] });
  } finally {
    await producer.disconnect();
  }
}

/**
 * Reads the first value on the topic, from the beginning, and returns its bytes. Waits up to the timeout for a
 * message to arrive.
 */
export async function readFirstValue(brokers: string[], topic: string, timeoutMs = 10_000): Promise<Buffer> {
  const consumer = new Kafka({ clientId: 'test-reader', brokers, logLevel: logLevel.NOTHING }).consumer({
    groupId: `bookoverflow-test-${randomUUID()}`,
  });
  await consumer.connect();
  try {
    await consumer.subscribe({ topic, fromBeginning: true });
    return await new Promise<Buffer>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`No message on ${topic} within ${timeoutMs}ms`)), timeoutMs);
      void consumer.run({
        eachMessage: async ({ message }) => {
          clearTimeout(timer);
          resolve(message.value ?? Buffer.alloc(0));
        },
      });
    });
  } finally {
    await consumer.disconnect();
  }
}

/**
 * Passes if no message arrives on the topic within the wait. Fails if one does. Reads from the beginning, so it sees
 * every message on the topic.
 */
export async function expectNoValue(brokers: string[], topic: string, waitMs = 5_000): Promise<void> {
  try {
    await readFirstValue(brokers, topic, waitMs);
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('No message on ')) return;
    throw error;
  }
  throw new Error(`A message was written to ${topic}`);
}

/**
 * Waits until the broker answers requests, not just until its port is open. The Kafka module waits only for the
 * port, which can accept connections before the broker is ready. Retries until a cluster description succeeds.
 */
export async function waitUntilBrokerReady(brokers: string[], timeoutMs = 60_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  const admin = new Kafka({ clientId: 'test-readiness', brokers, logLevel: logLevel.NOTHING }).admin();
  for (;;) {
    try {
      await admin.connect();
      await admin.describeCluster();
      await admin.disconnect();
      return;
    } catch (error) {
      await admin.disconnect().catch(() => undefined);
      if (Date.now() >= deadline) throw error;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
}
