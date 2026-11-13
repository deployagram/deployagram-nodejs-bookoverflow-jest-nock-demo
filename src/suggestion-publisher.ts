import { readFileSync } from 'node:fs';
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';
import { Kafka, logLevel, Partitioners, type Producer } from 'kafkajs';
import type { SuggestionMessage } from './book-suggestion.js';

export interface SuggestionPublisherOptions {
  brokers: string[];
  schemaRegistryUrl: string;
  topic: string;
  /** The Avro schema file, for the value. */
  schemaFile: URL;
  /** Wraps the producer, for example to add behaviour around each acknowledged record. */
  wrapProducer?: (producer: Producer) => Producer;
}

/**
 * Publishes suggestions to Kafka as Avro messages. The schema is registered under the topic's value subject, and
 * each message is encoded with the schema's id.
 */
export class SuggestionPublisher {
  private readonly registry: SchemaRegistry;
  private producer: Producer | undefined;
  private schemaId: number | undefined;

  constructor(private readonly options: SuggestionPublisherOptions) {
    this.registry = new SchemaRegistry({ host: options.schemaRegistryUrl });
  }

  async connect(): Promise<void> {
    const schema = readFileSync(this.options.schemaFile, 'utf8');
    const { id } = await this.registry.register(
      { type: SchemaType.AVRO, schema },
      { subject: `${this.options.topic}-value` },
    );
    this.schemaId = id;

    const kafka = new Kafka({ clientId: 'bookoverflow', brokers: this.options.brokers, logLevel: logLevel.WARN });
    const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
    await producer.connect();
    this.producer = this.options.wrapProducer ? this.options.wrapProducer(producer) : producer;
  }

  async publish(message: SuggestionMessage): Promise<void> {
    if (!this.producer || this.schemaId === undefined) {
      throw new Error('The publisher is not connected.');
    }
    const value = await this.registry.encode(this.schemaId, message);
    await this.producer.send({ topic: this.options.topic, messages: [{ value }] });
  }

  async disconnect(): Promise<void> {
    await this.producer?.disconnect();
  }
}
