import { SuggestionPublisher } from './suggestion-publisher.js';
import { createBookOverflowServer } from './book-overflow.js';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const schemaRegistryUrl = process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081';
const classicBooksUrl = process.env.CLASSIC_BOOKS_URL ?? 'http://localhost:9050';
const port = Number(process.env.PORT ?? 8081);

const publisher = new SuggestionPublisher({
  brokers,
  schemaRegistryUrl,
  topic: 'SuppliedSuggestionsTopic',
  schemaFile: new URL('../schemas/dont_forget_message.avsc', import.meta.url),
});
await publisher.connect();

const server = createBookOverflowServer({ classicBooksUrl, publisher });
server.listen(port, () => {
  console.info(`BookOverflow on port ${port}, asking ${classicBooksUrl}, publishing to ${brokers.join(',')}`);
});

const shutdown = (): void => {
  server.close();
  publisher.disconnect().finally(() => process.exit(0));
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
