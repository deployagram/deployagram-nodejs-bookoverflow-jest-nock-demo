import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { recordProducedMessages } from '@deployagram/kafkajs';
import { DiagramLoggerClient } from '@deployagram/logger-client';
import { recordNock, type NockRecorder } from '@deployagram/nock';
import nock from 'nock';
import { afterAll, afterEach, beforeAll, describe, expect, it } from '@jest/globals';
import { createBookOverflowServer } from '../src/book-overflow.js';
import { SuggestionPublisher } from '../src/suggestion-publisher.js';
import { readFirstValue, SUGGESTIONS_TOPIC } from './support/kafka.js';
import { brokersFromEnvironment, schemaRegistryUrlFromEnvironment } from './support/stack.js';
import { callRecordingInbound } from './support/inbound-call.js';
import { useDeployagram } from './support/deployagram.js';

/** ClassicBooks, as BookOverflow calls it. Nock stubs this address in-process. */
const CLASSIC_BOOKS_URL = 'http://localhost:9050';
const SCHEMA_FILE = new URL('../schemas/dont_forget_message.avsc', import.meta.url);
const EMAIL = 'test@deployagram.com';
const BOOK = {
  name: 'Refactoring',
  edition: '1st',
  format: 'paperback',
  authors: ['Fowler Martin', 'Beck Kent'],
};

describe('BookOverflowResourceTest', () => {
  let registry: SchemaRegistry;
  let brokers: string[];
  let logger: DiagramLoggerClient;
  let publisher: SuggestionPublisher;
  let server: Server;
  let appUrl: string;
  let recorder: NockRecorder | undefined;

  // Hooks that must run before the Deployagram adapter's: the recorder is flushed before the test's end is
  // marked, and the servers stop before the environment does.
  afterEach(async () => {
    await recorder?.flush();
    recorder?.stop();
    nock.cleanAll();
  });
  afterAll(async () => {
    server?.close();
    await publisher?.disconnect();
  });

  // Uses the Collector that the run started. There is no Proxy: ClassicBooks is stubbed in-process with Nock, and the
  // test records its own inbound call.
  const deployagram = useDeployagram({
    name: 'BookOverflowResourceTest',
  });

  beforeAll(async () => {
    brokers = brokersFromEnvironment();
    const schemaRegistryUrl = schemaRegistryUrlFromEnvironment();
    registry = new SchemaRegistry({ host: schemaRegistryUrl });

    logger = new DiagramLoggerClient({ baseUrl: deployagram.collectorUrl });

    // Each acknowledged record is reported to the Collector, as the Java producer interceptor reports it.
    publisher = new SuggestionPublisher({
      brokers,
      schemaRegistryUrl,
      topic: SUGGESTIONS_TOPIC,
      schemaFile: SCHEMA_FILE,
      wrapProducer: (producer) => recordProducedMessages(producer, { logger, producingApp: 'BookOverflow' }),
    });
    await publisher.connect();

    server = createBookOverflowServer({ classicBooksUrl: CLASSIC_BOOKS_URL, publisher });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    appUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  }, 180_000);

  it('callCForBook', async () => {
    givenClassicBooksHasABook();

    const response = await whenAReaderAsksForABook();

    thenTheReaderGetsTheBook(response);
    await thenTheSuggestionIsPublished();
  });

  // Given: ClassicBooks has a book. The application's call to it must ask for JSON, and is recorded to the Collector.
  function givenClassicBooksHasABook(): void {
    const scope = nock(CLASSIC_BOOKS_URL)
      .get('/ClassicBooks/random')
      .matchHeader('accept', 'application/json')
      .reply(200, BOOK, { 'Content-Type': 'application/json' });
    recorder = recordNock(scope, { logger, caller: 'BookOverflow', callee: 'ClassicBooks' });
  }

  // When: AppA calls BookOverflow over HTTP for a suggestion for the reader's email.
  async function whenAReaderAsksForABook(): Promise<{ status: number; body: string }> {
    const url = new URL(`${appUrl}/BookOverflow/suggestion`);
    url.searchParams.set('email', EMAIL);
    return callRecordingInbound(logger, { caller: 'AppA', callee: 'BookOverflow' }, url);
  }

  // Then: AppA gets the book back, as JSON.
  function thenTheReaderGetsTheBook(response: { status: number; body: string }): void {
    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)).toEqual(BOOK);
  }

  // Then: the suggestion for the reader is on the topic, as an Avro message.
  async function thenTheSuggestionIsPublished(): Promise<void> {
    const decoded = await registry.decode(await readFirstValue(brokers, SUGGESTIONS_TOPIC));
    expect(decoded).toEqual({ email: EMAIL, suggestion: BOOK });
  }
});
