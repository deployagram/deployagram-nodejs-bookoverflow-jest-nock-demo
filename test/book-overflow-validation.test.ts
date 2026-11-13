import { randomUUID } from 'node:crypto';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import nock from 'nock';
import { afterAll, afterEach, beforeAll, describe, expect, it } from '@jest/globals';
import { createBookOverflowServer } from '../src/book-overflow.js';
import { SuggestionPublisher } from '../src/suggestion-publisher.js';
import { createTopic, expectNoValue } from './support/kafka.js';
import { brokersFromEnvironment, schemaRegistryUrlFromEnvironment } from './support/stack.js';

/** ClassicBooks, as BookOverflow calls it. Nock stubs this address in-process. */
const CLASSIC_BOOKS_URL = 'http://localhost:9050';
const SCHEMA_FILE = new URL('../schemas/dont_forget_message.avsc', import.meta.url);

/** The stub for ClassicBooks. Its scope tells whether BookOverflow called it. */
type ClassicBooksStub = nock.Scope;

describe('BookOverflowResourceTest', () => {
  let brokers: string[];
  let publisher: SuggestionPublisher;
  let server: Server;
  let appUrl: string;
  let topic: string;

  beforeAll(async () => {
    brokers = brokersFromEnvironment();
    const schemaRegistryUrl = schemaRegistryUrlFromEnvironment();

    // A topic of its own, so the test sees only what this test's requests write to it.
    topic = `SuppliedSuggestionsTopic-${randomUUID().slice(0, 8)}`;
    await createTopic(brokers, topic);

    publisher = new SuggestionPublisher({ brokers, schemaRegistryUrl, topic, schemaFile: SCHEMA_FILE });
    await publisher.connect();

    server = createBookOverflowServer({ classicBooksUrl: CLASSIC_BOOKS_URL, publisher });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    appUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  }, 180_000);

  afterEach(() => {
    nock.cleanAll();
  });

  afterAll(async () => {
    server?.close();
    await publisher?.disconnect();
  });

  it('rejectsARequestWithoutAnEmail', async () => {
    const classicBooks = givenClassicBooksHasABook();

    const response = await whenAReaderAsksWithoutAnEmail();

    thenTheRequestIsRejected(response);
    thenClassicBooksIsNotCalled(classicBooks);
    await thenNothingIsPublished();
  });

  // Given: ClassicBooks has a book, so a call to it would succeed. The stub must not be called.
  function givenClassicBooksHasABook(): ClassicBooksStub {
    return nock(CLASSIC_BOOKS_URL).get('/ClassicBooks/random').reply(200, { name: 'Refactoring' });
  }

  // When: a reader asks BookOverflow over HTTP, with no email on the request.
  async function whenAReaderAsksWithoutAnEmail(): Promise<{ status: number; body: string }> {
    const response = await fetch(`${appUrl}/BookOverflow/suggestion`);
    return { status: response.status, body: await response.text() };
  }

  // Then: BookOverflow answers 400, and says the email is required.
  function thenTheRequestIsRejected(response: { status: number; body: string }): void {
    expect(response.status).toBe(400);
    expect(JSON.parse(response.body)).toEqual({ message: 'The email parameter is required' });
  }

  // Then: BookOverflow does not ask ClassicBooks for a book.
  function thenClassicBooksIsNotCalled(classicBooks: ClassicBooksStub): void {
    expect(classicBooks.isDone()).toBe(false);
  }

  // Then: nothing is written to the topic.
  async function thenNothingIsPublished(): Promise<void> {
    await expectNoValue(brokers, topic);
  }
});
