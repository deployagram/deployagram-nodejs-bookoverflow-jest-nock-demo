import { createServer, type Server, type ServerResponse } from 'node:http';
import { fetchRandomBook } from './classic-books-client.js';
import { toMessage } from './book-suggestion.js';
import type { SuggestionMessage } from './book-suggestion.js';

/** The part of the publisher the server uses. */
export interface SuggestionSink {
  publish(message: SuggestionMessage): Promise<void>;
}

export interface BookOverflowOptions {
  /** Where ClassicBooks is, for example "http://classicbooks:8080". */
  classicBooksUrl: string;
  publisher: SuggestionSink;
}

/**
 * The BookOverflow HTTP server. GET /BookOverflow/suggestion?email=... asks ClassicBooks for a book, publishes the
 * suggestion to Kafka for the reader, and responds with the book. This matches the Java BookOverflow demo.
 */
export function createBookOverflowServer(options: BookOverflowOptions): Server {
  return createServer(async (request, response) => {
    const url = new URL(request.url ?? '/', 'http://bookoverflow');
    if (request.method !== 'GET' || url.pathname !== '/BookOverflow/suggestion') {
      return sendJson(response, 404, { message: 'Not found' });
    }
    const email = url.searchParams.get('email');
    if (!email) {
      return sendJson(response, 400, { message: 'The email parameter is required' });
    }
    try {
      const book = await fetchRandomBook(options.classicBooksUrl);
      await options.publisher.publish(toMessage(email, book));
      return sendJson(response, 200, book);
    } catch (error) {
      return sendJson(response, 500, { message: error instanceof Error ? error.message : String(error) });
    }
  });
}

function sendJson(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { 'Content-Type': 'application/json' });
  response.end(JSON.stringify(body));
}
