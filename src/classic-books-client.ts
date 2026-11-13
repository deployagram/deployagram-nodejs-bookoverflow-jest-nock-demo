import { get } from 'node:http';
import type { BookSuggestion } from './book-suggestion.js';

/** Thrown when ClassicBooks does not return a book. */
export class ClassicBooksError extends Error {
  readonly status: number;

  constructor(status: number) {
    super(`ClassicBooks responded with HTTP ${status}`);
    this.name = 'ClassicBooksError';
    this.status = status;
  }
}

/**
 * GET /ClassicBooks/random: asks ClassicBooks for a book, as JSON. Uses Node's http module, not fetch, so the call
 * can be stubbed with Nock under Jest's CommonJS mode.
 */
export async function fetchRandomBook(baseUrl: string): Promise<BookSuggestion> {
  const url = new URL(`${baseUrl.replace(/\/+$/, '')}/ClassicBooks/random`);
  const { status, body } = await httpGet(url, { Accept: 'application/json' });
  if (status < 200 || status >= 300) {
    throw new ClassicBooksError(status);
  }
  return JSON.parse(body) as BookSuggestion;
}

function httpGet(url: URL, headers: Record<string, string>): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const request = get(url, { headers }, (response) => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', (chunk: string) => {
        body += chunk;
      });
      response.on('end', () => resolve({ status: response.statusCode ?? 0, body }));
      response.on('error', reject);
    });
    request.on('error', reject);
  });
}
