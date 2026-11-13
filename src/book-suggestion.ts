/** A book, as ClassicBooks returns it, and as BookOverflow returns it to its caller. */
export interface BookSuggestion {
  name: string;
  edition: string;
  format: string;
  authors: string[];
}

/** The message published to Kafka: the reader's email, and the book. The shape of the Avro schema. */
export interface SuggestionMessage {
  email: string;
  suggestion: BookSuggestion;
}

/** Builds the message for a suggestion. */
export function toMessage(email: string, book: BookSuggestion): SuggestionMessage {
  return {
    email,
    suggestion: {
      name: book.name,
      edition: book.edition,
      format: book.format,
      authors: book.authors,
    },
  };
}
