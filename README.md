# Node.js BookOverflow demo (Jest, Nock)

The Node.js version of the Java BookOverflow app in `deployagram-quarkus-bookoverflow-avro-demo`. It listens for
HTTP requests for a book recommendation, asks ClassicBooks over HTTP for a book, publishes the suggestion to Kafka as
an Avro message, and responds to the original request with the book.

## The application

- `GET /BookOverflow/suggestion?email=…` (the HTTP server) returns a book, as JSON. It answers 400 if there is no
  email, and 404 for any other path.
- It calls `GET /ClassicBooks/random` (the HTTP client), with `Accept: application/json`. The client uses Node's
  `http` module, not `fetch`, because Nock 14 stubs `http` but not `fetch`.
- It publishes `{ email, suggestion }` to `SuppliedSuggestionsTopic` as an Avro message. The schema is
  `schemas/dont_forget_message.avsc`, the Java demo's schema, registered with the Confluent Schema Registry.

## Tests

```bash
npm install
npm test
```

Docker must be running. Jest's global setup starts Kafka and the Schema Registry once per run and passes their
addresses to the tests through the environment. Global teardown launches a separate Node process that uses
Testcontainers to remove the two containers and their network by Docker ID. The tests are in two files:

- `test/book-overflow.test.ts` calls the application over HTTP, stubs ClassicBooks with Nock, and checks the response
  and the Avro message on the topic.
- `test/book-overflow-validation.test.ts` sends a request with no email. It checks for a 400, that ClassicBooks is
  not called, and that nothing is written to the topic.

## Decisions

- **The plain topic name.** BookOverflow publishes to `SuppliedSuggestionsTopic`, as the Java BookOverflow demo does.
  The Don't Forget demo uses the org-qualified name, because it consumes a topic published by another organisation.

- **Kafka image.** `apache/kafka-native` does not work with testcontainers' Kafka module, which starts the broker
  with `/etc/confluent/docker/run`. That script is only in Confluent's images, so the tests use
  `confluentinc/cp-kafka:7.6.1` in KRaft mode.
