# Solution for synchronous logging to ClickHouse

This is a documentation for technical solution implemented to fix issues related to logging of events to ClickHouse

## Problems

- Due to the lack of transactionality logs were missed in case of a web-worker failure before the business-logic step is executed
- Clickhouse network write errors was causing poor UX
- Clickhouse struggles with large numbers of small inserts

## Solution

### Addition of Transactional Outbox pattern

- New model `EventOutbox` was added to store events before publishing to ClickHouse

### Processing mechanism

- Events are first saved in the event_outbox within a transaction along with the main business logic
- A separate Celery worker periodically takes unprocessed events from the event_outbox, aggregates them and writes them to ClickHouse in batches by 100 items each
- Batching reduces the number of inserts to ClickHouse, which works better, than multiple single inserts, increases performance and stability
- Outbox task is decoupled from core log-processing logic

### Packet processing

- Celery Beat is used for periodic task, which sends data to ClickHouse

### Errors handling

- Repeated attempts to send events via Celery (using the `retry` mechanism) in case of errors
- Logging of errors using `structlog`

### Unit tests

- Removed the `test_event_log_entry_published` test due to change of `EventLogClient.insert` behavior
- Implemented the `test_event_inserted_to_outbox` test
- Implemented the `test_event_outbox_processed` test

## Changes to `EventLogClient.insert` behavior

Now `EventLogClient.insert` is not calling the `EventLogClient._convert_data` function to convert the data since it's now supposed to be called by `process_outbox_events` task, which queries `EventOutbox` model with data already converted

## Other small fixes

In `core.settings` at line 72 parameter `CLICKHOUSE_PORT` was using `CLICKHOUSE_HOST` environment variable, instead of `CLICKHOUSE_PORT`

## Installation

Put a `.env` file into the `src/core` directory. You can start with a template file:

```
cp src/core/.env.ci src/core/.env
```

Run the containers with
```
make run
```

and then run the installation script with:

```
make install
```

## Tests

`make test`

## Linter

`make lint`
