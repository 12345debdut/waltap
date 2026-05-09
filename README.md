# pgcdc

A Change Data Capture pipeline and read/write split proxy for PostgreSQL, built in Go.

`pgcdc` connects to PostgreSQL's logical replication stream, captures row-level changes (INSERT, UPDATE, DELETE) in real time, and delivers them to pluggable sinks. It also includes `pgproxy`, a TCP proxy that routes reads to replicas and writes to the primary.

## How it works

PostgreSQL's Write-Ahead Log (WAL) records every change before it hits disk. Logical decoding (via the `pgoutput` plugin) translates those raw WAL records into structured row-level events. `pgcdc` subscribes to this stream using the streaming replication protocol and delivers each event to a configurable sink.

```
PostgreSQL WAL
     |
     | logical replication (pgoutput)
     v
  pgcdc
     |
     +---> stdout (NDJSON)
     +---> webhook (HTTP POST per event)
     +---> webhook-batch (HTTP POST per batch)
     +---> kafka (per-table topics)
     +---> stdout-batch (JSON batches)
```

The proxy component sits between your application and PostgreSQL:

```
App --> pgproxy(:5434) --> Primary(:5433)   writes, transactions
                       --> Replica(:5435)   standalone SELECTs
```

## Features

- **Real-time CDC** via PostgreSQL logical replication
- **5 sink types**: stdout, webhook, kafka, stdout-batch, webhook-batch
- **Filtering**: by table name, by action (INSERT/UPDATE/DELETE), or both
- **Snapshotting**: load existing rows before streaming changes
- **Batching**: configurable batch size and flush interval
- **Retry with exponential backoff**: configurable attempts, delays, and max delay cap
- **Dead-letter queue**: failed events written to a JSONL file, stream continues
- **Prometheus metrics**: event counts, delivery latency, replication lag, retry stats
- **Read/write split proxy**: routes SELECTs to replica, writes to primary, transaction-aware

## Prerequisites

- Go 1.22+
- Docker and Docker Compose
- PostgreSQL 14+ with `wal_level=logical` (provided via docker-compose)

## Quickstart

**1. Start the infrastructure:**

```bash
make docker-up
```

This starts:
- PostgreSQL primary on port **5433** (with logical replication enabled)
- PostgreSQL replica on port **5435** (streaming replication)
- Kafka on port **9092** (KRaft mode, no Zookeeper)

**2. Create a test table:**

```bash
docker exec pgwal-lab psql -U lab -d wallab -c "
  CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL
  );
  ALTER TABLE users REPLICA IDENTITY FULL;
"
```

**3. Build and run the CDC pipeline:**

```bash
make build
./pgcdc --sink stdout
```

**4. In another terminal, make changes:**

```bash
docker exec pgwal-lab psql -U lab -d wallab -c "
  INSERT INTO users(name, email) VALUES('alice', 'alice@example.com');
"
```

You'll see the event in the pgcdc output:

```json
{"action":"INSERT","schema":"public","table":"users","new":{"email":"alice@example.com","id":1,"name":"alice"},"timestamp":"...","lsn":"0/...","xid":123}
```

## Usage

### CDC Pipeline

```bash
# Stream all changes to stdout
./pgcdc --sink stdout

# Filter to specific tables and actions
./pgcdc --sink stdout --tables users,orders --actions INSERT,UPDATE

# Webhook delivery with retry
./pgcdc --sink webhook \
  --webhook-url http://localhost:9090/events \
  --retry-attempts 5 \
  --retry-base-delay 100ms

# Webhook with dead-letter queue
./pgcdc --sink webhook \
  --webhook-url http://localhost:9090/events \
  --dlq-path ./failed-events.jsonl

# Kafka sink (topics: pgcdc.public.users, pgcdc.public.orders, ...)
./pgcdc --sink kafka \
  --kafka-brokers localhost:9092 \
  --kafka-topic-prefix pgcdc

# Batched webhook delivery
./pgcdc --sink webhook-batch \
  --webhook-url http://localhost:9090/batch \
  --batch-size 100 \
  --batch-wait 500ms

# Snapshot existing rows, then stream changes
./pgcdc --sink stdout --tables users --snapshot

# Custom Postgres connection
./pgcdc --db "postgres://user:pass@host:5432/mydb" --sink stdout
```

### Read/Write Split Proxy

```bash
./pgproxy \
  --listen :5434 \
  --primary "postgres://lab:lab@localhost:5433/wallab" \
  --replica "postgres://lab:lab@localhost:5435/wallab"
```

Then connect your application to `localhost:5434`. SELECTs go to the replica, writes go to the primary. Transactions are pinned to the primary.

### Prometheus Metrics

Metrics are exposed at `http://localhost:2112/metrics` by default. Key metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `pgcdc_events_total` | counter | Events processed (labels: action, table, status) |
| `pgcdc_delivery_duration_seconds` | histogram | Sink delivery latency including retries |
| `pgcdc_replication_lag_seconds` | gauge | Delay between commit time and processing time |
| `pgcdc_retries_total` | counter | Retry attempts (labels: outcome) |
| `pgcdc_dlq_events_total` | counter | Events written to dead-letter queue |
| `pgcdc_batch_flush_total` | counter | Batch flushes (labels: trigger) |
| `pgcdc_batch_size` | histogram | Events per batch flush |
| `pgcdc_snapshot_rows_total` | counter | Rows emitted during snapshot |

## CLI Reference

### pgcdc

```
  -db string             Postgres connection string (default "postgres://lab:lab@localhost:5433/wallab")
  -slot string           Replication slot name (default "pgcdc_slot")
  -publication string    Publication name (default "pgcdc_pub")
  -sink string           Sink type: stdout, webhook, kafka, stdout-batch, webhook-batch (default "stdout")
  -tables string         Comma-separated table names to capture (empty = all)
  -actions string        Comma-separated actions: INSERT,UPDATE,DELETE (empty = all)
  -snapshot              Load existing rows before streaming changes
  -webhook-url string    Webhook URL (required for webhook sinks)
  -kafka-brokers string  Kafka broker addresses (default "localhost:9092")
  -kafka-topic-prefix    Kafka topic name prefix (default "pgcdc")
  -batch-size int        Max events per batch (default 100)
  -batch-wait duration   Max wait before flushing a partial batch (default 500ms)
  -retry-attempts int    Max delivery attempts (default 5)
  -retry-base-delay      Initial retry delay, doubles each attempt (default 100ms)
  -retry-max-delay       Maximum retry delay cap (default 30s)
  -dlq-path string       Path to dead-letter queue file (empty = disabled)
  -metrics-addr string   Prometheus metrics endpoint (default ":2112")
```

### pgproxy

```
  -listen string    Address to listen on (default ":5434")
  -primary string   Primary Postgres DSN (default "postgres://lab:lab@localhost:5433/wallab")
  -replica string   Replica Postgres DSN (default "postgres://lab:lab@localhost:5435/wallab")
```

## Project Structure

```
cmd/
  pgcdc/                 CDC pipeline CLI
  pgproxy/               Read/write split proxy CLI
  webhook-test-server/   Test HTTP server for webhook sink
  flaky-server/          Intentionally-failing server for retry testing
internal/
  cdc/
    stream.go            WAL streaming engine (replication protocol, pgoutput decoding)
    filter.go            Table and action filtering
    snapshot.go           Initial data load via REPEATABLE READ
    event.go             ChangeEvent data model
  sink/
    sink.go              Sink interface
    stdout.go            NDJSON to stdout
    webhook.go           HTTP POST per event
    webhook_batch.go     HTTP POST per batch
    kafka.go             Kafka producer (franz-go, per-table topics)
    stdout_batch.go      Batched JSON to stdout
    batch.go             Batching decorator (size + time triggers)
    retry.go             Retry decorator (exponential backoff)
    dlq.go               Dead-letter queue (JSONL file)
    metrics.go           Prometheus metrics decorator
  proxy/
    proxy.go             TCP proxy with query routing
    classifier.go        SQL classification (read vs write vs transaction)
  metrics/
    metrics.go           Prometheus metric definitions
docker-compose.yml       Postgres primary + replica + Kafka
Makefile                 Build, test, run targets
```

## Architecture

The sink system uses the decorator pattern. Sinks compose inside-out:

```
MetricsSink -> RetrySink (onFailure: -> DLQ) -> WebhookSink
```

- **MetricsSink** (outermost): records delivery latency including retries
- **RetrySink**: exponential backoff, configurable failure callback
- **DLQSink**: writes failed events to file, returns nil so the stream continues
- **Inner sink**: the actual destination (stdout, webhook, kafka)

The proxy uses pgx connection pools for backends and handles the PostgreSQL wire protocol directly for client connections. It classifies queries by inspecting the first SQL keyword and routes accordingly.

## Development

```bash
make build        # Build pgcdc and pgproxy binaries
make test         # Run all tests with race detector
make test-cover   # Run tests with coverage report
make vet          # Run go vet
make docker-up    # Start Postgres + Kafka
make docker-down  # Stop everything
make clean        # Remove built binaries
```

## License

MIT
