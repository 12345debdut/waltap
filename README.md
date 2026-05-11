# waltap

A Change Data Capture pipeline for PostgreSQL that taps into the Write-Ahead Log via logical replication and delivers row-level changes to pluggable sinks (stdout, webhook, Kafka). Includes a read/write split TCP proxy.

## How It Works

PostgreSQL's Write-Ahead Log (WAL) records every change before it hits disk. Logical decoding (via the `pgoutput` plugin) translates those raw WAL records into structured row-level events. waltap subscribes to this stream using the streaming replication protocol and delivers each event to a configurable sink.

<p align="center">
  <img src="docs/assets/waltap-architecture.svg" alt="waltap architecture — animated data flow from PostgreSQL WAL through Stream/Filter/Sink Chain to stdout, webhook, kafka and DLQ sinks" width="100%"/>
</p>

<sub>↑ <em>The diagram is animated — change events flow from PostgreSQL through the pipeline and fan out to your sinks. Animations render directly on GitHub.</em></sub>

The proxy component sits between your application and PostgreSQL — parsing the wire protocol, classifying each query, and routing reads to the replica while writes and transactions stay on the primary:

<p align="center">
  <img src="docs/assets/waltap-proxy.svg" alt="waltap pgproxy — animated query routing showing SELECT going to replica and INSERT/UPDATE/BEGIN going to primary" width="100%"/>
</p>

<sub>↑ <em>The proxy classifies each SQL statement by its first keyword. Reads go to the replica, writes and transactions are pinned to the primary.</em></sub>

## Features

- **Real-time CDC** via PostgreSQL logical replication
- **5 sink types** — stdout, webhook, kafka, stdout-batch, webhook-batch
- **Filtering** — by table name, by action (INSERT/UPDATE/DELETE), or both
- **Snapshotting** — load existing rows before streaming changes
- **Batching** — configurable batch size and flush interval
- **Retry with exponential backoff** — configurable attempts, delays, and max delay cap
- **Dead-letter queue** — failed events written to a JSONL file, stream continues
- **Prometheus metrics** — event counts, delivery latency, replication lag, retry stats
- **Read/write split proxy** — routes SELECTs to replica, writes to primary, transaction-aware

## Prerequisites

- Go 1.25+
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

You'll see the event appear in the pgcdc terminal:

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

Connect your application to `localhost:5434`. SELECTs go to the replica, writes go to the primary. Transactions are pinned to the primary.

### Prometheus Metrics

Metrics are exposed at `http://localhost:2112/metrics` by default.

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

## Architecture

The sink system uses the decorator pattern. Sinks compose inside-out:

```
MetricsSink -> RetrySink (onFailure: -> DLQ) -> WebhookSink
```

- **MetricsSink** (outermost) — records delivery latency including retries
- **RetrySink** — exponential backoff, configurable failure callback
- **DLQSink** — writes failed events to file, returns nil so the stream continues
- **Inner sink** — the actual destination (stdout, webhook, kafka)

The proxy uses pgx connection pools for backends and handles the PostgreSQL wire protocol directly for client connections. It classifies queries by inspecting the first SQL keyword and routes accordingly.

## Learning

A 16-slide walkthrough of the entire project — what WAL is, how logical replication works, the architecture, sink chain, delivery guarantees, batching, the proxy, the wire protocol, query routing, and observability. Each slide is shown for 2 seconds and the loop repeats every 32 seconds. No video, no GIF — pure animated SVG that renders directly here on GitHub.

<p align="center">
  <img src="docs/assets/waltap-deck-preview.svg" alt="waltap auto-advancing deck preview — 16 slides covering WAL fundamentals, the CDC architecture, sink chain, delivery guarantees, batching, the read/write split proxy, the Postgres wire protocol, query routing, observability and a closing reveal of the GitHub repo. Each slide displays for 2 seconds in a continuous loop." width="100%"/>
</p>

<sub>↑ <em>The deck auto-advances through all 16 slides. If the animation pauses, refresh the page — GitHub sometimes caches the first frame.</em></sub>

Want the **fully interactive version**? The deck above is a preview baked into SVG. The full HTML deck at [`waltap-deck.html`](waltap-deck.html) has:

- A clickable **architecture diagram** with live event-flow animation (▶ stream events button) and per-component detail panels
- A **sink chain builder** — toggle `MetricsSink`, `RetrySink`, `BatchSink` on or off, swap destinations, and fire a particle through the chain you composed
- A **live SQL classifier** — type any SQL and watch it route in real time to the primary or replica, with the matching classifier rule flashing green
- Animated **Prometheus metric counters** that count up when the slide enters view
- **Inline edit mode** — press `E`, click any text to edit, `Ctrl+S` to export the customized deck

Clone the repo and open `waltap-deck.html` in a browser, or browse through the [docs/](docs/) directory below for the long-form write-ups behind each section.

## Documentation

Detailed documentation lives in the [`docs/`](docs/) directory:

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | System overview, data flow, sink interface, decorator pattern, proxy internals |
| [WAL and Logical Replication](docs/wal-and-logical-replication.md) | PostgreSQL WAL structure, logical decoding, replication protocol, pgoutput messages |
| [Sink Reference](docs/sinks.md) | All sink types, decorators (retry, batch, metrics, DLQ), configuration, chain construction |
| [Operations Guide](docs/operations.md) | Running, monitoring (Prometheus/Grafana), troubleshooting, operational procedures |

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
.github/
  workflows/
    ci.yml               GitHub Actions CI (vet, build, test)
cmd/
  pgcdc/                 CDC pipeline CLI
  pgproxy/               Read/write split proxy CLI
  webhook-test-server/   Test HTTP server for webhook sink
  flaky-server/          Intentionally-failing server for retry testing
docs/
  architecture.md        System overview, data flow, internals
  wal-and-logical-replication.md  PostgreSQL WAL and replication protocol
  sinks.md               Sink types and decorator reference
  operations.md          Running, monitoring, troubleshooting
internal/
  cdc/
    event.go             ChangeEvent data model
    event_test.go        ChangeEvent serialization tests
    filter.go            Table and action filtering
    filter_test.go       Filter matching tests
    snapshot.go          Initial data load via REPEATABLE READ
    stream.go            WAL streaming engine (replication protocol, pgoutput decoding)
  sink/
    sink.go              Sink interface
    stdout.go            NDJSON to stdout
    webhook.go           HTTP POST per event
    webhook_batch.go     HTTP POST per batch
    kafka.go             Kafka producer (franz-go, per-table topics)
    stdout_batch.go      Batched JSON to stdout
    batch.go             Batching decorator (size + time triggers)
    batch_test.go        Batch flush and close tests
    retry.go             Retry decorator (exponential backoff)
    retry_test.go        Retry and backoff tests
    dlq.go               Dead-letter queue (JSONL file)
    dlq_test.go          DLQ write and integration tests
    metrics.go           Prometheus metrics decorator
  proxy/
    proxy.go             TCP proxy with query routing
    proxy_test.go        Proxy integration tests (raw wire protocol)
    classifier.go        SQL classification (read vs write vs transaction)
    classifier_test.go   30 classification test cases
  metrics/
    metrics.go           Prometheus metric definitions
docker-compose.yml       Postgres primary + replica + Kafka
Makefile                 Build, test, run targets
```

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

### Test Coverage

| Package | Coverage | Notes |
|---------|----------|-------|
| `internal/proxy` | ~72% | Classifier fully tested; proxy integration tests use raw wire protocol |
| `internal/sink` | ~45% | All decorators tested (retry, batch, DLQ); sink types are thin wrappers |
| `internal/cdc` | ~13% | Requires a live Postgres with `wal_level=logical`; integration-tested manually |

The CDC package is intentionally undertested in unit tests because `stream.go` and `snapshot.go` require a real PostgreSQL instance with logical replication enabled. These paths are exercised via end-to-end testing with `docker-compose up` and manual verification. The proxy and sink packages have proper unit and integration tests.

### CI

GitHub Actions runs `go vet`, `go build`, and `go test` on every push and PR. See [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

## License

MIT
