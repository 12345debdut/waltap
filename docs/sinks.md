# Sink Reference

Sinks are where pgcdc delivers change events. Each sink implements the `Sink` interface and can be composed with decorators (retry, batching, metrics).

## Sink Types

### stdout

Writes one JSON object per line (NDJSON) to stdout. The simplest sink — useful for debugging, piping to other tools, or log aggregation.

```bash
./pgcdc --sink stdout
```

Output:

```json
{"action":"INSERT","schema":"public","table":"users","new":{"id":1,"name":"alice"},"timestamp":"...","lsn":"0/19CFB20","xid":792}
{"action":"UPDATE","schema":"public","table":"users","old":{"id":1,"name":"alice"},"new":{"id":1,"name":"bob"},"timestamp":"...","lsn":"0/19CFB88","xid":793}
```

Pipe to jq for pretty-printing:

```bash
./pgcdc --sink stdout | jq .
```

Filter to a specific table:

```bash
./pgcdc --sink stdout | jq 'select(.table == "orders")'
```

**File:** `internal/sink/stdout.go`

### webhook

HTTP POST per event. Integrates with any HTTP endpoint: serverless functions, REST APIs, webhook receivers.

```bash
./pgcdc --sink webhook --webhook-url http://localhost:9090/events
```

Each event sends:

```http
POST /events HTTP/1.1
Content-Type: application/json
X-PGCDC-Action: INSERT
X-PGCDC-Table: public.users

{"action":"INSERT","schema":"public","table":"users","new":{"id":1,...},...}
```

The sink considers the delivery successful if the endpoint returns HTTP 2xx. Any other status code triggers a retry (when wrapped with RetrySink).

**Timeout:** 10 seconds per request.

**File:** `internal/sink/webhook.go`

### webhook-batch

HTTP POST per batch. Sends a JSON array of events. More efficient than individual webhooks when the downstream can handle batch processing.

```bash
./pgcdc --sink webhook-batch \
  --webhook-url http://localhost:9090/batch \
  --batch-size 100 \
  --batch-wait 500ms
```

Each batch sends:

```http
POST /batch HTTP/1.1
Content-Type: application/json
X-PGCDC-Batch-Size: 5

[{"action":"INSERT",...},{"action":"UPDATE",...},...]
```

**Timeout:** 30 seconds per request (longer for larger batches).

**File:** `internal/sink/webhook_batch.go`

### kafka

Publishes each event to a Kafka topic. Uses per-table topics following the Debezium naming convention.

```bash
./pgcdc --sink kafka \
  --kafka-brokers localhost:9092 \
  --kafka-topic-prefix pgcdc
```

**Topic naming:** `{prefix}.{schema}.{table}`

Examples:
- `pgcdc.public.users`
- `pgcdc.public.orders`

**Message key:** JSON object with the `id` column (if present), e.g., `{"id": 42}`. This ensures all changes to the same row go to the same partition, preserving per-row ordering. If no `id` column exists, falls back to `schema.table.lsn`.

**Message value:** Full ChangeEvent as JSON.

**Producer settings:**
- `ProducerLinger(10ms)` — batches messages internally for throughput
- `RequiredAcks(AllISRAcks())` — waits for all in-sync replicas to ack
- `ProducerBatchMaxBytes(64KB)` — max batch size per partition

**Library:** franz-go (pure Go, no CGo/librdkafka dependency)

**File:** `internal/sink/kafka.go`

### stdout-batch

Writes batches as JSON objects to stdout. Useful for testing the batching logic.

```bash
./pgcdc --sink stdout-batch --batch-size 5 --batch-wait 1s
```

Output:

```json
{"batch_size":5,"events":[{...},{...},{...},{...},{...}]}
```

**File:** `internal/sink/stdout_batch.go`

## Decorators

### RetrySink

Wraps any Sink with exponential backoff retry.

```
Attempt 1: inner.Deliver(event)
  └─ fail → wait 100ms
Attempt 2: inner.Deliver(event)
  └─ fail → wait 200ms
Attempt 3: inner.Deliver(event)
  └─ fail → wait 400ms
...
All attempts exhausted → onFailure callback
```

**Configuration:**

| Flag | Default | Description |
|------|---------|-------------|
| `--retry-attempts` | 5 | Maximum delivery attempts |
| `--retry-base-delay` | 100ms | Initial backoff delay |
| `--retry-max-delay` | 30s | Maximum backoff delay cap |

The backoff formula: `delay = baseDelay * 2^attempt`, capped at `maxDelay`.

**Failure handling:** When all retries are exhausted, the `onFailure` callback decides what happens:

- **Default (no DLQ):** Returns the error. This stops the stream. On restart, the event replays from WAL.
- **With DLQ:** Writes the event to the DLQ file. Returns nil. The stream continues.

**Context cancellation:** If the context is cancelled during a retry wait (e.g., Ctrl+C shutdown), the retry loop exits immediately.

**File:** `internal/sink/retry.go`

### BatchSink

Wraps a `BatchDeliverer` and collects events into batches.

**Flush triggers (whichever comes first):**
1. Buffer reaches `--batch-size` events → synchronous flush
2. `--batch-wait` duration elapses since first event in batch → background flush via `time.AfterFunc`

**On shutdown:** `Close()` flushes any remaining buffered events.

**Thread safety:** Buffer access is mutex-protected because the timer goroutine and the Deliver goroutine can race.

| Flag | Default | Description |
|------|---------|-------------|
| `--batch-size` | 100 | Maximum events per batch |
| `--batch-wait` | 500ms | Maximum wait before flushing a partial batch |

**File:** `internal/sink/batch.go`

### MetricsSink

Wraps any Sink and records Prometheus metrics on every delivery. Always the outermost decorator — this way it captures the full delivery latency including retries.

Records:
- `pgcdc_delivery_duration_seconds` — histogram of delivery time
- `pgcdc_events_total` — counter with labels for action, table, and delivery status
- `pgcdc_replication_lag_seconds` — gauge measuring delay between commit time and processing time

**File:** `internal/sink/metrics.go`

### DLQSink

Dead-letter queue. Not a regular Sink — it's used as the `onFailure` callback for RetrySink.

```bash
./pgcdc --sink webhook \
  --webhook-url http://flaky-service/events \
  --dlq-path ./failed-events.jsonl
```

When an event exhausts all retry attempts, it's written to the DLQ file as a JSONL entry:

```json
{"event":{"action":"INSERT","schema":"public","table":"users","new":{...},...},"error":"POST http://... returned 503","failed_at":"2026-05-09T22:23:07Z"}
```

Each entry contains:
- `event` — the original ChangeEvent (preserved exactly)
- `error` — the last delivery error message
- `failed_at` — when the event was written to the DLQ

**Inspecting the DLQ:**

```bash
# Count failed events
wc -l failed-events.jsonl

# See which tables had failures
cat failed-events.jsonl | jq '.event.table' | sort | uniq -c

# See the errors
cat failed-events.jsonl | jq '.error'

# Extract just the events (for replay)
cat failed-events.jsonl | jq '.event'
```

**Safety:** If the DLQ file write itself fails, the original error is returned. This stops the stream — it's better to halt than silently lose events.

**File:** `internal/sink/dlq.go`

## Sink Chain Construction

The sink chain is built in `cmd/pgcdc/main.go`. The order matters:

```
1. Create the inner sink (based on --sink flag)
2. For network sinks: wrap with RetrySink
3. If --dlq-path: configure RetrySink's onFailure → DLQSink.Handler()
4. Wrap everything with MetricsSink (outermost)
```

Resulting chains:

| --sink | Chain |
|--------|-------|
| `stdout` | MetricsSink → StdoutSink |
| `webhook` | MetricsSink → RetrySink → WebhookSink |
| `webhook` + DLQ | MetricsSink → RetrySink(onFailure→DLQ) → WebhookSink |
| `kafka` | MetricsSink → RetrySink → KafkaSink |
| `stdout-batch` | MetricsSink → BatchSink → StdoutBatchSink |
| `webhook-batch` | MetricsSink → BatchSink → WebhookBatchSink |

Note: `stdout` doesn't get a RetrySink because writing to stdout can't transiently fail. Batch sinks don't currently get retry — the batch deliverer would need its own retry logic.
