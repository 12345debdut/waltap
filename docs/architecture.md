# Architecture

This document explains how pgcdc is structured internally, how data flows through the system, and why the code is organized the way it is.

## System Overview

pgcdc has two independent binaries that share library code:

```
                          ┌─────────────────────────────────┐
                          │           PostgreSQL             │
                          │                                  │
                          │  ┌───────┐       ┌───────────┐  │
                          │  │  WAL  │──────>│  pgoutput  │  │
                          │  └───────┘       └─────┬─────┘  │
                          │                        │         │
                          └────────────────────────┼─────────┘
                                                   │ logical replication
                                                   │ protocol
                                                   v
┌──────────────────────────────────────────────────────────────────────┐
│                           pgcdc binary                               │
│                                                                      │
│  ┌──────────┐    ┌────────┐    ┌─────────────────────────────────┐   │
│  │ Snapshot │───>│ Filter │───>│          Sink Chain              │   │
│  └──────────┘    └────────┘    │                                 │   │
│       │                        │  MetricsSink                    │   │
│       │          ┌────────┐    │    └─> RetrySink                │   │
│       └─────────>│ Stream │───>│          ├─> onFailure: DLQ     │   │
│                  └────────┘    │          └─> Inner Sink          │   │
│                                │              (stdout/webhook/   │   │
│                                │               kafka/batch)      │   │
│                                └─────────────────────────────────┘   │
│                                                                      │
│  ┌──────────────────┐                                                │
│  │ Prometheus /metrics │  :2112                                      │
│  └──────────────────┘                                                │
└──────────────────────────────────────────────────────────────────────┘


┌──────────────────────────────────────────────────────────────────────┐
│                          pgproxy binary                              │
│                                                                      │
│  Client ──TCP──> ┌─────────────────────┐ ──pgx pool──> Primary      │
│                  │  Wire Protocol       │                :5433       │
│                  │  Parser + Classifier │                            │
│                  │  + Query Router      │ ──pgx pool──> Replica      │
│                  └─────────────────────┘                :5435       │
│                          :5434                                       │
└──────────────────────────────────────────────────────────────────────┘
```

## Package Structure

```
internal/
├── cdc/              Core CDC engine
│   ├── stream.go       WAL streaming (replication protocol)
│   ├── filter.go       Table and action filtering
│   ├── snapshot.go     Initial data load
│   └── event.go        ChangeEvent data model
│
├── sink/             Event delivery destinations
│   ├── sink.go         Sink interface definition
│   ├── stdout.go       NDJSON to stdout
│   ├── webhook.go      HTTP POST per event
│   ├── webhook_batch.go  HTTP POST per batch
│   ├── kafka.go        Kafka producer
│   ├── stdout_batch.go   Batched JSON to stdout
│   ├── batch.go        Batching decorator
│   ├── retry.go        Retry decorator
│   ├── dlq.go          Dead-letter queue
│   └── metrics.go      Prometheus metrics decorator
│
├── proxy/            Read/write split proxy
│   ├── proxy.go        TCP proxy with wire protocol handling
│   └── classifier.go   SQL query classification
│
└── metrics/          Prometheus metric definitions
    └── metrics.go      Counter, histogram, gauge declarations
```

## Data Flow: CDC Pipeline

### Phase 1: Startup

```
1. Parse CLI flags
2. Start Prometheus metrics server (goroutine)
3. Create Filter from --tables and --actions
4. Build sink chain (inside-out):
   a. Create inner sink (stdout/webhook/kafka)
   b. Wrap with RetrySink (for network sinks)
   c. If --dlq-path set, configure RetrySink.onFailure → DLQ
   d. Wrap with MetricsSink (outermost)
5. If --snapshot: run Snapshot phase
6. Start Stream phase
```

### Phase 2: Snapshot (optional)

When `--snapshot` is set with `--tables`, pgcdc loads existing rows before streaming:

```
1. Connect to Postgres (normal SQL connection)
2. BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ
   ├── This freezes the view of all tables at this instant
   └── Concurrent writes are invisible to us
3. SELECT pg_current_wal_lsn() → record the LSN
4. For each table:
   a. Query pg_attribute for column names
   b. SELECT * FROM table
   c. For each row → emit as INSERT ChangeEvent (LSN="snapshot", Xid=0)
5. COMMIT (releases the snapshot)
```

The snapshot LSN marks where streaming should start. Any changes that committed before this LSN are captured by the snapshot; changes after it are captured by the stream.

### Phase 3: Streaming

```
1. Connect with replication protocol (?replication=database)
2. IdentifySystem → get server identity + current WAL position
3. Ensure publication exists:
   ├── If --tables set: CREATE PUBLICATION pgcdc_pub FOR TABLE users, orders
   └── If no tables: CREATE PUBLICATION pgcdc_pub FOR ALL TABLES
4. Ensure replication slot exists:
   ├── Try CREATE_REPLICATION_SLOT pgcdc_slot LOGICAL pgoutput
   └── If exists: look up confirmed_flush_lsn from pg_replication_slots
5. START_REPLICATION SLOT pgcdc_slot LOGICAL <start_lsn>
   └── Enter copy-both mode (bidirectional streaming)
6. Message loop:
   ├── Receive CopyData message from Postgres
   ├── Check first byte:
   │   ├── 'k' (PrimaryKeepalive): reply if requested
   │   └── 'w' (XLogData): decode pgoutput message
   │       ├── BeginMessage → track Xid + commit time
   │       ├── RelationMessage → cache schema info
   │       ├── InsertMessage → build ChangeEvent, call deliverIfMatch
   │       ├── UpdateMessage → build ChangeEvent (with Old if REPLICA IDENTITY FULL)
   │       ├── DeleteMessage → build ChangeEvent
   │       ├── CommitMessage → reset transaction state
   │       └── TruncateMessage → log (not forwarded)
   └── Periodically send StandbyStatusUpdate (ack LSN progress)
```

### Event Delivery

When a ChangeEvent passes the filter, it traverses the sink chain:

```
handler(ctx, event)
  └─> MetricsSink.Deliver
        ├── Record replication lag (event.Timestamp vs now)
        └── inner.Deliver
              └─> RetrySink.Deliver
                    ├── Attempt 1: inner.Deliver
                    │   └── fail? → wait baseDelay (100ms)
                    ├── Attempt 2: inner.Deliver
                    │   └── fail? → wait baseDelay * 2 (200ms)
                    ├── Attempt 3: inner.Deliver
                    │   └── fail? → wait baseDelay * 4 (400ms)
                    └── All failed → onFailure callback:
                        ├── Default: return error (stops stream)
                        └── With DLQ: write to file, return nil (stream continues)
        ├── Record delivery duration
        └── Increment events_total counter
```

## Sink Interface

Every destination implements this interface:

```go
type Sink interface {
    Deliver(ctx context.Context, event cdc.ChangeEvent) error
    Close() error
}
```

The contract:
- `Deliver` returns nil → event is considered delivered, LSN can be ack'd
- `Deliver` returns error → LSN is NOT ack'd, event replays from WAL on restart
- This gives **at-least-once** delivery semantics
- Implementations must be safe for concurrent use (future: per-table parallelism)
- `Close` is called once during shutdown for flushing buffers

### Decorator Pattern

Sinks compose as decorators. Each decorator adds one concern:

| Decorator | Responsibility |
|-----------|---------------|
| `MetricsSink` | Records Prometheus counters and histograms |
| `RetrySink` | Retries failed deliveries with exponential backoff |
| `BatchSink` | Collects events and delivers them in bulk |

This composition means you can mix features freely:

```
MetricsSink → RetrySink → WebhookSink           (single events, retried)
MetricsSink → BatchSink → WebhookBatchSink       (batched, no retry)
MetricsSink → RetrySink → KafkaSink              (single events, retried, to Kafka)
```

### Batch Sink

`BatchSink` wraps a `BatchDeliverer` (different from `Sink`) and provides dual flush triggers:

```
Events arrive one-by-one via Deliver()
         │
         v
    ┌─────────┐
    │  Buffer  │  (protected by sync.Mutex)
    └────┬────┘
         │
    ┌────┴────────────────────────┐
    │                             │
    v                             v
Size trigger                 Time trigger
(buffer >= maxSize)        (time.AfterFunc(maxWait))
    │                             │
    └──────────┬──────────────────┘
               v
      DeliverBatch(events)
```

- Size trigger: fires synchronously inside `Deliver()` when buffer reaches `maxSize`
- Time trigger: fires in a background goroutine via `time.AfterFunc`
- `Close()` flushes remaining buffered events

## Data Flow: Read/Write Split Proxy

### Connection Setup

```
Client                   Proxy                  Primary       Replica
  │                        │                       │              │
  │── SSLRequest ─────────>│                       │              │
  │<── 'N' (reject) ──────│                       │              │
  │                        │                       │              │
  │── StartupMessage ─────>│                       │              │
  │   (user=lab,db=wallab) │                       │              │
  │                        │── pgxpool.New ───────>│              │
  │                        │<── pool ready ────────│              │
  │                        │── pgxpool.New ───────────────────────>│
  │                        │<── pool ready ────────────────────────│
  │                        │                       │              │
  │<── AuthenticationOk ──│                       │              │
  │<── ParameterStatus ───│                       │              │
  │<── ReadyForQuery ─────│                       │              │
```

The proxy authenticates with backends via pgx (handles SCRAM-SHA-256 automatically) and trust-authenticates the client directly.

### Query Routing

```
Client                   Proxy                  Primary       Replica
  │                        │                       │              │
  │── Query('SELECT..') ──>│                       │              │
  │                        │── ClassifyQuery ──> READ              │
  │                        │── pool.Query ─────────────────────────>│
  │                        │<── rows ──────────────────────────────│
  │<── RowDescription ────│                       │              │
  │<── DataRow(s) ────────│                       │              │
  │<── CommandComplete ───│                       │              │
  │<── ReadyForQuery ─────│                       │              │
  │                        │                       │              │
  │── Query('INSERT..') ──>│                       │              │
  │                        │── ClassifyQuery ──> WRITE             │
  │                        │── pool.Query ────────>│              │
  │                        │<── result ────────────│              │
  │<── CommandComplete ───│                       │              │
  │<── ReadyForQuery ─────│                       │              │
  │                        │                       │              │
  │── Query('BEGIN') ─────>│                       │              │
  │                        │── inTransaction=true  │              │
  │                        │── pool.Query ────────>│              │
  │<── ReadyForQuery ─────│                       │              │
  │                        │                       │              │
  │── Query('SELECT..') ──>│                       │              │
  │                        │── (in transaction)    │              │
  │                        │── pool.Query ────────>│  (primary!)  │
  │<── rows ──────────────│                       │              │
```

### Query Classification

The classifier inspects the first SQL keyword:

| First keyword | Classification | Routed to |
|--------------|----------------|-----------|
| `SELECT`, `SHOW`, `EXPLAIN`, `SET`, `RESET`, `DISCARD` | READ | Replica |
| `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, `DROP`, `TRUNCATE` | WRITE | Primary |
| `BEGIN`, `START` | TRANSACTION | Primary (sets `inTransaction=true`) |
| `COMMIT`, `END`, `ROLLBACK` | TRANSACTION | Primary (sets `inTransaction=false`) |
| `COPY ... TO` | READ | Replica |
| `COPY ... FROM` | WRITE | Primary |
| Anything else | WRITE | Primary (safe default) |

The safety property: misrouting a read to primary wastes replica capacity but is correct. Misrouting a write to replica fails with a clear error. The classifier is conservative — unknown statements go to primary.

## ChangeEvent Schema

Every row-level change is represented as:

```json
{
  "action": "INSERT",
  "schema": "public",
  "table": "users",
  "new": {
    "id": 1,
    "name": "alice",
    "email": "alice@example.com"
  },
  "old": null,
  "timestamp": "2026-05-09T12:00:00Z",
  "lsn": "0/19CFB20",
  "xid": 792
}
```

| Field | Type | Present | Description |
|-------|------|---------|-------------|
| `action` | string | Always | `"INSERT"`, `"UPDATE"`, or `"DELETE"` |
| `schema` | string | Always | PostgreSQL schema name (usually `"public"`) |
| `table` | string | Always | Table name |
| `new` | object | INSERT, UPDATE | Column values after the change |
| `old` | object | DELETE, UPDATE* | Column values before the change |
| `timestamp` | string | Always | Transaction commit time in Postgres (not receive time) |
| `lsn` | string | Always | WAL position (Log Sequence Number) |
| `xid` | uint32 | Always | Transaction ID (all events with same Xid are one transaction) |

*`old` for UPDATE requires `REPLICA IDENTITY FULL` on the table. With the default identity, `old` is only populated for primary key columns.

## Delivery Guarantees

**At-least-once delivery.** If the sink's `Deliver` returns an error, the stream does NOT acknowledge that LSN to Postgres. On restart, Postgres replays events from the last acknowledged position. This means:

- Events may be delivered more than once after a crash/restart
- Events are never lost (assuming WAL is not recycled before ack)
- Consumers should be idempotent or track the LSN for deduplication

The dead-letter queue modifies this: when DLQ is enabled, poison events are written to a file and the error is swallowed (returns nil). The stream continues and the LSN is ack'd. The DLQ file preserves the events for manual inspection and replay.

## Thread Safety

- `Stream`: single-goroutine (the message loop runs in one goroutine)
- `BatchSink`: mutex-protected (timer goroutine vs Deliver goroutine)
- `DLQSink.Handler()`: mutex-protected file writes
- `Proxy`: one goroutine per client connection, connection pools are thread-safe
- All Prometheus metrics: atomic (provided by the prometheus library)
