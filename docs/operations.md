# Operations Guide

How to run, monitor, and troubleshoot pgcdc in practice.

## Running the Infrastructure

### Start Everything

```bash
make docker-up
```

This starts three containers:
- `pgwal-lab` — PostgreSQL 16 primary (port 5433)
- `pgwal-replica` — PostgreSQL 16 streaming replica (port 5435)
- `pgwal-kafka` — Apache Kafka 3.9 in KRaft mode (port 9092)

### Start Only Postgres (No Kafka)

```bash
make docker-up-pg
```

### Stop Everything

```bash
make docker-down
```

### First-Time Database Setup

After `docker-compose up`, create your tables and set replica identity:

```bash
docker exec pgwal-lab psql -U lab -d wallab -c "
  CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL
  );
  ALTER TABLE users REPLICA IDENTITY FULL;
"
```

`REPLICA IDENTITY FULL` ensures UPDATE and DELETE events include all column values in the `old` field, not just the primary key.

## Monitoring

### Prometheus Metrics

pgcdc exposes metrics at `http://localhost:2112/metrics` (configurable via `--metrics-addr`).

**Key queries for Grafana/Prometheus:**

```promql
# Events processed per second
rate(pgcdc_events_total[5m])

# Error rate
rate(pgcdc_events_total{status="error"}[5m])

# Delivery latency p99
histogram_quantile(0.99, rate(pgcdc_delivery_duration_seconds_bucket[5m]))

# Delivery latency p50
histogram_quantile(0.50, rate(pgcdc_delivery_duration_seconds_bucket[5m]))

# Current replication lag
pgcdc_replication_lag_seconds

# Retry rate
rate(pgcdc_retries_total[5m])

# DLQ events (should be 0 normally)
rate(pgcdc_dlq_events_total[5m])

# Batch flush rate
rate(pgcdc_batch_flush_total[5m])

# Average batch size
rate(pgcdc_batch_size_sum[5m]) / rate(pgcdc_batch_size_count[5m])
```

**Alerting suggestions:**

| Condition | Severity | Meaning |
|-----------|----------|---------|
| `pgcdc_replication_lag_seconds > 30` | Warning | Falling behind, check sink throughput |
| `pgcdc_replication_lag_seconds > 300` | Critical | 5+ minutes behind, WAL may accumulate |
| `rate(pgcdc_events_total{status="error"}[5m]) > 0` | Warning | Delivery failures occurring |
| `pgcdc_dlq_events_total > 0` | Warning | Events going to dead-letter queue |

### Checking Replication Slot Health

```sql
-- On the primary: check slot status
SELECT slot_name, active, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name = 'pgcdc_slot';
```

If `active` is `false` and lag is growing, pgcdc is not running and WAL is accumulating.

### Checking Streaming Replica Status

```sql
-- On the primary: check replica connection
SELECT client_addr, state, sent_lsn, replay_lsn,
       pg_size_pretty(pg_wal_lsn_diff(sent_lsn, replay_lsn)) AS replay_lag
FROM pg_stat_replication;
```

## Troubleshooting

### "replication slot is active for PID ..."

Another connection is using the slot. This happens when you restart pgcdc without the previous process fully shutting down.

```sql
-- Find and terminate the old connection
SELECT pg_terminate_backend(active_pid)
FROM pg_replication_slots
WHERE slot_name = 'pgcdc_slot' AND active_pid IS NOT NULL;
```

### WAL Accumulation (Disk Full Risk)

An inactive replication slot prevents WAL recycling. Check:

```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained_wal
FROM pg_replication_slots;
```

If a slot is inactive and retaining gigabytes of WAL:

```sql
-- Drop the slot (you'll lose unprocessed events)
SELECT pg_drop_replication_slot('pgcdc_slot');
```

pgcdc will recreate the slot on next startup.

### "publication already exists"

pgcdc checks for the publication on startup and reuses it if it exists. If you change `--tables`, you need to drop the old publication:

```sql
DROP PUBLICATION pgcdc_pub;
```

pgcdc will recreate it with the new table list.

### Kafka: UNKNOWN_TOPIC_OR_PARTITION

This happens on the first event for a new table if Kafka's auto-topic-creation is enabled but hasn't finished. The retry logic handles this automatically — the second attempt succeeds after the topic is created.

If auto-topic-creation is disabled, create topics manually:

```bash
kafka-topics.sh --create --topic pgcdc.public.users \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

### Replica Not Syncing

Check the replica logs:

```bash
docker logs pgwal-replica
```

Common issues:
- Primary's `pg_hba.conf` doesn't allow replication from the replica's IP
- The `lab` user doesn't have the `REPLICATION` privilege
- Network connectivity between containers

Fix:

```sql
-- On primary: grant replication
ALTER ROLE lab REPLICATION;

-- On primary: add pg_hba rule
-- Add to pg_hba.conf: host replication all 0.0.0.0/0 md5
SELECT pg_reload_conf();
```

## Operational Procedures

### Clean Restart

```bash
# Stop pgcdc (Ctrl+C or kill)
# The slot retains its position

# Restart pgcdc — it resumes from last ack'd LSN
./pgcdc --sink stdout
```

### Changing Table Filters

```bash
# 1. Stop pgcdc
# 2. Drop the old publication
docker exec pgwal-lab psql -U lab -d wallab -c "DROP PUBLICATION pgcdc_pub;"

# 3. Optionally drop and recreate the slot (to avoid replaying old events)
docker exec pgwal-lab psql -U lab -d wallab -c "SELECT pg_drop_replication_slot('pgcdc_slot');"

# 4. Start pgcdc with new filters
./pgcdc --sink stdout --tables users,orders,payments
```

### Replaying DLQ Events

DLQ events are preserved as JSON and can be replayed manually:

```bash
# Extract events from the DLQ
cat failed-events.jsonl | jq -c '.event' > replay.jsonl

# Send each event to the webhook manually
while read -r event; do
  curl -s -X POST http://localhost:9090/events \
    -H "Content-Type: application/json" \
    -d "$event"
done < replay.jsonl
```

### Full Reset

To start completely fresh:

```bash
# Stop pgcdc
make docker-down
docker volume rm walengine_pgdata walengine_pgreplica
make docker-up

# Recreate tables, pgcdc will create new slot + publication
```
