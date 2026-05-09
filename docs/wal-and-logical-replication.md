# PostgreSQL WAL and Logical Replication

This document explains the PostgreSQL internals that pgcdc builds on. Understanding these concepts is essential to understanding why the code works the way it does.

## Write-Ahead Logging (WAL)

Every change in PostgreSQL is written to the Write-Ahead Log before it reaches the actual data files. This is the core durability guarantee: if Postgres crashes after writing to WAL but before updating the data file, it can replay the WAL to recover.

### WAL Structure

WAL is stored as a sequence of 16MB segment files in `pg_wal/`:

```
pg_wal/
├── 000000010000000000000001
├── 000000010000000000000002
├── 000000010000000000000003
└── ...
```

Each file name encodes: `TimelineID (8 hex) + LogID (8 hex) + SegmentID (8 hex)`

Inside each segment, WAL records are packed sequentially. Each record has:

```
┌──────────────────────────────────────────┐
│ xl_tot_len  (uint32) - total length      │
│ xl_xid      (uint32) - transaction ID    │
│ xl_prev     (uint64) - previous record   │
│ xl_info     (uint8)  - record type flags │
│ xl_rmid     (uint8)  - resource manager  │
│ xl_crc      (uint32) - CRC checksum      │
│ data...              - record payload    │
└──────────────────────────────────────────┘
```

The `xl_rmid` (resource manager) identifies which subsystem produced the record:

| rmid | Name | What it tracks |
|------|------|----------------|
| 0 | XLOG | Checkpoints, switches |
| 1 | Transaction | Commit, abort |
| 10 | Heap | INSERT, UPDATE, DELETE on tables |
| 11 | Heap2 | VACUUM, freeze |
| ... | ... | ... |

You can inspect raw WAL with `pg_waldump`:

```bash
pg_waldump 000000010000000000000001
```

This shows the physical WAL — byte-level page changes. For CDC, we need something higher-level.

## Physical vs Logical WAL

**Physical WAL** records byte-level changes to data pages. A single `INSERT INTO users(name) VALUES('alice')` might produce:

```
Heap INSERT: off 3, blkref #0: rel 1663/16384/16385 blk 0
```

This tells you: "on page 0 of table 16385 in database 16384, a tuple was inserted at offset 3." You'd need to understand Postgres's page layout, tuple format, and OID mappings to reconstruct the row. Physical replication uses this to keep byte-identical copies of the database.

**Logical WAL** translates physical changes into row-level events:

```
table public.users: INSERT: id[integer]:1 name[text]:'alice'
```

This is human-readable and schema-aware. Logical decoding is what makes CDC possible — it gives us structured events without needing to understand page layouts.

## Logical Decoding

Logical decoding is PostgreSQL's built-in framework for translating WAL into application-level change events. It requires:

1. **`wal_level = logical`** — tells Postgres to write extra information into WAL (like table OIDs and column values) that physical replication doesn't need
2. **An output plugin** — transforms decoded changes into a specific format
3. **A replication slot** — tracks how far a consumer has read

### Output Plugins

PostgreSQL ships with two built-in plugins:

| Plugin | Format | Use case |
|--------|--------|----------|
| `test_decoding` | Human-readable text | Debugging, learning |
| `pgoutput` | Binary protocol | Production CDC (what pgcdc uses) |

`test_decoding` output looks like:

```
BEGIN 1234
table public.users: INSERT: id[integer]:1 name[text]:'alice'
COMMIT 1234
```

`pgoutput` is a binary protocol that produces structured messages (BeginMessage, RelationMessage, InsertMessage, etc.) — faster to parse and used by all production CDC tools.

### Publications

A publication defines which tables to capture:

```sql
-- Capture specific tables
CREATE PUBLICATION pgcdc_pub FOR TABLE users, orders;

-- Capture all tables
CREATE PUBLICATION pgcdc_pub FOR ALL TABLES;
```

When a publication specifies tables, Postgres skips decoding WAL for unrelated tables entirely. This is server-side filtering — the WAL records exist but the decoding step (which is CPU-intensive) is skipped for tables not in the publication.

### Replication Slots

A slot is a server-side bookmark that tracks a consumer's read position:

```sql
-- View all slots
SELECT slot_name, confirmed_flush_lsn, active FROM pg_replication_slots;
```

Critical properties:
- **Slots prevent WAL recycling.** Postgres won't delete WAL segments that a slot hasn't consumed yet. An inactive slot can cause WAL to accumulate indefinitely.
- **Slots survive restarts.** The position is persisted to disk.
- **Slots are exclusive.** Only one connection can use a slot at a time.

pgcdc creates a slot named `pgcdc_slot` (configurable with `--slot`). If you stop pgcdc, the slot keeps tracking where you left off. When you restart, streaming resumes from the last acknowledged position.

### REPLICA IDENTITY

Controls what data Postgres includes for UPDATE and DELETE events:

```sql
-- Default: only primary key columns in Old
ALTER TABLE users REPLICA IDENTITY DEFAULT;

-- Full: all columns in both Old and New
ALTER TABLE users REPLICA IDENTITY FULL;
```

| Identity | INSERT.New | UPDATE.Old | UPDATE.New | DELETE.Old |
|----------|-----------|------------|------------|------------|
| DEFAULT | All columns | PK only | All columns | PK only |
| FULL | All columns | All columns | All columns | All columns |

pgcdc works with both, but `FULL` gives consumers more information for building audit logs or syncing external systems.

## The Streaming Replication Protocol

pgcdc connects to Postgres using the streaming replication protocol — the same protocol used by physical replicas. The connection string includes `?replication=database` to enable this mode.

### Handshake

```
pgcdc                                      PostgreSQL
  │                                            │
  │── IdentifySystem ─────────────────────────>│
  │<── SystemID, Timeline, XLogPos ────────────│
  │                                            │
  │── CREATE_REPLICATION_SLOT ────────────────>│
  │<── SlotName, ConsistentPoint ──────────────│
  │                                            │
  │── START_REPLICATION SLOT pgcdc_slot        │
  │   LOGICAL 0/19CC5C3                        │
  │   (proto_version '1',                      │
  │    publication_names 'pgcdc_pub') ────────>│
  │                                            │
  │   [enters copy-both mode]                  │
  │<══════════════════════════════════════════>│
```

### Copy-Both Mode

After `START_REPLICATION`, the connection enters "copy-both mode" — both sides can send messages simultaneously:

**Postgres → pgcdc (server messages):**

Each message is wrapped in a `CopyData` envelope. The first byte identifies the type:

| Byte | Message | Purpose |
|------|---------|---------|
| `'w'` (0x77) | XLogData | Contains WAL data (pgoutput messages) |
| `'k'` (0x6B) | PrimaryKeepalive | Heartbeat; may request a reply |

**pgcdc → Postgres (client messages):**

| Message | Purpose |
|---------|---------|
| StandbyStatusUpdate | Reports the last WAL position pgcdc has processed |

The status update is critical: it tells Postgres "I've processed everything up to LSN X, you can recycle WAL before that point." pgcdc sends this periodically (every 10 seconds) and whenever Postgres requests it via a keepalive.

### pgoutput Messages

Inside each XLogData message, the payload is a pgoutput protocol message:

| Message | When | Content |
|---------|------|---------|
| `BeginMessage` | Transaction starts | Xid, commit timestamp, final LSN |
| `RelationMessage` | First time a table appears (or schema changes) | Table name, column names, column types |
| `InsertMessage` | Row inserted | Relation ID + TupleData (new values) |
| `UpdateMessage` | Row updated | Relation ID + OldTupleData + NewTupleData |
| `DeleteMessage` | Row deleted | Relation ID + OldTupleData |
| `CommitMessage` | Transaction ends | Commit LSN, commit timestamp |
| `TruncateMessage` | Table truncated | List of relation IDs |

pgcdc maintains a **relation cache** (`map[uint32]*RelationMessage`) because InsertMessage, UpdateMessage, and DeleteMessage only contain a relation ID — you need the cached RelationMessage to know the table name and column definitions.

### TupleData Decoding

Each column in a TupleData has a type flag:

| Flag | Meaning | How pgcdc handles it |
|------|---------|---------------------|
| `'n'` | NULL | Sets value to `nil` |
| `'u'` | Unchanged TOAST | Omits from map (column wasn't part of the change) |
| `'t'` | Text format | Decodes using pgx type codec |
| `'b'` | Binary format | Decodes using pgx type codec |

pgcdc uses pgx's type system (`pgtype.Map`) to decode column values from their PostgreSQL wire format into Go native types (int64, string, time.Time, etc.), which then marshal cleanly to JSON.

## LSN (Log Sequence Number)

An LSN is a 64-bit pointer into the WAL stream, displayed as `segment/offset` in hex (e.g., `0/19CFB20`). Every WAL record has a unique LSN, and they increase monotonically.

pgcdc uses LSNs for:
- **Resuming after restart**: the replication slot stores the last ack'd LSN
- **Ordering events**: events are ordered by LSN, not wall clock time
- **Consumer deduplication**: consumers can track the last processed LSN for exactly-once semantics

## How Streaming Replication Works (Replica)

The pgproxy binary uses a streaming replica. Here's how that works:

```
Primary                           Replica
  │                                  │
  │  WAL records ──────────────────> │  (WAL receiver process)
  │  (continuous stream)             │
  │                                  │  Apply WAL changes
  │                                  │  to local data files
  │                                  │
  │ <── StandbyStatusUpdate ──────── │  (reports replay position)
```

The replica:
1. Starts with a base backup (`pg_basebackup`) — a physical copy of all data files
2. Connects to the primary's replication protocol
3. Receives WAL records continuously
4. Applies them to its local copy
5. Serves read-only queries via `hot_standby=on`

This is **physical replication** (byte-level), different from the **logical replication** that pgcdc uses. Both use WAL, but at different abstraction levels:

| Aspect | Physical (replica) | Logical (pgcdc) |
|--------|-------------------|-----------------|
| WAL data | Raw page bytes | Decoded row changes |
| Plugin | None (built-in) | pgoutput |
| Output | Identical database copy | Structured events (JSON) |
| Use case | High availability, read scaling | CDC, event streaming |
