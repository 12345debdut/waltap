package cdc

import "time"

// ChangeEvent represents a single row-level change captured from WAL.
//
// This is the core data structure of the entire pipeline. Every sink
// receives these, every downstream system consumes these. The design
// choices here matter:
//
//   - Action is a string ("INSERT"/"UPDATE"/"DELETE") not an enum,
//     because JSON consumers shouldn't need our Go types to decode it.
//
//   - Old/New are map[string]any, not typed structs, because the schema
//     varies per table. The consumer uses the Table/Schema fields plus
//     the column names to interpret the values.
//
//   - Timestamp is the transaction commit time from Postgres, NOT when
//     we received the event. This matters for ordering guarantees —
//     events are ordered by WAL position (LSN), not wall clock.
//
//   - LSN is included so consumers can implement exactly-once delivery
//     by tracking the last processed LSN.
type ChangeEvent struct {
	// Action is the type of change: "INSERT", "UPDATE", or "DELETE".
	Action string `json:"action"`

	// Schema is the Postgres schema name (usually "public").
	Schema string `json:"schema"`

	// Table is the table name.
	Table string `json:"table"`

	// New contains the column values after the change.
	// Present for INSERT and UPDATE. nil for DELETE.
	New map[string]any `json:"new,omitempty"`

	// Old contains the column values before the change.
	// Present for DELETE and UPDATE (only if REPLICA IDENTITY is FULL
	// or the changed columns are part of the replica identity).
	// nil for INSERT.
	Old map[string]any `json:"old,omitempty"`

	// Timestamp is when the transaction committed in Postgres.
	Timestamp time.Time `json:"timestamp"`

	// LSN is the WAL position of this change. Consumers can use this
	// for deduplication and exactly-once processing.
	LSN string `json:"lsn"`

	// Xid is the Postgres transaction ID. All events with the same
	// Xid belong to the same atomic transaction.
	Xid uint32 `json:"xid"`
}
