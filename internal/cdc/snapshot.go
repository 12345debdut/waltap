package cdc

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

// Snapshot reads all existing rows from the specified tables and emits
// them as INSERT events to the handler.
//
// Why is this needed?
// A replication slot only captures changes AFTER it's created. If the
// table already has 1 million rows, the CDC consumer never sees them.
// A snapshot loads the current state first, then the stream picks up
// from where the snapshot ended.
//
// How it works:
//  1. Open a transaction with REPEATABLE READ isolation — this gives us
//     a consistent view of all tables at the same point in time.
//  2. Record the current WAL LSN — the stream will start from here.
//  3. SELECT * from each table, emit each row as an INSERT ChangeEvent.
//  4. Commit (releases the snapshot).
//
// The snapshot LSN and the slot's start LSN must match, or we'll either
// miss changes (gap) or see duplicates (overlap). The caller is responsible
// for creating the slot at the right LSN.
type Snapshot struct {
	connStr string
	tables  []string
	handler func(context.Context, ChangeEvent) error
}

// NewSnapshot creates a snapshot loader.
//
// tables: list of table names to snapshot (e.g., ["users", "orders"]).
// If empty, no snapshot is taken.
func NewSnapshot(connStr string, tables []string, handler func(context.Context, ChangeEvent) error) *Snapshot {
	return &Snapshot{
		connStr: connStr,
		tables:  tables,
		handler: handler,
	}
}

// Run performs the snapshot and returns the LSN at which the snapshot
// was taken. The caller should start streaming from this LSN.
func (s *Snapshot) Run(ctx context.Context) (snapshotLSN string, err error) {
	if len(s.tables) == 0 {
		return "", nil
	}

	// Connect with normal (non-replication) connection.
	conn, err := pgx.Connect(ctx, s.connStr)
	if err != nil {
		return "", fmt.Errorf("snapshot connect: %w", err)
	}
	defer conn.Close(ctx)

	// Start a REPEATABLE READ transaction.
	//
	// This is important: REPEATABLE READ gives us a consistent snapshot
	// of the entire database at one point in time. Even if other transactions
	// insert/update/delete rows while we're reading, we see the state as
	// it was when our transaction started.
	//
	// READ COMMITTED (the default) would NOT work — we'd see partial
	// updates from concurrent transactions, leading to an inconsistent
	// snapshot.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return "", fmt.Errorf("begin snapshot tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Record the current WAL position. Changes after this LSN will be
	// captured by the streaming phase.
	var lsn string
	err = tx.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	if err != nil {
		return "", fmt.Errorf("get snapshot LSN: %w", err)
	}
	log.Printf("snapshot at LSN %s", lsn)

	// Snapshot each table.
	for _, table := range s.tables {
		count, err := s.snapshotTable(ctx, tx, table)
		if err != nil {
			return "", fmt.Errorf("snapshot table %s: %w", table, err)
		}
		log.Printf("snapshot: %s — %d rows", table, count)
	}

	// We don't actually need to commit — ROLLBACK is fine since we only
	// read data. But committing is cleaner for logging.
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit snapshot tx: %w", err)
	}

	return lsn, nil
}

// snapshotTable reads all rows from a single table and emits them as INSERTs.
func (s *Snapshot) snapshotTable(ctx context.Context, tx pgx.Tx, table string) (int64, error) {
	// Get the schema — we need column names for the ChangeEvent.
	//
	// We query pg_attribute joined with pg_class to get column names and
	// their order, excluding system columns (attnum > 0) and dropped
	// columns (NOT attisdropped).
	colRows, err := tx.Query(ctx, `
		SELECT a.attname
		FROM pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE c.relname = $1
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY a.attnum
	`, table)
	if err != nil {
		return 0, fmt.Errorf("query columns: %w", err)
	}

	var columns []string
	for colRows.Next() {
		var col string
		if err := colRows.Scan(&col); err != nil {
			colRows.Close()
			return 0, fmt.Errorf("scan column name: %w", err)
		}
		columns = append(columns, col)
	}
	colRows.Close()
	if err := colRows.Err(); err != nil {
		return 0, fmt.Errorf("iterate columns: %w", err)
	}

	if len(columns) == 0 {
		return 0, fmt.Errorf("table %q not found or has no columns", table)
	}

	// Read all rows. We use a cursor (rows.Next()) rather than loading
	// everything into memory — this handles tables of any size.
	//
	// Table names come from our --tables flag, not user input, but we
	// quote them for defense-in-depth.
	rows, err := tx.Query(ctx, fmt.Sprintf("SELECT * FROM %s", quoteIdent(table)))
	if err != nil {
		return 0, fmt.Errorf("query rows: %w", err)
	}
	defer rows.Close()

	now := time.Now()
	var count int64

	for rows.Next() {
		// Scan into a generic slice of any
		values, err := rows.Values()
		if err != nil {
			return count, fmt.Errorf("scan row: %w", err)
		}

		// Build the column→value map
		rowMap := make(map[string]any, len(columns))
		for i, col := range columns {
			if i < len(values) {
				rowMap[col] = values[i]
			}
		}

		event := ChangeEvent{
			Action:    "INSERT",
			Schema:    "public", // TODO: support non-public schemas
			Table:     table,
			New:       rowMap,
			Timestamp: now,
			LSN:       "snapshot",
			Xid:       0,
		}

		if err := s.handler(ctx, event); err != nil {
			return count, fmt.Errorf("handler: %w", err)
		}
		count++
	}

	return count, rows.Err()
}

// quoteIdent quotes a SQL identifier to prevent injection.
func quoteIdent(name string) string {
	return `"` + name + `"`
}
