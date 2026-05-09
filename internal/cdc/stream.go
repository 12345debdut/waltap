package cdc

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// Stream manages the WAL streaming connection and decodes change events.
//
// It owns the replication connection, the relation cache, and the LSN
// tracking. The caller provides a handler function that receives decoded
// ChangeEvents — this decouples streaming from delivery.
type Stream struct {
	// Connection strings — we need two connections:
	//   replConn: speaks replication protocol (for WAL streaming)
	//   normalConnStr: speaks normal SQL (for DDL like CREATE PUBLICATION)
	replConnStr   string
	normalConnStr string

	slotName    string
	publication string

	// handler is called for each decoded change event.
	// If it returns an error, we stop acknowledging progress —
	// on restart, events will be re-delivered from the last ack'd LSN.
	handler func(context.Context, ChangeEvent) error

	// filter controls which events reach the handler.
	// Applied at two levels:
	//   - Postgres-side: publication is created FOR TABLE ... if tables are specified
	//   - Go-side: events are checked against filter before calling handler
	filter Filter

	// Internal state
	relations map[uint32]*pglogrepl.RelationMessage
	typeMap   *pgtype.Map

	// Current transaction context — we track these to attach
	// transaction metadata (xid, commit time) to each row event.
	currentXid        uint32
	currentCommitTime time.Time
}

// NewStream creates a new CDC stream.
//
// connStr should be a standard Postgres connection string like
// "postgres://user:pass@host:port/db". The replication=database
// parameter is added automatically for the replication connection.
func NewStream(connStr string, slotName string, publication string, filter Filter, handler func(context.Context, ChangeEvent) error) *Stream {
	return &Stream{
		replConnStr:   connStr + "?replication=database",
		normalConnStr: connStr,
		slotName:      slotName,
		publication:   publication,
		filter:        filter,
		handler:       handler,
		relations:     make(map[uint32]*pglogrepl.RelationMessage),
		typeMap:       pgtype.NewMap(),
	}
}

// Run starts streaming and blocks until the context is cancelled.
//
// The flow:
//  1. Connect with replication protocol
//  2. Ensure publication exists (via a separate normal connection)
//  3. Ensure replication slot exists
//  4. START_REPLICATION — enter copy-both mode
//  5. Loop: receive messages, decode, call handler, ack progress
func (s *Stream) Run(ctx context.Context) error {
	// --- Connect using replication protocol ---
	conn, err := pgconn.Connect(ctx, s.replConnStr)
	if err != nil {
		return fmt.Errorf("replication connect: %w", err)
	}
	defer conn.Close(ctx)
	log.Println("connected to postgres (replication mode)")

	// --- Identify system ---
	sysIdent, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return fmt.Errorf("IdentifySystem: %w", err)
	}
	log.Printf("SystemID=%s Timeline=%d XLogPos=%s",
		sysIdent.SystemID, sysIdent.Timeline, sysIdent.XLogPos)

	// --- Ensure publication ---
	if err := s.ensurePublication(ctx); err != nil {
		return fmt.Errorf("ensure publication: %w", err)
	}

	// --- Ensure replication slot ---
	startLSN, err := s.ensureSlot(ctx, conn)
	if err != nil {
		return fmt.Errorf("ensure slot: %w", err)
	}
	log.Printf("starting from LSN %s", startLSN)

	// --- Start replication ---
	err = pglogrepl.StartReplication(ctx, conn, s.slotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				"publication_names '" + s.publication + "'",
			},
		},
	)
	if err != nil {
		return fmt.Errorf("StartReplication: %w", err)
	}
	log.Println("streaming started — waiting for changes...")

	// --- Message loop ---
	return s.messageLoop(ctx, conn)
}

// messageLoop receives and processes messages until ctx is cancelled.
func (s *Stream) messageLoop(ctx context.Context, conn *pgconn.PgConn) error {
	var (
		lastReceivedLSN pglogrepl.LSN
		lastAckedLSN    pglogrepl.LSN
		ackInterval     = 10 * time.Second
		nextAckTime     = time.Now().Add(ackInterval)
	)

	for {
		if ctx.Err() != nil {
			return nil // clean shutdown
		}

		// --- Send periodic acknowledgment ---
		if time.Now().After(nextAckTime) && lastReceivedLSN > lastAckedLSN {
			if err := s.sendAck(ctx, conn, lastReceivedLSN); err != nil {
				return fmt.Errorf("ack: %w", err)
			}
			log.Printf("ack'd LSN %s", lastReceivedLSN)
			lastAckedLSN = lastReceivedLSN
			nextAckTime = time.Now().Add(ackInterval)
		}

		// --- Receive next message (with timeout for ack + shutdown checks) ---
		recvCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		rawMsg, err := conn.ReceiveMessage(recvCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue // no message — loop back to ack/shutdown check
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive: %w", err)
		}

		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if keepalive.ReplyRequested {
				if err := s.sendAck(ctx, conn, lastReceivedLSN); err != nil {
					return fmt.Errorf("keepalive reply: %w", err)
				}
			}

		case pglogrepl.XLogDataByteID:
			xlogData, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlogdata: %w", err)
			}
			lastReceivedLSN = xlogData.WALStart + pglogrepl.LSN(len(xlogData.WALData))

			if err := s.processMessage(ctx, xlogData); err != nil {
				return fmt.Errorf("process message: %w", err)
			}
		}
	}
}

// processMessage decodes a pgoutput message and calls the handler for row events.
func (s *Stream) processMessage(ctx context.Context, xlogData pglogrepl.XLogData) error {
	msg, err := pglogrepl.Parse(xlogData.WALData)
	if err != nil {
		return fmt.Errorf("parse pgoutput: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.BeginMessage:
		// Track transaction metadata so we can attach it to row events.
		s.currentXid = m.Xid
		s.currentCommitTime = m.CommitTime

	case *pglogrepl.CommitMessage:
		// Transaction complete. Reset state.
		s.currentXid = 0
		s.currentCommitTime = time.Time{}

	case *pglogrepl.RelationMessage:
		// Cache schema info. This arrives once per table at the start
		// of the stream, and again if the schema changes (ALTER TABLE).
		s.relations[m.RelationID] = m

	case *pglogrepl.InsertMessage:
		rel, ok := s.relations[m.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation %d", m.RelationID)
		}
		event := ChangeEvent{
			Action:    "INSERT",
			Schema:    rel.Namespace,
			Table:     rel.RelationName,
			New:       s.decodeTuple(m.Tuple, rel),
			Timestamp: s.currentCommitTime,
			LSN:       xlogData.WALStart.String(),
			Xid:       s.currentXid,
		}
		if err := s.deliverIfMatch(ctx, event); err != nil {
			return err
		}

	case *pglogrepl.UpdateMessage:
		rel, ok := s.relations[m.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation %d", m.RelationID)
		}
		event := ChangeEvent{
			Action:    "UPDATE",
			Schema:    rel.Namespace,
			Table:     rel.RelationName,
			New:       s.decodeTuple(m.NewTuple, rel),
			Timestamp: s.currentCommitTime,
			LSN:       xlogData.WALStart.String(),
			Xid:       s.currentXid,
		}
		if m.OldTuple != nil {
			event.Old = s.decodeTuple(m.OldTuple, rel)
		}
		if err := s.deliverIfMatch(ctx, event); err != nil {
			return err
		}

	case *pglogrepl.DeleteMessage:
		rel, ok := s.relations[m.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation %d", m.RelationID)
		}
		event := ChangeEvent{
			Action:    "DELETE",
			Schema:    rel.Namespace,
			Table:     rel.RelationName,
			Old:       s.decodeTuple(m.OldTuple, rel),
			Timestamp: s.currentCommitTime,
			LSN:       xlogData.WALStart.String(),
			Xid:       s.currentXid,
		}
		if err := s.deliverIfMatch(ctx, event); err != nil {
			return err
		}

	case *pglogrepl.TruncateMessage:
		// We could emit a special event here, but truncate is rare
		// and many CDC consumers don't handle it. Log it for now.
		log.Printf("TRUNCATE on %d relations (not forwarded to sink)", m.RelationNum)
	}

	return nil
}

// decodeTuple converts a pgoutput TupleData into a map of column_name → value.
func (s *Stream) decodeTuple(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessage) map[string]any {
	if tuple == nil {
		return nil
	}
	values := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		colDef := rel.Columns[i]
		switch col.DataType {
		case 'n':
			values[colDef.Name] = nil
		case 'u':
			// Unchanged TOAST column — the value exists but wasn't sent
			// because it didn't change. We skip it from the map so the
			// consumer knows this column wasn't part of the change.
		case 't':
			val, err := s.decodeTextColumn(col.Data, colDef.DataType)
			if err != nil {
				values[colDef.Name] = string(col.Data)
			} else {
				values[colDef.Name] = val
			}
		case 'b':
			val, err := s.decodeBinaryColumn(col.Data, colDef.DataType)
			if err != nil {
				values[colDef.Name] = fmt.Sprintf("(binary %d bytes)", len(col.Data))
			} else {
				values[colDef.Name] = val
			}
		}
	}
	return values
}

// decodeTextColumn decodes a column value from Postgres text format using pgx's type system.
func (s *Stream) decodeTextColumn(data []byte, oid uint32) (any, error) {
	typ, ok := s.typeMap.TypeForOID(oid)
	if !ok {
		return string(data), nil
	}
	return typ.Codec.DecodeValue(s.typeMap, oid, pgtype.TextFormatCode, data)
}

// decodeBinaryColumn decodes a column value from Postgres binary format using pgx's type system.
func (s *Stream) decodeBinaryColumn(data []byte, oid uint32) (any, error) {
	typ, ok := s.typeMap.TypeForOID(oid)
	if !ok {
		return nil, fmt.Errorf("unknown OID %d", oid)
	}
	return typ.Codec.DecodeValue(s.typeMap, oid, pgtype.BinaryFormatCode, data)
}

// deliverIfMatch checks the filter and calls the handler if the event passes.
func (s *Stream) deliverIfMatch(ctx context.Context, event ChangeEvent) error {
	if !s.filter.Match(event) {
		return nil // filtered out — skip silently
	}
	if err := s.handler(ctx, event); err != nil {
		return fmt.Errorf("handler %s: %w", event.Action, err)
	}
	return nil
}

// sendAck reports progress to Postgres so it can recycle WAL segments.
func (s *Stream) sendAck(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lsn + 1,
			WALFlushPosition: lsn + 1,
			WALApplyPosition: lsn + 1,
		},
	)
}

// ensurePublication creates the publication via a normal SQL connection.
func (s *Stream) ensurePublication(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, s.normalConnStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	result := conn.ExecParams(ctx,
		"SELECT 1 FROM pg_publication WHERE pubname = $1",
		[][]byte{[]byte(s.publication)}, nil, nil, nil,
	)
	rr := result.Read()
	if rr.Err != nil {
		return rr.Err
	}

	if len(rr.Rows) == 0 {
		// Build the CREATE PUBLICATION statement.
		// If the filter specifies tables, use FOR TABLE ... so Postgres
		// skips decoding WAL for unrelated tables entirely (server-side filtering).
		// Otherwise, FOR ALL TABLES captures everything.
		pubSQL := "CREATE PUBLICATION " + s.publication
		if tables := s.filter.TableList(); len(tables) > 0 {
			pubSQL += " FOR TABLE " + strings.Join(tables, ", ")
		} else {
			pubSQL += " FOR ALL TABLES"
		}

		result := conn.ExecParams(ctx, pubSQL, nil, nil, nil, nil)
		if err := result.Read().Err; err != nil {
			return fmt.Errorf("CREATE PUBLICATION: %w", err)
		}
		log.Printf("created publication %q (%s)", s.publication, pubSQL)
	} else {
		log.Printf("publication %q exists", s.publication)
	}
	return nil
}

// ensureSlot creates the replication slot or reuses an existing one.
func (s *Stream) ensureSlot(ctx context.Context, conn *pgconn.PgConn) (pglogrepl.LSN, error) {
	result, err := pglogrepl.CreateReplicationSlot(ctx, conn, s.slotName,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication},
	)
	if err != nil {
		// Slot exists — look up its position via normal connection.
		lsn, lookupErr := s.getSlotLSN(ctx)
		if lookupErr != nil {
			return 0, fmt.Errorf("create failed (%v), lookup failed: %w", err, lookupErr)
		}
		log.Printf("reusing slot %q", s.slotName)
		return lsn, nil
	}

	log.Printf("created slot %q", s.slotName)
	lsn, _ := pglogrepl.ParseLSN(result.ConsistentPoint)
	return lsn, nil
}

// getSlotLSN looks up the confirmed_flush_lsn for an existing replication slot.
func (s *Stream) getSlotLSN(ctx context.Context) (pglogrepl.LSN, error) {
	conn, err := pgconn.Connect(ctx, s.normalConnStr)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	result := conn.ExecParams(ctx,
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
		[][]byte{[]byte(s.slotName)}, nil, nil, nil,
	)
	rr := result.Read()
	if rr.Err != nil {
		return 0, rr.Err
	}
	if len(rr.Rows) == 0 {
		return 0, fmt.Errorf("slot %q not found", s.slotName)
	}
	return pglogrepl.ParseLSN(string(rr.Rows[0][0]))
}
