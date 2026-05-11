package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Proxy is a Postgres read/write split proxy.
//
// It sits between the application and Postgres, parsing the wire protocol
// to route queries to either the primary (writes) or replica (reads).
//
// Architecture:
//
//   Client ←→ Proxy ←→ Primary (for writes + transactions)
//                   ←→ Replica (for standalone SELECTs)
//
// How it works:
//   1. Client connects to the proxy via raw TCP
//   2. Proxy handles the Postgres startup/auth handshake directly
//   3. Proxy maintains pgx connection pools to both backends
//   4. For each client query, proxy classifies it and routes to the right pool
//   5. Response is relayed back to the client in wire protocol format
//
// Why two layers (raw TCP for clients, pgx for backends)?
//   - Raw TCP on the client side: we need to intercept and parse the
//     wire protocol to classify queries before they reach a backend.
//   - pgx for backends: handles SCRAM-SHA-256 auth, connection pooling,
//     and the full Postgres protocol correctly. Implementing SCRAM
//     ourselves would be complex and error-prone.
//
// When a transaction starts (BEGIN), ALL queries go to primary until
// COMMIT/ROLLBACK. This is critical — a SELECT inside a transaction
// might depend on an uncommitted INSERT in the same transaction.
type Proxy struct {
	listenAddr  string
	primaryDSN  string
	replicaDSN  string
	listener    net.Listener
	primaryPool *pgxpool.Pool
	replicaPool *pgxpool.Pool
	activeConns atomic.Int64
}

// NewProxy creates a proxy that listens on listenAddr and routes
// queries between primary and replica Postgres instances.
//
// primaryDSN and replicaDSN are full Postgres connection strings
// (e.g., "postgres://user:pass@host:port/db").
func NewProxy(listenAddr, primaryDSN, replicaDSN string) *Proxy {
	return &Proxy{
		listenAddr: listenAddr,
		primaryDSN: primaryDSN,
		replicaDSN: replicaDSN,
	}
}

// ListenAndRun creates a listener and runs the proxy.
func (p *Proxy) ListenAndRun(ctx context.Context) error {
	ln, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	return p.Run(ctx, ln)
}

// Run starts the proxy on the given listener and blocks until the context is cancelled.
func (p *Proxy) Run(ctx context.Context, ln net.Listener) error {
	p.listener = ln

	// Set up connection pools to both backends
	var err error
	p.primaryPool, err = pgxpool.New(ctx, p.primaryDSN)
	if err != nil {
		return fmt.Errorf("connect to primary: %w", err)
	}
	defer p.primaryPool.Close()

	p.replicaPool, err = pgxpool.New(ctx, p.replicaDSN)
	if err != nil {
		return fmt.Errorf("connect to replica: %w", err)
	}
	defer p.replicaPool.Close()

	// Verify both backends are reachable
	if err := p.primaryPool.Ping(ctx); err != nil {
		return fmt.Errorf("ping primary: %w", err)
	}
	if err := p.replicaPool.Ping(ctx); err != nil {
		return fmt.Errorf("ping replica: %w", err)
	}

	log.Printf("proxy listening on %s (primary=%s, replica=%s)",
		ln.Addr().String(), p.primaryDSN, p.replicaDSN)

	// Close listener when context cancels
	go func() {
		<-ctx.Done()
		p.listener.Close()
	}()

	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("accept error: %v", err)
			continue
		}

		p.activeConns.Add(1)
		go func() {
			defer p.activeConns.Add(-1)
			p.handleClient(ctx, conn)
		}()
	}
}

// handleClient manages a single client connection.
//
// We handle the Postgres wire protocol startup ourselves (SSLRequest,
// startup message, authentication) and then route queries.
//
// For simplicity, we use "trust" auth with the client — the proxy
// itself handles auth with the backends via pgx. In production,
// you'd implement proper client auth here too.
func (p *Proxy) handleClient(ctx context.Context, client net.Conn) {
	defer client.Close()

	clientAddr := client.RemoteAddr().String()
	log.Printf("[%s] new connection", clientAddr)

	// Step 1: Handle SSL negotiation
	startupMsg, err := readRawMessage(client, true)
	if err != nil {
		log.Printf("[%s] failed to read startup: %v", clientAddr, err)
		return
	}

	// Check for SSLRequest (length=8, code=80877103)
	if len(startupMsg) == 8 {
		code := binary.BigEndian.Uint32(startupMsg[4:8])
		if code == 80877103 {
			// Reject SSL — client will retry without it
			if _, err := client.Write([]byte{'N'}); err != nil {
				log.Printf("[%s] failed to send SSL rejection: %v", clientAddr, err)
				return
			}
			startupMsg, err = readRawMessage(client, true)
			if err != nil {
				log.Printf("[%s] failed to read startup after SSL: %v", clientAddr, err)
				return
			}
		}
	}

	// Step 2: Parse the startup message to extract user/database
	// We'll use these to log, but trust-auth the client
	user, database := parseStartupParams(startupMsg)
	log.Printf("[%s] startup: user=%s database=%s", clientAddr, user, database)

	// Step 3: Send AuthenticationOk + minimal server params + ReadyForQuery
	// This completes the client handshake without involving the backends.
	if err := sendAuthOk(client); err != nil {
		log.Printf("[%s] failed to send auth ok: %v", clientAddr, err)
		return
	}

	log.Printf("[%s] authenticated, entering query loop", clientAddr)

	// Step 4: Message loop — read queries, route, execute, relay responses
	var inTransaction bool
	var txFailed bool // tracks whether the transaction hit an error

	for {
		if ctx.Err() != nil {
			return
		}

		msg, err := readRawMessage(client, false)
		if err != nil {
			if err != io.EOF {
				log.Printf("[%s] client read error: %v", clientAddr, err)
			}
			return
		}

		if len(msg) == 0 {
			continue
		}

		msgType := msg[0]

		// Terminate — client disconnecting
		if msgType == 'X' {
			log.Printf("[%s] client disconnected", clientAddr)
			return
		}

		// We only handle simple query protocol ('Q') for now.
		// Extended protocol ('P','B','D','E','S') is more complex and
		// would need stateful tracking. Most tools (psql, simple apps)
		// use simple query protocol. pgx uses extended by default but
		// can be configured to use simple.
		if msgType == 'Q' {
			sql := extractSQL(msg)
			qtype := ClassifyQuery(sql)

			var useReplica bool

			switch {
			case qtype == QueryTransaction:
				upper := firstToken(sql)
				switch {
				case upper == "BEGIN" || upper == "START" || upper == "begin" || upper == "start":
					inTransaction = true
					txFailed = false
					log.Printf("[%s] BEGIN → primary (entering transaction)", clientAddr)
				default:
					inTransaction = false
					txFailed = false
					log.Printf("[%s] %s → primary (ending transaction)", clientAddr, upper)
				}

			case inTransaction:
				log.Printf("[%s] %s → primary (in transaction)", clientAddr, truncateSQL(sql))

			case qtype == QueryRead:
				useReplica = true
				log.Printf("[%s] %s → replica", clientAddr, truncateSQL(sql))

			default:
				log.Printf("[%s] %s → primary", clientAddr, truncateSQL(sql))
			}

			// Execute on the chosen backend
			var pool *pgxpool.Pool
			if useReplica {
				pool = p.replicaPool
			} else {
				pool = p.primaryPool
			}

			// Determine the correct ReadyForQuery status byte:
			//   'I' = idle (not in transaction)
			//   'T' = in transaction
			//   'E' = failed transaction (error occurred inside a BEGIN block)
			readyStatus := byte('I')
			if inTransaction {
				readyStatus = 'T'
			}

			if err := p.executeAndRelay(ctx, pool, sql, client, readyStatus); err != nil {
				log.Printf("[%s] query error: %v", clientAddr, err)
				sendError(client, err.Error())
				if inTransaction {
					txFailed = true
				}
				// Send the right status: 'E' if we're in a failed transaction
				errStatus := readyStatus
				if txFailed {
					errStatus = 'E'
				}
				sendReadyForQuery(client, errStatus)
				continue
			}
		} else {
			// For non-query messages, send an error
			sendError(client, "only simple query protocol is supported by the proxy")
			sendReadyForQuery(client, 'I')
		}
	}
}

// executeAndRelay executes a SQL query on a pool connection and sends
// the results back to the client in Postgres wire protocol format.
// readyStatus is the ReadyForQuery status byte to send after the response
// ('I' = idle, 'T' = in transaction, 'E' = failed transaction).
func (p *Proxy) executeAndRelay(ctx context.Context, pool *pgxpool.Pool, sql string, client net.Conn, readyStatus byte) error {
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Send RowDescription
	fields := rows.FieldDescriptions()
	if len(fields) > 0 {
		pgFields := make([]pgconn.FieldDescription, len(fields))
		for i, f := range fields {
			pgFields[i] = pgconn.FieldDescription{
				Name:                 f.Name,
				TableOID:             f.TableOID,
				TableAttributeNumber: f.TableAttributeNumber,
				DataTypeOID:          f.DataTypeOID,
				DataTypeSize:         f.DataTypeSize,
				TypeModifier:         f.TypeModifier,
				Format:               f.Format,
			}
		}
		if err := sendRowDescription(client, pgFields); err != nil {
			return err
		}

		// Send DataRows
		for rows.Next() {
			values := rows.RawValues()
			if err := sendDataRow(client, values); err != nil {
				return err
			}
		}

		if rows.Err() != nil {
			return rows.Err()
		}
	}

	// Send CommandComplete
	tag := rows.CommandTag().String()
	if tag == "" {
		tag = "SELECT 0"
	}
	if err := sendCommandComplete(client, tag); err != nil {
		return err
	}

	// Send ReadyForQuery with the correct transaction status
	return sendReadyForQuery(client, readyStatus)
}

// --- Wire protocol helpers ---

// parseStartupParams extracts user and database from the startup message.
func parseStartupParams(msg []byte) (user, database string) {
	if len(msg) < 8 {
		return "", ""
	}
	// Skip length (4) and protocol version (4)
	params := msg[8:]
	for len(params) > 1 {
		// Find key
		keyEnd := 0
		for i, b := range params {
			if b == 0 {
				keyEnd = i
				break
			}
		}
		key := string(params[:keyEnd])
		params = params[keyEnd+1:]

		// Find value
		valEnd := 0
		for i, b := range params {
			if b == 0 {
				valEnd = i
				break
			}
		}
		value := string(params[:valEnd])
		params = params[valEnd+1:]

		switch key {
		case "user":
			user = value
		case "database":
			database = value
		}
	}
	return
}

// sendAuthOk sends AuthenticationOk + BackendKeyData + ReadyForQuery to complete
// the client handshake.
func sendAuthOk(w io.Writer) error {
	// AuthenticationOk: 'R' + int32(8) + int32(0)
	authOk := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
	if _, err := w.Write(authOk); err != nil {
		return err
	}

	// ParameterStatus messages — pgx needs at least server_version and client_encoding
	for _, kv := range [][2]string{
		{"server_version", "16.0 (pgproxy)"},
		{"client_encoding", "UTF8"},
		{"server_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"},
		{"integer_datetimes", "on"},
		{"standard_conforming_strings", "on"},
	} {
		if err := sendParameterStatus(w, kv[0], kv[1]); err != nil {
			return err
		}
	}

	// BackendKeyData: 'K' + int32(12) + int32(pid) + int32(secret)
	keyData := []byte{'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 1}
	if _, err := w.Write(keyData); err != nil {
		return err
	}

	// ReadyForQuery
	return sendReadyForQuery(w, 'I')
}

func sendParameterStatus(w io.Writer, key, value string) error {
	// 'S' + int32(len) + key\0 + value\0
	payloadLen := 4 + len(key) + 1 + len(value) + 1
	msg := make([]byte, 1+payloadLen)
	msg[0] = 'S'
	binary.BigEndian.PutUint32(msg[1:5], uint32(payloadLen))
	pos := 5
	copy(msg[pos:], key)
	pos += len(key)
	msg[pos] = 0
	pos++
	copy(msg[pos:], value)
	pos += len(value)
	msg[pos] = 0
	_, err := w.Write(msg)
	return err
}

func sendReadyForQuery(w io.Writer, status byte) error {
	// 'Z' + int32(5) + byte(status)
	// status: 'I' = idle, 'T' = in transaction, 'E' = failed transaction
	msg := []byte{'Z', 0, 0, 0, 5, status}
	_, err := w.Write(msg)
	return err
}

func sendError(w io.Writer, errMsg string) error {
	// ErrorResponse: 'E' + int32(len) + fields...
	// Fields: 'S' severity, 'M' message, 0 terminator
	severity := "ERROR"
	payloadLen := 4 + 1 + len(severity) + 1 + 1 + len(errMsg) + 1 + 1
	msg := make([]byte, 1+payloadLen)
	msg[0] = 'E'
	binary.BigEndian.PutUint32(msg[1:5], uint32(payloadLen))
	pos := 5
	msg[pos] = 'S'
	pos++
	copy(msg[pos:], severity)
	pos += len(severity)
	msg[pos] = 0
	pos++
	msg[pos] = 'M'
	pos++
	copy(msg[pos:], errMsg)
	pos += len(errMsg)
	msg[pos] = 0
	pos++
	msg[pos] = 0 // terminator
	_, err := w.Write(msg)
	return err
}

func sendCommandComplete(w io.Writer, tag string) error {
	// 'C' + int32(len) + tag\0
	payloadLen := 4 + len(tag) + 1
	msg := make([]byte, 1+payloadLen)
	msg[0] = 'C'
	binary.BigEndian.PutUint32(msg[1:5], uint32(payloadLen))
	copy(msg[5:], tag)
	msg[5+len(tag)] = 0
	_, err := w.Write(msg)
	return err
}

func sendRowDescription(w io.Writer, fields []pgconn.FieldDescription) error {
	// RowDescription: 'T' + int32(len) + int16(numFields) + fields...
	// Each field: name\0 + int32(tableOID) + int16(colNum) + int32(typeOID)
	//           + int16(typeSize) + int32(typeMod) + int16(format)

	// Calculate total size
	size := 4 + 2 // length + field count
	for _, f := range fields {
		size += len(f.Name) + 1 + 4 + 2 + 4 + 2 + 4 + 2
	}

	msg := make([]byte, 1+size)
	msg[0] = 'T'
	binary.BigEndian.PutUint32(msg[1:5], uint32(size))
	binary.BigEndian.PutUint16(msg[5:7], uint16(len(fields)))

	pos := 7
	for _, f := range fields {
		copy(msg[pos:], f.Name)
		pos += len(f.Name)
		msg[pos] = 0
		pos++
		binary.BigEndian.PutUint32(msg[pos:pos+4], f.TableOID)
		pos += 4
		binary.BigEndian.PutUint16(msg[pos:pos+2], f.TableAttributeNumber)
		pos += 2
		binary.BigEndian.PutUint32(msg[pos:pos+4], f.DataTypeOID)
		pos += 4
		binary.BigEndian.PutUint16(msg[pos:pos+2], uint16(f.DataTypeSize))
		pos += 2
		binary.BigEndian.PutUint32(msg[pos:pos+4], uint32(f.TypeModifier))
		pos += 4
		binary.BigEndian.PutUint16(msg[pos:pos+2], uint16(f.Format))
		pos += 2
	}

	_, err := w.Write(msg)
	return err
}

func sendDataRow(w io.Writer, values [][]byte) error {
	// DataRow: 'D' + int32(len) + int16(numCols) + columns...
	// Each column: int32(len) + bytes  (len=-1 for NULL)

	size := 4 + 2 // length + column count
	for _, v := range values {
		if v == nil {
			size += 4 // NULL marker
		} else {
			size += 4 + len(v) // length + data
		}
	}

	msg := make([]byte, 1+size)
	msg[0] = 'D'
	binary.BigEndian.PutUint32(msg[1:5], uint32(size))
	binary.BigEndian.PutUint16(msg[5:7], uint16(len(values)))

	pos := 7
	for _, v := range values {
		if v == nil {
			binary.BigEndian.PutUint32(msg[pos:pos+4], 0xFFFFFFFF) // -1 for NULL
			pos += 4
		} else {
			binary.BigEndian.PutUint32(msg[pos:pos+4], uint32(len(v)))
			pos += 4
			copy(msg[pos:], v)
			pos += len(v)
		}
	}

	_, err := w.Write(msg)
	return err
}

// readRawMessage reads a single Postgres wire protocol message.
func readRawMessage(r io.Reader, isStartup bool) ([]byte, error) {
	if isStartup {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, lenBuf); err != nil {
			return nil, err
		}
		msgLen := int(binary.BigEndian.Uint32(lenBuf))
		if msgLen < 4 || msgLen > 10000 {
			return nil, fmt.Errorf("invalid startup message length: %d", msgLen)
		}
		msg := make([]byte, msgLen)
		copy(msg[:4], lenBuf)
		if _, err := io.ReadFull(r, msg[4:]); err != nil {
			return nil, err
		}
		return msg, nil
	}

	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	msgLen := int(binary.BigEndian.Uint32(header[1:5]))
	if msgLen < 4 {
		return nil, fmt.Errorf("invalid message length: %d", msgLen)
	}

	msg := make([]byte, 1+msgLen)
	copy(msg[:5], header)
	if msgLen > 4 {
		if _, err := io.ReadFull(r, msg[5:]); err != nil {
			return nil, err
		}
	}
	return msg, nil
}

// extractSQL extracts the SQL string from a Query ('Q') or Parse ('P') message.
func extractSQL(msg []byte) string {
	if len(msg) < 6 {
		return ""
	}

	switch msg[0] {
	case 'Q':
		payload := msg[5:]
		for i, b := range payload {
			if b == 0 {
				return string(payload[:i])
			}
		}
		return string(payload)

	case 'P':
		payload := msg[5:]
		nameEnd := 0
		for i, b := range payload {
			if b == 0 {
				nameEnd = i + 1
				break
			}
		}
		sqlPayload := payload[nameEnd:]
		for i, b := range sqlPayload {
			if b == 0 {
				return string(sqlPayload[:i])
			}
		}
		return string(sqlPayload)
	}

	return ""
}

// truncateSQL shortens a SQL string for logging.
func truncateSQL(sql string) string {
	if len(sql) > 60 {
		return sql[:57] + "..."
	}
	return sql
}

// ActiveConns returns the number of active client connections.
func (p *Proxy) ActiveConns() int64 {
	return p.activeConns.Load()
}

// Close shuts down the proxy listener.
func (p *Proxy) Close() error {
	if p.listener != nil {
		return p.listener.Close()
	}
	return nil
}
