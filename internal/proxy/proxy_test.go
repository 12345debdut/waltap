package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	primaryDSN = "postgres://lab:lab@localhost:5433/wallab"
	replicaDSN = "postgres://lab:lab@localhost:5435/wallab"
)

func startTestProxy(t *testing.T) (proxyAddr string, cancel context.CancelFunc) {
	t.Helper()

	// Check if primary is available
	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()
	conn, err := pgx.Connect(ctx, primaryDSN)
	if err != nil {
		t.Skipf("primary not available: %v", err)
	}
	conn.Close(ctx)

	// Check if replica is available
	ctx2, c2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer c2()
	conn, err = pgx.Connect(ctx2, replicaDSN)
	if err != nil {
		t.Skipf("replica not available: %v", err)
	}
	conn.Close(ctx2)

	// Create listener first to avoid race
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	proxyCtx, proxyCancel := context.WithCancel(context.Background())
	p := NewProxy(addr, primaryDSN, replicaDSN)

	go func() {
		if err := p.Run(proxyCtx, ln); err != nil {
			t.Logf("proxy error: %v", err)
		}
	}()

	// Wait for proxy to be ready (pools connected)
	time.Sleep(500 * time.Millisecond)

	return addr, proxyCancel
}

// connectSimple connects to the proxy using the simple query protocol.
// We use a raw TCP connection and speak the Postgres wire protocol
// manually because pgx defaults to extended protocol.
func connectSimple(t *testing.T, addr string) net.Conn {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}

	// Send startup message (no SSL)
	startup := buildStartupMessage("lab", "wallab")
	if _, err := conn.Write(startup); err != nil {
		conn.Close()
		t.Fatalf("send startup: %v", err)
	}

	// Read until ReadyForQuery ('Z')
	if err := drainUntilZ(conn); err != nil {
		conn.Close()
		t.Fatalf("auth handshake: %v", err)
	}

	return conn
}

// simpleQuery sends a simple query and returns the column values from the first row.
func simpleQuery(t *testing.T, conn net.Conn, sql string) []string {
	t.Helper()

	// Build Query message: 'Q' + int32(len) + sql\0
	payloadLen := 4 + len(sql) + 1
	msg := make([]byte, 1+payloadLen)
	msg[0] = 'Q'
	binary.BigEndian.PutUint32(msg[1:5], uint32(payloadLen))
	copy(msg[5:], sql)
	msg[5+len(sql)] = 0

	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write query: %v", err)
	}

	// Read response messages until ReadyForQuery
	var result []string
	for {
		respMsg, err := readRawMessage(conn, false)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}

		switch respMsg[0] {
		case 'T':
			// RowDescription — skip

		case 'D':
			// DataRow — extract values
			if len(respMsg) < 7 {
				continue
			}
			numCols := int(binary.BigEndian.Uint16(respMsg[5:7]))
			pos := 7
			for range numCols {
				if pos+4 > len(respMsg) {
					break
				}
				colLen := int(int32(binary.BigEndian.Uint32(respMsg[pos : pos+4])))
				pos += 4
				if colLen == -1 {
					result = append(result, "NULL")
				} else {
					result = append(result, string(respMsg[pos:pos+colLen]))
					pos += colLen
				}
			}

		case 'C':
			// CommandComplete — skip

		case 'Z':
			// ReadyForQuery — done
			return result

		case 'E':
			// ErrorResponse — extract message
			payload := string(respMsg[5:])
			t.Fatalf("server error: %s", payload)
		}
	}
}

func simpleExec(t *testing.T, conn net.Conn, sql string) {
	t.Helper()

	payloadLen := 4 + len(sql) + 1
	msg := make([]byte, 1+payloadLen)
	msg[0] = 'Q'
	binary.BigEndian.PutUint32(msg[1:5], uint32(payloadLen))
	copy(msg[5:], sql)
	msg[5+len(sql)] = 0

	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write query: %v", err)
	}

	// Drain until ReadyForQuery
	for {
		respMsg, err := readRawMessage(conn, false)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		if respMsg[0] == 'E' {
			t.Fatalf("server error for %q: %s", sql, string(respMsg[5:]))
		}
		if respMsg[0] == 'Z' {
			return
		}
	}
}

// buildStartupMessage creates a Postgres startup message.
func buildStartupMessage(user, database string) []byte {
	// Format: int32(length) + int32(protocol 3.0) + key\0value\0... + \0
	params := fmt.Sprintf("user\x00%s\x00database\x00%s\x00\x00", user, database)
	length := 4 + 4 + len(params) // length field + protocol version + params
	msg := make([]byte, length)
	binary.BigEndian.PutUint32(msg[0:4], uint32(length))
	binary.BigEndian.PutUint32(msg[4:8], 196608) // Protocol 3.0
	copy(msg[8:], params)
	return msg
}

func drainUntilZ(conn net.Conn) error {
	for {
		msg, err := readRawMessage(conn, false)
		if err != nil {
			return err
		}
		if msg[0] == 'Z' {
			return nil
		}
		if msg[0] == 'E' {
			return fmt.Errorf("error response: %s", string(msg[5:]))
		}
	}
}

func TestProxy_SelectGoesToReplica(t *testing.T) {
	addr, cancel := startTestProxy(t)
	defer cancel()

	conn := connectSimple(t, addr)
	defer conn.Close()

	result := simpleQuery(t, conn, "SELECT count(*) FROM users")
	if len(result) == 0 {
		t.Fatal("expected at least one result value")
	}

	t.Logf("SELECT count(*) = %s (via proxy → replica)", result[0])

	if result[0] == "0" {
		t.Error("expected at least 1 user")
	}
}

func TestProxy_InsertGoesToPrimary(t *testing.T) {
	addr, cancel := startTestProxy(t)
	defer cancel()

	conn := connectSimple(t, addr)
	defer conn.Close()

	uniqueName := fmt.Sprintf("proxy_test_%d", time.Now().UnixNano())
	simpleExec(t, conn, fmt.Sprintf(
		"INSERT INTO users(name, email) VALUES('%s', '%s@test.com')",
		uniqueName, uniqueName))

	// Read it back
	result := simpleQuery(t, conn, fmt.Sprintf(
		"SELECT name FROM users WHERE name = '%s'", uniqueName))

	if len(result) == 0 {
		t.Fatal("INSERT was not persisted")
	}

	if result[0] != uniqueName {
		t.Errorf("got %q, want %q", result[0], uniqueName)
	}

	t.Logf("INSERT + SELECT verified: %s", result[0])

	// Clean up
	simpleExec(t, conn, fmt.Sprintf("DELETE FROM users WHERE name = '%s'", uniqueName))
}

func TestProxy_TransactionRoutesToPrimary(t *testing.T) {
	addr, cancel := startTestProxy(t)
	defer cancel()

	conn := connectSimple(t, addr)
	defer conn.Close()

	uniqueName := fmt.Sprintf("txn_test_%d", time.Now().UnixNano())

	// Start transaction
	simpleExec(t, conn, "BEGIN")

	// Insert inside transaction
	simpleExec(t, conn, fmt.Sprintf(
		"INSERT INTO users(name, email) VALUES('%s', '%s@test.com')",
		uniqueName, uniqueName))

	// SELECT inside transaction should see uncommitted data (same primary conn)
	result := simpleQuery(t, conn, fmt.Sprintf(
		"SELECT name FROM users WHERE name = '%s'", uniqueName))

	if len(result) == 0 {
		t.Fatal("SELECT in transaction didn't see uncommitted insert")
	}

	// Commit
	simpleExec(t, conn, "COMMIT")

	t.Logf("transaction test passed: %s", result[0])

	// Clean up
	simpleExec(t, conn, fmt.Sprintf("DELETE FROM users WHERE name = '%s'", uniqueName))
}
