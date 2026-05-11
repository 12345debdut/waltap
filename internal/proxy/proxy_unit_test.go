package proxy

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// --- extractSQL tests ---

func TestExtractSQL_QueryMessage(t *testing.T) {
	// Build a 'Q' message: 'Q' + int32(len) + "SELECT 1\0"
	sql := "SELECT 1"
	payloadLen := 4 + len(sql) + 1
	msg := make([]byte, 1+payloadLen)
	msg[0] = 'Q'
	binary.BigEndian.PutUint32(msg[1:5], uint32(payloadLen))
	copy(msg[5:], sql)
	msg[5+len(sql)] = 0

	got := extractSQL(msg)
	if got != sql {
		t.Errorf("extractSQL(Q) = %q, want %q", got, sql)
	}
}

func TestExtractSQL_ParseMessage(t *testing.T) {
	// Build a 'P' message: 'P' + int32(len) + name\0 + sql\0 + int16(nparams)
	stmtName := ""
	sql := "INSERT INTO users VALUES($1)"

	// payload: name\0 + sql\0 + int16(1)
	payloadLen := 4 + len(stmtName) + 1 + len(sql) + 1 + 2
	msg := make([]byte, 1+payloadLen)
	msg[0] = 'P'
	binary.BigEndian.PutUint32(msg[1:5], uint32(payloadLen))
	pos := 5
	copy(msg[pos:], stmtName)
	pos += len(stmtName)
	msg[pos] = 0
	pos++
	copy(msg[pos:], sql)
	pos += len(sql)
	msg[pos] = 0

	got := extractSQL(msg)
	if got != sql {
		t.Errorf("extractSQL(P) = %q, want %q", got, sql)
	}
}

func TestExtractSQL_ParseMessage_NamedStatement(t *testing.T) {
	stmtName := "my_stmt"
	sql := "SELECT * FROM orders WHERE id = $1"

	payloadLen := 4 + len(stmtName) + 1 + len(sql) + 1 + 2
	msg := make([]byte, 1+payloadLen)
	msg[0] = 'P'
	binary.BigEndian.PutUint32(msg[1:5], uint32(payloadLen))
	pos := 5
	copy(msg[pos:], stmtName)
	pos += len(stmtName)
	msg[pos] = 0
	pos++
	copy(msg[pos:], sql)
	pos += len(sql)
	msg[pos] = 0

	got := extractSQL(msg)
	if got != sql {
		t.Errorf("extractSQL(P named) = %q, want %q", got, sql)
	}
}

func TestExtractSQL_UnknownType(t *testing.T) {
	msg := []byte{'B', 0, 0, 0, 6, 0} // Bind message
	got := extractSQL(msg)
	if got != "" {
		t.Errorf("extractSQL(B) = %q, want empty", got)
	}
}

func TestExtractSQL_TooShort(t *testing.T) {
	msg := []byte{'Q', 0}
	got := extractSQL(msg)
	if got != "" {
		t.Errorf("extractSQL(short) = %q, want empty", got)
	}
}

// --- QueryType.String tests ---

func TestQueryType_String(t *testing.T) {
	tests := []struct {
		qt   QueryType
		want string
	}{
		{QueryRead, "READ"},
		{QueryWrite, "WRITE"},
		{QueryTransaction, "TXN"},
		{QueryType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		got := tt.qt.String()
		if got != tt.want {
			t.Errorf("QueryType(%d).String() = %q, want %q", tt.qt, got, tt.want)
		}
	}
}

// --- truncateSQL tests ---

func TestTruncateSQL_Short(t *testing.T) {
	got := truncateSQL("SELECT 1")
	if got != "SELECT 1" {
		t.Errorf("truncateSQL = %q, want 'SELECT 1'", got)
	}
}

func TestTruncateSQL_ExactlyAtLimit(t *testing.T) {
	sql := strings.Repeat("x", 60)
	got := truncateSQL(sql)
	if got != sql {
		t.Errorf("60-char string should not be truncated")
	}
}

func TestTruncateSQL_Long(t *testing.T) {
	sql := strings.Repeat("x", 100)
	got := truncateSQL(sql)
	if len(got) != 60 {
		t.Errorf("truncated length = %d, want 60", len(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Error("truncated string should end with '...'")
	}
}

// --- sendError tests ---

func TestSendError(t *testing.T) {
	var buf bytes.Buffer
	err := sendError(&buf, "something went wrong")
	if err != nil {
		t.Fatalf("sendError: %v", err)
	}

	msg := buf.Bytes()

	// First byte should be 'E' (ErrorResponse)
	if msg[0] != 'E' {
		t.Errorf("first byte = %c, want E", msg[0])
	}

	// Verify we can find the severity field 'S' and message field 'M'
	payload := string(msg[5:])
	if !strings.Contains(payload, "ERROR") {
		t.Error("error response should contain severity 'ERROR'")
	}
	if !strings.Contains(payload, "something went wrong") {
		t.Error("error response should contain the error message")
	}
}

// --- sendReadyForQuery tests ---

func TestSendReadyForQuery_Idle(t *testing.T) {
	var buf bytes.Buffer
	err := sendReadyForQuery(&buf, 'I')
	if err != nil {
		t.Fatalf("sendReadyForQuery: %v", err)
	}

	msg := buf.Bytes()
	if len(msg) != 6 {
		t.Fatalf("message length = %d, want 6", len(msg))
	}
	if msg[0] != 'Z' {
		t.Errorf("first byte = %c, want Z", msg[0])
	}
	if msg[5] != 'I' {
		t.Errorf("status = %c, want I", msg[5])
	}
}

func TestSendReadyForQuery_InTransaction(t *testing.T) {
	var buf bytes.Buffer
	sendReadyForQuery(&buf, 'T')
	if buf.Bytes()[5] != 'T' {
		t.Errorf("status = %c, want T", buf.Bytes()[5])
	}
}

func TestSendReadyForQuery_Failed(t *testing.T) {
	var buf bytes.Buffer
	sendReadyForQuery(&buf, 'E')
	if buf.Bytes()[5] != 'E' {
		t.Errorf("status = %c, want E", buf.Bytes()[5])
	}
}

// --- sendCommandComplete tests ---

func TestSendCommandComplete(t *testing.T) {
	var buf bytes.Buffer
	err := sendCommandComplete(&buf, "INSERT 0 1")
	if err != nil {
		t.Fatalf("sendCommandComplete: %v", err)
	}

	msg := buf.Bytes()
	if msg[0] != 'C' {
		t.Errorf("first byte = %c, want C", msg[0])
	}

	// Extract the tag from the message
	payload := msg[5:]
	tag := string(payload[:len(payload)-1]) // strip null terminator
	if tag != "INSERT 0 1" {
		t.Errorf("tag = %q, want 'INSERT 0 1'", tag)
	}
}

// --- sendParameterStatus tests ---

func TestSendParameterStatus(t *testing.T) {
	var buf bytes.Buffer
	err := sendParameterStatus(&buf, "server_version", "16.0")
	if err != nil {
		t.Fatalf("sendParameterStatus: %v", err)
	}

	msg := buf.Bytes()
	if msg[0] != 'S' {
		t.Errorf("first byte = %c, want S", msg[0])
	}

	payload := string(msg[5:])
	if !strings.Contains(payload, "server_version") {
		t.Error("should contain key")
	}
	if !strings.Contains(payload, "16.0") {
		t.Error("should contain value")
	}
}

// --- sendRowDescription tests ---

func TestSendRowDescription(t *testing.T) {
	var buf bytes.Buffer
	fields := []pgconn.FieldDescription{
		{
			Name:                 "id",
			TableOID:             0,
			TableAttributeNumber: 1,
			DataTypeOID:          23, // int4
			DataTypeSize:         4,
			TypeModifier:         -1,
			Format:               0,
		},
		{
			Name:                 "name",
			TableOID:             0,
			TableAttributeNumber: 2,
			DataTypeOID:          25, // text
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		},
	}

	err := sendRowDescription(&buf, fields)
	if err != nil {
		t.Fatalf("sendRowDescription: %v", err)
	}

	msg := buf.Bytes()
	if msg[0] != 'T' {
		t.Errorf("first byte = %c, want T", msg[0])
	}

	// Field count at offset 5-6
	fieldCount := binary.BigEndian.Uint16(msg[5:7])
	if fieldCount != 2 {
		t.Errorf("field count = %d, want 2", fieldCount)
	}
}

func TestSendRowDescription_Empty(t *testing.T) {
	var buf bytes.Buffer
	err := sendRowDescription(&buf, nil)
	if err != nil {
		t.Fatalf("sendRowDescription: %v", err)
	}

	msg := buf.Bytes()
	if msg[0] != 'T' {
		t.Errorf("first byte = %c, want T", msg[0])
	}
	fieldCount := binary.BigEndian.Uint16(msg[5:7])
	if fieldCount != 0 {
		t.Errorf("field count = %d, want 0", fieldCount)
	}
}

// --- sendDataRow tests ---

func TestSendDataRow(t *testing.T) {
	var buf bytes.Buffer
	values := [][]byte{
		[]byte("1"),
		[]byte("alice"),
		nil, // NULL
	}

	err := sendDataRow(&buf, values)
	if err != nil {
		t.Fatalf("sendDataRow: %v", err)
	}

	msg := buf.Bytes()
	if msg[0] != 'D' {
		t.Errorf("first byte = %c, want D", msg[0])
	}

	// Column count at offset 5-6
	colCount := binary.BigEndian.Uint16(msg[5:7])
	if colCount != 3 {
		t.Errorf("column count = %d, want 3", colCount)
	}

	// Parse column values
	pos := 7

	// Column 0: "1"
	col0Len := int32(binary.BigEndian.Uint32(msg[pos : pos+4]))
	pos += 4
	if col0Len != 1 {
		t.Errorf("col0 len = %d, want 1", col0Len)
	}
	if string(msg[pos:pos+int(col0Len)]) != "1" {
		t.Errorf("col0 = %q, want 1", string(msg[pos:pos+int(col0Len)]))
	}
	pos += int(col0Len)

	// Column 1: "alice"
	col1Len := int32(binary.BigEndian.Uint32(msg[pos : pos+4]))
	pos += 4
	if col1Len != 5 {
		t.Errorf("col1 len = %d, want 5", col1Len)
	}
	if string(msg[pos:pos+int(col1Len)]) != "alice" {
		t.Errorf("col1 = %q, want alice", string(msg[pos:pos+int(col1Len)]))
	}
	pos += int(col1Len)

	// Column 2: NULL (-1)
	col2Len := int32(binary.BigEndian.Uint32(msg[pos : pos+4]))
	if col2Len != -1 {
		t.Errorf("col2 len = %d, want -1 (NULL)", col2Len)
	}
}

func TestSendDataRow_Empty(t *testing.T) {
	var buf bytes.Buffer
	err := sendDataRow(&buf, nil)
	if err != nil {
		t.Fatalf("sendDataRow: %v", err)
	}

	msg := buf.Bytes()
	if msg[0] != 'D' {
		t.Errorf("first byte = %c, want D", msg[0])
	}
	colCount := binary.BigEndian.Uint16(msg[5:7])
	if colCount != 0 {
		t.Errorf("column count = %d, want 0", colCount)
	}
}

// --- sendAuthOk tests ---

func TestSendAuthOk(t *testing.T) {
	var buf bytes.Buffer
	err := sendAuthOk(&buf)
	if err != nil {
		t.Fatalf("sendAuthOk: %v", err)
	}

	data := buf.Bytes()

	// Should start with AuthenticationOk: 'R' + int32(8) + int32(0)
	if data[0] != 'R' {
		t.Errorf("first byte = %c, want R", data[0])
	}

	// Should end with ReadyForQuery 'Z'
	// Find the last 'Z' in the output
	found := false
	for i := len(data) - 6; i >= 0; i-- {
		if data[i] == 'Z' {
			found = true
			if data[i+5] != 'I' {
				t.Errorf("ReadyForQuery status = %c, want I", data[i+5])
			}
			break
		}
	}
	if !found {
		t.Error("no ReadyForQuery message found")
	}

	// Should contain parameter status messages
	payload := string(data)
	if !strings.Contains(payload, "server_version") {
		t.Error("should contain server_version parameter")
	}
	if !strings.Contains(payload, "client_encoding") {
		t.Error("should contain client_encoding parameter")
	}
}

// --- parseStartupParams tests ---

func TestParseStartupParams(t *testing.T) {
	msg := buildStartupMessage("testuser", "testdb")
	user, database := parseStartupParams(msg)

	if user != "testuser" {
		t.Errorf("user = %q, want testuser", user)
	}
	if database != "testdb" {
		t.Errorf("database = %q, want testdb", database)
	}
}

func TestParseStartupParams_Empty(t *testing.T) {
	user, database := parseStartupParams(nil)
	if user != "" || database != "" {
		t.Errorf("expected empty for nil, got user=%q database=%q", user, database)
	}
}

func TestParseStartupParams_TooShort(t *testing.T) {
	user, database := parseStartupParams([]byte{0, 0, 0, 4})
	if user != "" || database != "" {
		t.Errorf("expected empty for short msg, got user=%q database=%q", user, database)
	}
}

// --- readRawMessage tests ---

func TestReadRawMessage_Startup(t *testing.T) {
	startup := buildStartupMessage("user", "db")
	reader := bytes.NewReader(startup)

	msg, err := readRawMessage(reader, true)
	if err != nil {
		t.Fatalf("readRawMessage: %v", err)
	}

	// The message should equal our startup message
	if !bytes.Equal(msg, startup) {
		t.Error("startup message mismatch")
	}
}

func TestReadRawMessage_Regular(t *testing.T) {
	// Build a Query message
	sql := "SELECT 1"
	payloadLen := 4 + len(sql) + 1
	original := make([]byte, 1+payloadLen)
	original[0] = 'Q'
	binary.BigEndian.PutUint32(original[1:5], uint32(payloadLen))
	copy(original[5:], sql)
	original[5+len(sql)] = 0

	reader := bytes.NewReader(original)
	msg, err := readRawMessage(reader, false)
	if err != nil {
		t.Fatalf("readRawMessage: %v", err)
	}

	if !bytes.Equal(msg, original) {
		t.Error("regular message mismatch")
	}
	if msg[0] != 'Q' {
		t.Errorf("msg type = %c, want Q", msg[0])
	}
}

func TestReadRawMessage_EOF(t *testing.T) {
	reader := bytes.NewReader(nil)
	_, err := readRawMessage(reader, false)
	if err == nil {
		t.Fatal("expected error for empty reader")
	}
}

func TestReadRawMessage_InvalidLength(t *testing.T) {
	// Startup with crazy length
	msg := make([]byte, 4)
	binary.BigEndian.PutUint32(msg, 50000)
	reader := bytes.NewReader(msg)

	_, err := readRawMessage(reader, true)
	if err == nil {
		t.Fatal("expected error for invalid length")
	}
}

func TestReadRawMessage_RegularMinLength(t *testing.T) {
	// Message with length < 4
	msg := []byte{'Q', 0, 0, 0, 3} // length = 3, which is invalid
	reader := bytes.NewReader(msg)

	_, err := readRawMessage(reader, false)
	if err == nil {
		t.Fatal("expected error for length < 4")
	}
}

func TestReadRawMessage_RegularExactMinLength(t *testing.T) {
	// Message with length = 4 (just header, no body) — should succeed
	msg := []byte{'Q', 0, 0, 0, 4}
	reader := bytes.NewReader(msg)

	result, err := readRawMessage(reader, false)
	if err != nil {
		t.Fatalf("readRawMessage: %v", err)
	}
	if len(result) != 5 {
		t.Errorf("result length = %d, want 5", len(result))
	}
}

// --- readRawMessage edge cases ---

func TestReadRawMessage_StartupTooSmall(t *testing.T) {
	// Startup with length < 4 (invalid)
	msg := make([]byte, 4)
	binary.BigEndian.PutUint32(msg, 3) // length = 3
	reader := bytes.NewReader(msg)

	_, err := readRawMessage(reader, true)
	if err == nil {
		t.Fatal("expected error for length < 4")
	}
}

func TestReadRawMessage_StartupExactMinLength(t *testing.T) {
	// Startup with length = 4 (just the length field, no body)
	msg := make([]byte, 4)
	binary.BigEndian.PutUint32(msg, 4)
	reader := bytes.NewReader(msg)

	result, err := readRawMessage(reader, true)
	if err != nil {
		t.Fatalf("readRawMessage: %v", err)
	}
	if len(result) != 4 {
		t.Errorf("result length = %d, want 4", len(result))
	}
}

func TestReadRawMessage_MultipleMessages(t *testing.T) {
	// Two query messages back to back
	sql1 := "SELECT 1"
	sql2 := "SELECT 2"

	var buf bytes.Buffer
	for _, sql := range []string{sql1, sql2} {
		payloadLen := 4 + len(sql) + 1
		msg := make([]byte, 1+payloadLen)
		msg[0] = 'Q'
		binary.BigEndian.PutUint32(msg[1:5], uint32(payloadLen))
		copy(msg[5:], sql)
		msg[5+len(sql)] = 0
		buf.Write(msg)
	}

	reader := bytes.NewReader(buf.Bytes())

	msg1, err := readRawMessage(reader, false)
	if err != nil {
		t.Fatalf("first message: %v", err)
	}
	if extractSQL(msg1) != sql1 {
		t.Errorf("first SQL = %q, want %q", extractSQL(msg1), sql1)
	}

	msg2, err := readRawMessage(reader, false)
	if err != nil {
		t.Fatalf("second message: %v", err)
	}
	if extractSQL(msg2) != sql2 {
		t.Errorf("second SQL = %q, want %q", extractSQL(msg2), sql2)
	}
}

// --- extractSQL edge cases ---

func TestExtractSQL_QueryNoNullTerminator(t *testing.T) {
	// Q message without a null terminator at the end
	sql := "SELECT 1"
	payloadLen := 4 + len(sql) // no +1 for null
	msg := make([]byte, 1+payloadLen)
	msg[0] = 'Q'
	binary.BigEndian.PutUint32(msg[1:5], uint32(payloadLen))
	copy(msg[5:], sql)

	got := extractSQL(msg)
	if got != sql {
		t.Errorf("extractSQL(no null) = %q, want %q", got, sql)
	}
}

func TestExtractSQL_EmptyQuery(t *testing.T) {
	// Q message with just a null byte
	msg := []byte{'Q', 0, 0, 0, 5, 0}
	got := extractSQL(msg)
	if got != "" {
		t.Errorf("extractSQL(empty) = %q, want empty", got)
	}
}

// --- sendAuthOk with failing writer ---

type failWriter struct {
	failAfter int
	written   int
}

func (f *failWriter) Write(p []byte) (n int, err error) {
	if f.written+len(p) > f.failAfter {
		remaining := f.failAfter - f.written
		if remaining <= 0 {
			return 0, io.ErrShortWrite
		}
		f.written += remaining
		return remaining, io.ErrShortWrite
	}
	f.written += len(p)
	return len(p), nil
}

func TestSendAuthOk_WriterFails(t *testing.T) {
	// Writer that fails after a few bytes
	w := &failWriter{failAfter: 5}
	err := sendAuthOk(w)
	if err == nil {
		t.Fatal("expected error from failing writer")
	}
}

func TestSendError_WriterFails(t *testing.T) {
	w := &failWriter{failAfter: 0}
	err := sendError(w, "test error")
	if err == nil {
		t.Fatal("expected error from failing writer")
	}
}

func TestSendReadyForQuery_WriterFails(t *testing.T) {
	w := &failWriter{failAfter: 0}
	err := sendReadyForQuery(w, 'I')
	if err == nil {
		t.Fatal("expected error from failing writer")
	}
}

func TestSendCommandComplete_WriterFails(t *testing.T) {
	w := &failWriter{failAfter: 0}
	err := sendCommandComplete(w, "SELECT 1")
	if err == nil {
		t.Fatal("expected error from failing writer")
	}
}

func TestSendParameterStatus_WriterFails(t *testing.T) {
	w := &failWriter{failAfter: 0}
	err := sendParameterStatus(w, "key", "value")
	if err == nil {
		t.Fatal("expected error from failing writer")
	}
}

func TestSendRowDescription_WriterFails(t *testing.T) {
	w := &failWriter{failAfter: 0}
	fields := []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1},
	}
	err := sendRowDescription(w, fields)
	if err == nil {
		t.Fatal("expected error from failing writer")
	}
}

func TestSendDataRow_WriterFails(t *testing.T) {
	w := &failWriter{failAfter: 0}
	err := sendDataRow(w, [][]byte{[]byte("test")})
	if err == nil {
		t.Fatal("expected error from failing writer")
	}
}

// --- ActiveConns tests ---

func TestProxy_ActiveConns(t *testing.T) {
	p := NewProxy(":0", "", "")
	if got := p.ActiveConns(); got != 0 {
		t.Errorf("ActiveConns() = %d, want 0", got)
	}

	p.activeConns.Add(5)
	if got := p.ActiveConns(); got != 5 {
		t.Errorf("ActiveConns() = %d, want 5", got)
	}

	p.activeConns.Add(-3)
	if got := p.ActiveConns(); got != 2 {
		t.Errorf("ActiveConns() = %d, want 2", got)
	}
}

// --- Close tests ---

func TestProxy_Close_NoListener(t *testing.T) {
	p := NewProxy(":0", "", "")
	err := p.Close()
	if err != nil {
		t.Errorf("Close without listener: %v", err)
	}
}

func TestProxy_Close_WithListener(t *testing.T) {
	// Use a pipe to simulate — we just need something closeable
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()

	// The listener field needs to be a net.Listener.
	// Let's use a real listener on port 0.
	ln, err := listenOnFreePort()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	p := NewProxy(":0", "", "")
	p.listener = ln

	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	// Closing again should error (already closed)
	if err := p.Close(); err == nil {
		t.Log("second close returned nil (acceptable)")
	}
}

// --- NewProxy tests ---

func TestNewProxy(t *testing.T) {
	p := NewProxy(":5500", "postgres://primary", "postgres://replica")
	if p.listenAddr != ":5500" {
		t.Errorf("listenAddr = %q, want :5500", p.listenAddr)
	}
	if p.primaryDSN != "postgres://primary" {
		t.Errorf("primaryDSN = %q", p.primaryDSN)
	}
	if p.replicaDSN != "postgres://replica" {
		t.Errorf("replicaDSN = %q", p.replicaDSN)
	}
}

// --- firstToken tests ---

func TestFirstToken(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"SELECT * FROM users", "SELECT"},
		{"select 1", "select"},
		{"BEGIN", "BEGIN"},
		{"INSERT INTO users", "INSERT"},
		{"  SELECT 1", ""},    // leading space → empty first token
		{"FUNC(arg)", "FUNC"}, // parenthesis stops the token
		{"", ""},
	}

	for _, tt := range tests {
		got := firstToken(tt.input)
		if got != tt.want {
			t.Errorf("firstToken(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// --- stripLeadingComments tests ---

func TestStripLeadingComments(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			"no comments",
			"SELECT 1",
			"SELECT 1",
		},
		{
			"line comment",
			"-- this is a comment\nSELECT 1",
			"SELECT 1",
		},
		{
			"block comment",
			"/* comment */ SELECT 1",
			"SELECT 1",
		},
		{
			"nested block comment",
			"/* outer /* inner */ still */ SELECT 1",
			"SELECT 1",
		},
		{
			"multiple line comments",
			"-- first\n-- second\nSELECT 1",
			"SELECT 1",
		},
		{
			"only comment",
			"-- just a comment",
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripLeadingComments(tt.input)
			got = strings.TrimSpace(got)
			if got != tt.want {
				t.Errorf("stripLeadingComments = %q, want %q", got, tt.want)
			}
		})
	}
}

// helper

func listenOnFreePort() (*fakeListener, error) {
	return &fakeListener{closed: false}, nil
}

type fakeListener struct {
	closed bool
}

func (f *fakeListener) Accept() (net.Conn, error) {
	return nil, io.EOF
}

func (f *fakeListener) Close() error {
	if f.closed {
		return io.ErrClosedPipe
	}
	f.closed = true
	return nil
}

func (f *fakeListener) Addr() net.Addr {
	return fakeAddr{}
}

type fakeAddr struct{}

func (fakeAddr) Network() string { return "tcp" }
func (fakeAddr) String() string  { return "127.0.0.1:0" }
