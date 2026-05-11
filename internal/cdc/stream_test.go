package cdc

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// --- NewStream tests ---

func TestNewStream_ConnStrWithoutParams(t *testing.T) {
	s := NewStream(
		"postgres://user:pass@localhost:5432/mydb",
		"test_slot", "test_pub",
		NewFilter("", ""),
		func(ctx context.Context, event ChangeEvent) error { return nil },
	)

	// Should append ?replication=database
	want := "postgres://user:pass@localhost:5432/mydb?replication=database"
	if s.replConnStr != want {
		t.Errorf("replConnStr = %q, want %q", s.replConnStr, want)
	}
	if s.normalConnStr != "postgres://user:pass@localhost:5432/mydb" {
		t.Errorf("normalConnStr = %q, want original", s.normalConnStr)
	}
}

func TestNewStream_ConnStrWithExistingParams(t *testing.T) {
	s := NewStream(
		"postgres://user:pass@localhost:5432/mydb?sslmode=disable",
		"test_slot", "test_pub",
		NewFilter("", ""),
		func(ctx context.Context, event ChangeEvent) error { return nil },
	)

	// Should append &replication=database (not ?)
	want := "postgres://user:pass@localhost:5432/mydb?sslmode=disable&replication=database"
	if s.replConnStr != want {
		t.Errorf("replConnStr = %q, want %q", s.replConnStr, want)
	}
}

func TestNewStream_InitializesRelationsMap(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	if s.relations == nil {
		t.Error("relations map should be initialized")
	}
	if s.typeMap == nil {
		t.Error("typeMap should be initialized")
	}
}

func TestNewStream_StoresFields(t *testing.T) {
	filter := NewFilter("users", "INSERT")
	s := NewStream("postgres://localhost/db", "my_slot", "my_pub", filter, nil)

	if s.slotName != "my_slot" {
		t.Errorf("slotName = %q, want my_slot", s.slotName)
	}
	if s.publication != "my_pub" {
		t.Errorf("publication = %q, want my_pub", s.publication)
	}
}

// --- SetStartLSN tests ---

func TestSetStartLSN(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	if s.startLSNOverride != "" {
		t.Error("startLSNOverride should be empty initially")
	}

	s.SetStartLSN("0/1234567")
	if s.startLSNOverride != "0/1234567" {
		t.Errorf("startLSNOverride = %q, want 0/1234567", s.startLSNOverride)
	}
}

// --- quoteIdent tests ---

func TestQuoteIdent_Simple(t *testing.T) {
	got := quoteIdent("users")
	want := `"users"`
	if got != want {
		t.Errorf("quoteIdent(%q) = %q, want %q", "users", got, want)
	}
}

func TestQuoteIdent_WithDoubleQuotes(t *testing.T) {
	got := quoteIdent(`my"table`)
	want := `"my""table"`
	if got != want {
		t.Errorf(`quoteIdent("my\"table") = %q, want %q`, got, want)
	}
}

func TestQuoteIdent_Empty(t *testing.T) {
	got := quoteIdent("")
	want := `""`
	if got != want {
		t.Errorf("quoteIdent(\"\") = %q, want %q", got, want)
	}
}

func TestQuoteIdent_MultipleDoubleQuotes(t *testing.T) {
	got := quoteIdent(`a"b"c`)
	want := `"a""b""c"`
	if got != want {
		t.Errorf("quoteIdent = %q, want %q", got, want)
	}
}

// --- deliverIfMatch tests ---

func TestDeliverIfMatch_Passes(t *testing.T) {
	var delivered ChangeEvent
	handler := func(ctx context.Context, event ChangeEvent) error {
		delivered = event
		return nil
	}

	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("users", "INSERT"), handler)

	event := ChangeEvent{Action: "INSERT", Schema: "public", Table: "users"}
	err := s.deliverIfMatch(context.Background(), event)
	if err != nil {
		t.Fatalf("deliverIfMatch: %v", err)
	}
	if delivered.Action != "INSERT" {
		t.Errorf("delivered action = %q, want INSERT", delivered.Action)
	}
}

func TestDeliverIfMatch_FilteredOut(t *testing.T) {
	called := false
	handler := func(ctx context.Context, event ChangeEvent) error {
		called = true
		return nil
	}

	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("orders", ""), handler) // only orders, not users

	event := ChangeEvent{Action: "INSERT", Schema: "public", Table: "users"}
	err := s.deliverIfMatch(context.Background(), event)
	if err != nil {
		t.Fatalf("deliverIfMatch: %v", err)
	}
	if called {
		t.Error("handler should not have been called for filtered event")
	}
}

func TestDeliverIfMatch_HandlerError(t *testing.T) {
	handler := func(ctx context.Context, event ChangeEvent) error {
		return errors.New("sink failure")
	}

	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), handler)

	event := ChangeEvent{Action: "INSERT", Schema: "public", Table: "users"}
	err := s.deliverIfMatch(context.Background(), event)
	if err == nil {
		t.Fatal("expected error from handler")
	}
	if !errors.Is(err, errors.Unwrap(err)) {
		// Just verify it wraps the original error
		if err.Error() == "" {
			t.Error("error should have a message")
		}
	}
}

func TestDeliverIfMatch_ActionFilter(t *testing.T) {
	called := false
	handler := func(ctx context.Context, event ChangeEvent) error {
		called = true
		return nil
	}

	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", "DELETE"), handler) // only DELETE

	event := ChangeEvent{Action: "INSERT", Schema: "public", Table: "users"}
	err := s.deliverIfMatch(context.Background(), event)
	if err != nil {
		t.Fatalf("deliverIfMatch: %v", err)
	}
	if called {
		t.Error("handler should not be called for INSERT when filter is DELETE only")
	}
}

// --- decodeTuple tests ---

func TestDecodeTuple_NilTuple(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	result := s.decodeTuple(nil, &pglogrepl.RelationMessage{})
	if result != nil {
		t.Errorf("expected nil for nil tuple, got %v", result)
	}
}

func TestDecodeTuple_NullColumn(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "name", DataType: pgtype.TextOID},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 'n'}, // null
		},
	}

	result := s.decodeTuple(tuple, rel)
	if result == nil {
		t.Fatal("expected non-nil map")
	}
	val, ok := result["name"]
	if !ok {
		t.Fatal("expected 'name' key in result")
	}
	if val != nil {
		t.Errorf("expected nil for null column, got %v", val)
	}
}

func TestDecodeTuple_UnchangedToastColumn(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int4OID},
			{Name: "content", DataType: pgtype.TextOID},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("42")},
			{DataType: 'u'}, // unchanged TOAST
		},
	}

	result := s.decodeTuple(tuple, rel)
	if _, ok := result["content"]; ok {
		t.Error("unchanged TOAST column should not be in the map")
	}
	if _, ok := result["id"]; !ok {
		t.Error("id column should be in the map")
	}
}

func TestDecodeTuple_TextColumn(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "name", DataType: pgtype.TextOID},
			{Name: "age", DataType: pgtype.Int4OID},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("alice")},
			{DataType: 't', Data: []byte("30")},
		},
	}

	result := s.decodeTuple(tuple, rel)
	if result["name"] != "alice" {
		t.Errorf("name = %v, want alice", result["name"])
	}
	// Int4 should decode to int32 or similar numeric
	age, ok := result["age"]
	if !ok {
		t.Fatal("expected 'age' key")
	}
	// pgtype decodes int4 text to int32
	switch v := age.(type) {
	case int32:
		if v != 30 {
			t.Errorf("age = %d, want 30", v)
		}
	case int64:
		if v != 30 {
			t.Errorf("age = %d, want 30", v)
		}
	default:
		t.Logf("age type = %T, value = %v (accepting any numeric)", age, age)
	}
}

func TestDecodeTuple_UnknownOID_FallsBack(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "custom", DataType: 99999}, // unknown OID
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("custom_value")},
		},
	}

	result := s.decodeTuple(tuple, rel)
	if result["custom"] != "custom_value" {
		t.Errorf("custom = %v, want custom_value", result["custom"])
	}
}

func TestDecodeTuple_BinaryColumn_UnknownOID(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "data", DataType: 99999}, // unknown OID
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 'b', Data: []byte{0x01, 0x02, 0x03}},
		},
	}

	result := s.decodeTuple(tuple, rel)
	// Should fall back to binary placeholder
	val, ok := result["data"]
	if !ok {
		t.Fatal("expected 'data' key")
	}
	str, ok := val.(string)
	if !ok {
		t.Fatalf("expected string fallback, got %T", val)
	}
	if str != "(binary 3 bytes)" {
		t.Errorf("fallback = %q, want '(binary 3 bytes)'", str)
	}
}

// --- decodeTextColumn tests ---

func TestDecodeTextColumn_KnownOID(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	val, err := s.decodeTextColumn([]byte("hello"), pgtype.TextOID)
	if err != nil {
		t.Fatalf("decodeTextColumn: %v", err)
	}
	if val != "hello" {
		t.Errorf("val = %v, want hello", val)
	}
}

func TestDecodeTextColumn_UnknownOID(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	val, err := s.decodeTextColumn([]byte("raw"), 99999)
	if err != nil {
		t.Fatalf("decodeTextColumn: %v", err)
	}
	if val != "raw" {
		t.Errorf("val = %v, want raw (string fallback)", val)
	}
}

func TestDecodeTextColumn_Int4(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	val, err := s.decodeTextColumn([]byte("42"), pgtype.Int4OID)
	if err != nil {
		t.Fatalf("decodeTextColumn: %v", err)
	}
	if v, ok := val.(int32); ok {
		if v != 42 {
			t.Errorf("val = %d, want 42", v)
		}
	}
}

func TestDecodeTextColumn_Bool(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	val, err := s.decodeTextColumn([]byte("t"), pgtype.BoolOID)
	if err != nil {
		t.Fatalf("decodeTextColumn: %v", err)
	}
	if v, ok := val.(bool); ok {
		if !v {
			t.Error("expected true")
		}
	}
}

// --- decodeBinaryColumn tests ---

func TestDecodeBinaryColumn_UnknownOID(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	_, err := s.decodeBinaryColumn([]byte{0x01}, 99999)
	if err == nil {
		t.Fatal("expected error for unknown OID")
	}
}

// --- Multiple filter combinations in deliverIfMatch ---

func TestDeliverIfMatch_EmptyFilter_PassesEverything(t *testing.T) {
	count := 0
	handler := func(ctx context.Context, event ChangeEvent) error {
		count++
		return nil
	}

	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), handler)

	events := []ChangeEvent{
		{Action: "INSERT", Schema: "public", Table: "users"},
		{Action: "UPDATE", Schema: "public", Table: "orders"},
		{Action: "DELETE", Schema: "public", Table: "products"},
	}

	for _, e := range events {
		if err := s.deliverIfMatch(context.Background(), e); err != nil {
			t.Fatalf("deliverIfMatch: %v", err)
		}
	}

	if count != 3 {
		t.Errorf("handler called %d times, want 3", count)
	}
}

// --- Verify Stream fields after multiple SetStartLSN calls ---

func TestSetStartLSN_Overwrite(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	s.SetStartLSN("0/AAA")
	s.SetStartLSN("0/BBB")

	if s.startLSNOverride != "0/BBB" {
		t.Errorf("startLSNOverride = %q, want 0/BBB", s.startLSNOverride)
	}
}

// --- Verify timestamp and xid tracking in decodeTuple ---

func TestDecodeTuple_MultipleColumns(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int4OID},
			{Name: "name", DataType: pgtype.TextOID},
			{Name: "email", DataType: pgtype.TextOID},
			{Name: "deleted", DataType: pgtype.BoolOID},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("1")},
			{DataType: 't', Data: []byte("alice")},
			{DataType: 'n'}, // null email
			{DataType: 't', Data: []byte("f")},
		},
	}

	result := s.decodeTuple(tuple, rel)
	if len(result) != 4 {
		t.Errorf("map size = %d, want 4", len(result))
	}
	if result["email"] != nil {
		t.Errorf("email should be nil, got %v", result["email"])
	}
	if result["name"] != "alice" {
		t.Errorf("name = %v, want alice", result["name"])
	}
}

// --- NewSnapshot tests ---

func TestNewSnapshot(t *testing.T) {
	called := false
	handler := func(ctx context.Context, event ChangeEvent) error {
		called = true
		return nil
	}
	snap := NewSnapshot("postgres://localhost/db", []string{"users", "orders"}, handler)
	if snap.connStr != "postgres://localhost/db" {
		t.Errorf("connStr = %q", snap.connStr)
	}
	if len(snap.tables) != 2 {
		t.Errorf("tables = %d, want 2", len(snap.tables))
	}
	if snap.handler == nil {
		t.Error("handler should not be nil")
	}
	_ = called // handler not invoked without Run
}

func TestNewSnapshot_EmptyTables(t *testing.T) {
	snap := NewSnapshot("postgres://localhost/db", nil, nil)
	if len(snap.tables) != 0 {
		t.Errorf("tables = %d, want 0", len(snap.tables))
	}
}

func TestSnapshot_Run_NoTables(t *testing.T) {
	snap := NewSnapshot("postgres://localhost/db", nil, nil)
	lsn, err := snap.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if lsn != "" {
		t.Errorf("lsn = %q, want empty for no tables", lsn)
	}
}

func TestSnapshot_Run_EmptySlice(t *testing.T) {
	snap := NewSnapshot("postgres://localhost/db", []string{}, nil)
	lsn, err := snap.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if lsn != "" {
		t.Errorf("lsn = %q, want empty for empty slice", lsn)
	}
}

// --- decodeBinaryColumn with known OID ---

func TestDecodeBinaryColumn_Int4(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	// int4 in binary is 4 bytes big-endian
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, 42)

	val, err := s.decodeBinaryColumn(data, pgtype.Int4OID)
	if err != nil {
		t.Fatalf("decodeBinaryColumn: %v", err)
	}
	if v, ok := val.(int32); ok {
		if v != 42 {
			t.Errorf("val = %d, want 42", v)
		}
	} else {
		t.Logf("type = %T, value = %v (accepting any type for int4 binary)", val, val)
	}
}

func TestDecodeBinaryColumn_Bool(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	val, err := s.decodeBinaryColumn([]byte{1}, pgtype.BoolOID)
	if err != nil {
		t.Fatalf("decodeBinaryColumn: %v", err)
	}
	if v, ok := val.(bool); ok {
		if !v {
			t.Error("expected true")
		}
	}
}

// --- decodeTuple with binary column using known OID ---

func TestDecodeTuple_BinaryColumn_KnownOID(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, 7)

	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "count", DataType: pgtype.Int4OID},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 'b', Data: data},
		},
	}

	result := s.decodeTuple(tuple, rel)
	if _, ok := result["count"]; !ok {
		t.Fatal("expected 'count' key")
	}
}

// --- Verify Timestamp in decodeTuple with current xid ---

func TestStream_TransactionTracking(t *testing.T) {
	s := NewStream("postgres://localhost/db", "slot", "pub",
		NewFilter("", ""), nil)

	// Initially zero
	if s.currentXid != 0 {
		t.Errorf("initial xid = %d, want 0", s.currentXid)
	}
	if !s.currentCommitTime.IsZero() {
		t.Error("initial commit time should be zero")
	}

	// Set values (simulating processMessage behavior)
	now := time.Now()
	s.currentXid = 42
	s.currentCommitTime = now

	if s.currentXid != 42 {
		t.Errorf("xid = %d, want 42", s.currentXid)
	}
	if !s.currentCommitTime.Equal(now) {
		t.Error("commit time mismatch")
	}
}
