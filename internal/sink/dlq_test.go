package sink

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/debdutsaha/pgcdc/internal/cdc"
)

func TestDLQSink_WritesFailedEvent(t *testing.T) {
	// Create a temp file for the DLQ
	f, err := os.CreateTemp("", "pgcdc-dlq-*.jsonl")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	dlq, err := NewDLQSink(path)
	if err != nil {
		t.Fatalf("NewDLQSink: %v", err)
	}
	defer dlq.Close()

	handler := dlq.Handler()

	event := cdc.ChangeEvent{
		Action: "INSERT",
		Schema: "public",
		Table:  "users",
		New:    map[string]any{"id": float64(1), "name": "alice"},
		LSN:    "0/ABC123",
		Xid:    42,
	}
	deliveryErr := errors.New("webhook returned 503")

	// Handler should return nil (stream continues)
	err = handler(event, deliveryErr)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	// Close to flush
	dlq.Close()

	// Read back the DLQ file
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read DLQ file: %v", err)
	}

	var entry DLQEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("unmarshal DLQ entry: %v", err)
	}

	if entry.Event.Action != "INSERT" {
		t.Errorf("event action = %q, want INSERT", entry.Event.Action)
	}
	if entry.Event.Table != "users" {
		t.Errorf("event table = %q, want users", entry.Event.Table)
	}
	if entry.Error != "webhook returned 503" {
		t.Errorf("error = %q, want 'webhook returned 503'", entry.Error)
	}
	if entry.FailedAt.IsZero() {
		t.Error("FailedAt should not be zero")
	}
}

func TestDLQSink_MultipleEntries(t *testing.T) {
	f, err := os.CreateTemp("", "pgcdc-dlq-*.jsonl")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	dlq, err := NewDLQSink(path)
	if err != nil {
		t.Fatalf("NewDLQSink: %v", err)
	}

	handler := dlq.Handler()

	// Write 3 failed events
	for i := range 3 {
		event := cdc.ChangeEvent{
			Action:    "INSERT",
			Schema:    "public",
			Table:     "orders",
			Timestamp: time.Now(),
			Xid:       uint32(i),
		}
		if err := handler(event, errors.New("timeout")); err != nil {
			t.Fatalf("handler %d: %v", i, err)
		}
	}

	dlq.Close()

	// Read back — should have 3 lines
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read DLQ file: %v", err)
	}

	dec := json.NewDecoder(jsonReader(data))
	count := 0
	for dec.More() {
		var entry DLQEntry
		if err := dec.Decode(&entry); err != nil {
			t.Fatalf("decode entry %d: %v", count, err)
		}
		count++
	}

	if count != 3 {
		t.Errorf("DLQ entries = %d, want 3", count)
	}
}

func TestDLQSink_WithRetrySink(t *testing.T) {
	// Integration test: RetrySink + DLQ
	f, err := os.CreateTemp("", "pgcdc-dlq-*.jsonl")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	dlq, err := NewDLQSink(path)
	if err != nil {
		t.Fatalf("NewDLQSink: %v", err)
	}
	defer dlq.Close()

	// Inner sink that always fails
	inner := &mockSink{failUntil: 100}

	retry := NewRetrySink(inner,
		WithMaxAttempts(2),
		WithBaseDelay(1*time.Millisecond),
		WithOnFailure(dlq.Handler()),
	)

	event := cdc.ChangeEvent{
		Action: "DELETE",
		Schema: "public",
		Table:  "orders",
		Old:    map[string]any{"id": float64(99)},
		LSN:    "0/DEAD",
	}

	// Deliver should succeed (event goes to DLQ, not error returned)
	err = retry.Deliver(t.Context(), event)
	if err != nil {
		t.Fatalf("expected nil error (DLQ handled it), got: %v", err)
	}

	// Verify DLQ file has the entry
	dlq.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read DLQ file: %v", err)
	}

	var entry DLQEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if entry.Event.Action != "DELETE" {
		t.Errorf("DLQ event action = %q, want DELETE", entry.Event.Action)
	}
	if entry.Event.Table != "orders" {
		t.Errorf("DLQ event table = %q, want orders", entry.Event.Table)
	}
}

// jsonReader wraps a byte slice for json.NewDecoder.
func jsonReader(data []byte) *jsonBytesReader {
	return &jsonBytesReader{data: data, pos: 0}
}

type jsonBytesReader struct {
	data []byte
	pos  int
}

func (r *jsonBytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, os.ErrClosed
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
