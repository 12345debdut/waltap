package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/debdutsaha/pgcdc/internal/cdc"
)

// --- StdoutSink tests ---

func TestStdoutSink_Deliver(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	s := &StdoutSink{encoder: json.NewEncoder(w)}
	event := cdc.ChangeEvent{
		Action: "INSERT",
		Schema: "public",
		Table:  "users",
		New:    map[string]any{"id": float64(1), "name": "alice"},
		LSN:    "0/ABC",
		Xid:    42,
	}

	err := s.Deliver(context.Background(), event)
	w.Close()

	os.Stdout = oldStdout

	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	output, _ := io.ReadAll(r)
	var decoded cdc.ChangeEvent
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("unmarshal output: %v (raw: %s)", err, string(output))
	}

	if decoded.Action != "INSERT" {
		t.Errorf("Action = %q, want INSERT", decoded.Action)
	}
	if decoded.Table != "users" {
		t.Errorf("Table = %q, want users", decoded.Table)
	}
}

func TestStdoutSink_Close(t *testing.T) {
	s := NewStdoutSink()
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// --- WebhookSink tests ---

func TestWebhookSink_Deliver_Success(t *testing.T) {
	var receivedBody []byte
	var receivedHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := NewWebhookSink(server.URL)
	event := cdc.ChangeEvent{
		Action: "UPDATE",
		Schema: "public",
		Table:  "orders",
		New:    map[string]any{"id": float64(5), "status": "shipped"},
		LSN:    "0/DEF",
	}

	err := s.Deliver(context.Background(), event)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	// Verify Content-Type header
	if ct := receivedHeaders.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	// Verify custom headers
	if action := receivedHeaders.Get("X-PGCDC-Action"); action != "UPDATE" {
		t.Errorf("X-PGCDC-Action = %q, want UPDATE", action)
	}
	if tbl := receivedHeaders.Get("X-PGCDC-Table"); tbl != "public.orders" {
		t.Errorf("X-PGCDC-Table = %q, want public.orders", tbl)
	}

	// Verify body
	var decoded cdc.ChangeEvent
	if err := json.Unmarshal(receivedBody, &decoded); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if decoded.Action != "UPDATE" {
		t.Errorf("body action = %q, want UPDATE", decoded.Action)
	}
}

func TestWebhookSink_Deliver_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	s := NewWebhookSink(server.URL)
	event := cdc.ChangeEvent{Action: "INSERT", Schema: "public", Table: "users"}

	err := s.Deliver(context.Background(), event)
	if err == nil {
		t.Fatal("expected error for 503 response")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error should mention status code: %v", err)
	}
}

func TestWebhookSink_Deliver_ConnectionRefused(t *testing.T) {
	s := NewWebhookSink("http://127.0.0.1:1") // nothing listening
	event := cdc.ChangeEvent{Action: "INSERT", Schema: "public", Table: "users"}

	err := s.Deliver(context.Background(), event)
	if err == nil {
		t.Fatal("expected error for refused connection")
	}
}

func TestWebhookSink_Close(t *testing.T) {
	s := NewWebhookSink("http://example.com")
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// --- WebhookBatchSink tests ---

func TestWebhookBatchSink_DeliverBatch_Success(t *testing.T) {
	var receivedBody []byte
	var receivedHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := NewWebhookBatchSink(server.URL)
	events := []cdc.ChangeEvent{
		{Action: "INSERT", Schema: "public", Table: "users", New: map[string]any{"id": float64(1)}},
		{Action: "UPDATE", Schema: "public", Table: "users", New: map[string]any{"id": float64(2)}},
		{Action: "DELETE", Schema: "public", Table: "users", Old: map[string]any{"id": float64(3)}},
	}

	err := s.DeliverBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("DeliverBatch: %v", err)
	}

	// Verify batch size header
	if bs := receivedHeaders.Get("X-PGCDC-Batch-Size"); bs != "3" {
		t.Errorf("X-PGCDC-Batch-Size = %q, want 3", bs)
	}

	// Verify body is a JSON array
	var decoded []cdc.ChangeEvent
	if err := json.Unmarshal(receivedBody, &decoded); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if len(decoded) != 3 {
		t.Errorf("batch size = %d, want 3", len(decoded))
	}
}

func TestWebhookBatchSink_DeliverBatch_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	s := NewWebhookBatchSink(server.URL)
	events := []cdc.ChangeEvent{
		{Action: "INSERT", Schema: "public", Table: "users"},
	}

	err := s.DeliverBatch(context.Background(), events)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should mention status code: %v", err)
	}
}

func TestWebhookBatchSink_Close(t *testing.T) {
	s := NewWebhookBatchSink("http://example.com")
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// --- StdoutBatchSink tests ---

func TestStdoutBatchSink_DeliverBatch(t *testing.T) {
	// Redirect stdout to a pipe
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()
	os.Stdout = w
	os.Stderr = wErr

	s := &StdoutBatchSink{encoder: json.NewEncoder(w)}
	events := []cdc.ChangeEvent{
		{Action: "INSERT", Schema: "public", Table: "users"},
		{Action: "DELETE", Schema: "public", Table: "orders"},
	}

	err := s.DeliverBatch(context.Background(), events)
	w.Close()
	wErr.Close()
	os.Stdout = oldStdout
	os.Stderr = oldStderr

	if err != nil {
		t.Fatalf("DeliverBatch: %v", err)
	}

	output, _ := io.ReadAll(r)
	io.ReadAll(rErr) // drain stderr

	var batch batchOutput
	if err := json.Unmarshal(output, &batch); err != nil {
		t.Fatalf("unmarshal: %v (raw: %s)", err, string(output))
	}
	if batch.BatchSize != 2 {
		t.Errorf("BatchSize = %d, want 2", batch.BatchSize)
	}
	if len(batch.Events) != 2 {
		t.Errorf("Events count = %d, want 2", len(batch.Events))
	}
}

func TestStdoutBatchSink_Close(t *testing.T) {
	s := NewStdoutBatchSink()
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// --- MetricsSink tests ---

func TestMetricsSink_Deliver_Success(t *testing.T) {
	inner := &mockSink{failUntil: 0}
	s := NewMetricsSink(inner, "test")

	event := cdc.ChangeEvent{
		Action:    "INSERT",
		Schema:    "public",
		Table:     "users",
		Timestamp: time.Now().Add(-100 * time.Millisecond),
		LSN:       "0/1",
	}

	err := s.Deliver(context.Background(), event)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if inner.deliverCount.Load() != 1 {
		t.Errorf("inner deliver count = %d, want 1", inner.deliverCount.Load())
	}
}

func TestMetricsSink_Deliver_Error(t *testing.T) {
	inner := &mockSink{failUntil: 100}
	s := NewMetricsSink(inner, "test")

	event := cdc.ChangeEvent{Action: "INSERT", Schema: "public", Table: "users"}
	err := s.Deliver(context.Background(), event)
	if err == nil {
		t.Fatal("expected error from inner sink")
	}
}

func TestMetricsSink_Deliver_ZeroTimestamp(t *testing.T) {
	inner := &mockSink{failUntil: 0}
	s := NewMetricsSink(inner, "test")

	// Zero timestamp should not panic
	event := cdc.ChangeEvent{Action: "INSERT", Schema: "public", Table: "users"}
	err := s.Deliver(context.Background(), event)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
}

func TestMetricsSink_Close(t *testing.T) {
	inner := &mockSink{}
	s := NewMetricsSink(inner, "test")
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// --- KafkaSink.buildKey tests ---

func TestKafkaSink_buildKey_WithID(t *testing.T) {
	s := &KafkaSink{topicPrefix: "pgcdc"}

	event := cdc.ChangeEvent{
		Action: "INSERT",
		Schema: "public",
		Table:  "users",
		New:    map[string]any{"id": float64(42), "name": "alice"},
		LSN:    "0/ABC",
	}

	key := s.buildKey(event)
	var parsed map[string]any
	if err := json.Unmarshal(key, &parsed); err != nil {
		t.Fatalf("unmarshal key: %v", err)
	}
	if parsed["id"] != float64(42) {
		t.Errorf("key id = %v, want 42", parsed["id"])
	}
}

func TestKafkaSink_buildKey_DeleteUsesOld(t *testing.T) {
	s := &KafkaSink{topicPrefix: "pgcdc"}

	event := cdc.ChangeEvent{
		Action: "DELETE",
		Schema: "public",
		Table:  "users",
		Old:    map[string]any{"id": float64(99)},
		LSN:    "0/DEF",
	}

	key := s.buildKey(event)
	var parsed map[string]any
	if err := json.Unmarshal(key, &parsed); err != nil {
		t.Fatalf("unmarshal key: %v", err)
	}
	if parsed["id"] != float64(99) {
		t.Errorf("key id = %v, want 99", parsed["id"])
	}
}

func TestKafkaSink_buildKey_NoID_Fallback(t *testing.T) {
	s := &KafkaSink{topicPrefix: "pgcdc"}

	event := cdc.ChangeEvent{
		Action: "INSERT",
		Schema: "public",
		Table:  "users",
		New:    map[string]any{"name": "alice"}, // no "id" column
		LSN:    "0/ABC",
	}

	key := s.buildKey(event)
	expected := "public.users.0/ABC"
	if string(key) != expected {
		t.Errorf("key = %q, want %q", string(key), expected)
	}
}

func TestKafkaSink_buildKey_NilMaps_Fallback(t *testing.T) {
	s := &KafkaSink{topicPrefix: "pgcdc"}

	event := cdc.ChangeEvent{
		Action: "DELETE",
		Schema: "public",
		Table:  "users",
		LSN:    "0/123",
	}

	key := s.buildKey(event)
	expected := "public.users.0/123"
	if string(key) != expected {
		t.Errorf("key = %q, want %q", string(key), expected)
	}
}

// --- RetrySink.Close delegates ---

func TestRetrySink_Close(t *testing.T) {
	inner := &mockSink{}
	retry := NewRetrySink(inner, WithMaxAttempts(3))
	if err := retry.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// --- RetrySink default onFailure returns error ---

func TestRetrySink_DefaultOnFailure(t *testing.T) {
	inner := &mockSink{failUntil: 100}
	retry := NewRetrySink(inner,
		WithMaxAttempts(2),
		WithBaseDelay(1*time.Millisecond),
	)

	event := cdc.ChangeEvent{Action: "INSERT", Schema: "public", Table: "users", LSN: "0/1"}
	err := retry.Deliver(context.Background(), event)
	if err == nil {
		t.Fatal("expected error from default onFailure")
	}
	if !strings.Contains(err.Error(), "all retries exhausted") {
		t.Errorf("error = %q, want it to mention 'all retries exhausted'", err.Error())
	}
}

// --- WebhookSink context cancellation ---

func TestWebhookSink_Deliver_ContextCancelled(t *testing.T) {
	// Server that sleeps forever
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Second)
	}))
	defer server.Close()

	s := NewWebhookSink(server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	event := cdc.ChangeEvent{Action: "INSERT", Schema: "public", Table: "users"}
	err := s.Deliver(ctx, event)
	if err == nil {
		t.Fatal("expected error from context timeout")
	}
}

// --- Integration: MetricsSink wraps WebhookSink ---

func TestMetricsSink_WrapsWebhookSink(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	webhook := NewWebhookSink(server.URL)
	metrics := NewMetricsSink(webhook, "webhook")
	defer metrics.Close()

	event := cdc.ChangeEvent{
		Action:    "INSERT",
		Schema:    "public",
		Table:     "users",
		Timestamp: time.Now(),
		LSN:       "0/1",
	}

	if err := metrics.Deliver(context.Background(), event); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if callCount != 1 {
		t.Errorf("webhook called %d times, want 1", callCount)
	}
}

// --- Batch + Webhook integration ---

func TestBatchSink_WithWebhookBatch(t *testing.T) {
	var batches [][]cdc.ChangeEvent

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var events []cdc.ChangeEvent
		json.Unmarshal(body, &events)
		batches = append(batches, events)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	inner := NewWebhookBatchSink(server.URL)
	batch := NewBatchSink(inner, 2, 10*time.Second)
	defer batch.Close()

	ctx := context.Background()
	for i := range 4 {
		event := cdc.ChangeEvent{
			Action: "INSERT",
			Schema: "public",
			Table:  "users",
			New:    map[string]any{"id": float64(i)},
		}
		if err := batch.Deliver(ctx, event); err != nil {
			t.Fatalf("Deliver %d: %v", i, err)
		}
	}

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
}

// --- DLQ Close idempotent ---

func TestDLQSink_CloseIdempotent(t *testing.T) {
	f, err := os.CreateTemp("", "pgcdc-dlq-*.jsonl")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	dlq, err := NewDLQSink(path)
	if err != nil {
		t.Fatalf("NewDLQSink: %v", err)
	}
	dlq.Close()
	// Second close should not panic (may return error, that's OK)
	dlq.Close()
}

// --- NewWebhookSink timeout ---

func TestNewWebhookSink_HasTimeout(t *testing.T) {
	s := NewWebhookSink("http://example.com")
	if s.client.Timeout != 10*time.Second {
		t.Errorf("timeout = %v, want 10s", s.client.Timeout)
	}
}

func TestNewWebhookBatchSink_HasTimeout(t *testing.T) {
	s := NewWebhookBatchSink("http://example.com")
	if s.client.Timeout != 30*time.Second {
		t.Errorf("timeout = %v, want 30s", s.client.Timeout)
	}
}

// --- batchOutput JSON structure ---

func TestBatchOutput_JSON(t *testing.T) {
	out := batchOutput{
		BatchSize: 2,
		Events: []cdc.ChangeEvent{
			{Action: "INSERT", Schema: "public", Table: "users"},
			{Action: "DELETE", Schema: "public", Table: "orders"},
		},
	}

	data, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded batchOutput
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.BatchSize != 2 {
		t.Errorf("BatchSize = %d, want 2", decoded.BatchSize)
	}
	if len(decoded.Events) != 2 {
		t.Errorf("Events = %d, want 2", len(decoded.Events))
	}
}

// --- RetryOption coverage ---

func TestRetryOptions(t *testing.T) {
	inner := &mockSink{}
	r := NewRetrySink(inner)

	// Verify defaults
	if r.maxAttempts != 5 {
		t.Errorf("default maxAttempts = %d, want 5", r.maxAttempts)
	}
	if r.baseDelay != 100*time.Millisecond {
		t.Errorf("default baseDelay = %v, want 100ms", r.baseDelay)
	}
	if r.maxDelay != 30*time.Second {
		t.Errorf("default maxDelay = %v, want 30s", r.maxDelay)
	}

	// Apply options
	r2 := NewRetrySink(inner,
		WithMaxAttempts(10),
		WithBaseDelay(200*time.Millisecond),
		WithMaxDelay(1*time.Minute),
	)
	if r2.maxAttempts != 10 {
		t.Errorf("maxAttempts = %d, want 10", r2.maxAttempts)
	}
	if r2.baseDelay != 200*time.Millisecond {
		t.Errorf("baseDelay = %v, want 200ms", r2.baseDelay)
	}
	if r2.maxDelay != 1*time.Minute {
		t.Errorf("maxDelay = %v, want 1m", r2.maxDelay)
	}
}

// --- Verify WebhookSink sends POST ---

func TestWebhookSink_SendsPOST(t *testing.T) {
	var method string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := NewWebhookSink(server.URL)
	event := cdc.ChangeEvent{Action: "INSERT", Schema: "public", Table: "users"}
	s.Deliver(context.Background(), event)

	if method != "POST" {
		t.Errorf("method = %q, want POST", method)
	}
}

// --- Verify WebhookBatchSink sends POST ---

func TestWebhookBatchSink_SendsPOST(t *testing.T) {
	var method string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := NewWebhookBatchSink(server.URL)
	events := []cdc.ChangeEvent{{Action: "INSERT", Schema: "public", Table: "users"}}
	s.DeliverBatch(context.Background(), events)

	if method != "POST" {
		t.Errorf("method = %q, want POST", method)
	}
}

// --- Ensure unused bytes.Buffer reference compiles ---
var _ = bytes.NewBuffer
var _ = fmt.Sprint
