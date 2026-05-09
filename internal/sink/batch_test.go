package sink

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/debdutsaha/pgcdc/internal/cdc"
)

// mockBatchDeliverer records all batches it receives.
type mockBatchDeliverer struct {
	mu      sync.Mutex
	batches [][]cdc.ChangeEvent
}

func (m *mockBatchDeliverer) DeliverBatch(_ context.Context, events []cdc.ChangeEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Copy the slice so it doesn't get reused
	batch := make([]cdc.ChangeEvent, len(events))
	copy(batch, events)
	m.batches = append(m.batches, batch)
	return nil
}

func (m *mockBatchDeliverer) Close() error { return nil }

func (m *mockBatchDeliverer) getBatches() [][]cdc.ChangeEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([][]cdc.ChangeEvent, len(m.batches))
	copy(result, m.batches)
	return result
}

func TestBatchSink_FlushOnSize(t *testing.T) {
	mock := &mockBatchDeliverer{}
	batch := NewBatchSink(mock, 3, 10*time.Second) // large maxWait so timer doesn't fire
	defer batch.Close()

	ctx := context.Background()

	// Send 3 events — should flush immediately when hitting batch size
	for i := range 3 {
		event := cdc.ChangeEvent{Action: "INSERT", Table: "users", Xid: uint32(i)}
		if err := batch.Deliver(ctx, event); err != nil {
			t.Fatalf("Deliver %d: %v", i, err)
		}
	}

	batches := mock.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if len(batches[0]) != 3 {
		t.Errorf("batch size = %d, want 3", len(batches[0]))
	}
}

func TestBatchSink_FlushOnTime(t *testing.T) {
	mock := &mockBatchDeliverer{}
	batch := NewBatchSink(mock, 100, 100*time.Millisecond) // small maxWait
	defer batch.Close()

	ctx := context.Background()

	// Send 2 events (less than batch size)
	for i := range 2 {
		event := cdc.ChangeEvent{Action: "INSERT", Table: "users", Xid: uint32(i)}
		if err := batch.Deliver(ctx, event); err != nil {
			t.Fatalf("Deliver %d: %v", i, err)
		}
	}

	// No flush yet — batch isn't full
	if batches := mock.getBatches(); len(batches) != 0 {
		t.Fatalf("expected 0 batches before timer, got %d", len(batches))
	}

	// Wait for the timer to fire
	time.Sleep(250 * time.Millisecond)

	batches := mock.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch after timer, got %d", len(batches))
	}
	if len(batches[0]) != 2 {
		t.Errorf("batch size = %d, want 2", len(batches[0]))
	}
}

func TestBatchSink_MultipleBatches(t *testing.T) {
	mock := &mockBatchDeliverer{}
	batch := NewBatchSink(mock, 2, 10*time.Second)
	defer batch.Close()

	ctx := context.Background()

	// Send 5 events with batch size 2 → should get 2 full batches
	for i := range 4 {
		event := cdc.ChangeEvent{Action: "INSERT", Table: "users", Xid: uint32(i)}
		if err := batch.Deliver(ctx, event); err != nil {
			t.Fatalf("Deliver %d: %v", i, err)
		}
	}

	batches := mock.getBatches()
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	if len(batches[0]) != 2 || len(batches[1]) != 2 {
		t.Errorf("batch sizes = [%d, %d], want [2, 2]", len(batches[0]), len(batches[1]))
	}
}

func TestBatchSink_CloseFlushesRemaining(t *testing.T) {
	mock := &mockBatchDeliverer{}
	batch := NewBatchSink(mock, 100, 10*time.Second)

	ctx := context.Background()

	// Send 2 events (won't trigger size or time flush)
	for i := range 2 {
		event := cdc.ChangeEvent{Action: "INSERT", Table: "users", Xid: uint32(i)}
		if err := batch.Deliver(ctx, event); err != nil {
			t.Fatalf("Deliver %d: %v", i, err)
		}
	}

	// No batches yet
	if batches := mock.getBatches(); len(batches) != 0 {
		t.Fatalf("expected 0 batches before Close, got %d", len(batches))
	}

	// Close should flush remaining
	batch.Close()

	batches := mock.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch after Close, got %d", len(batches))
	}
	if len(batches[0]) != 2 {
		t.Errorf("batch size = %d, want 2", len(batches[0]))
	}
}
