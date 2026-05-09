package sink

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debdutsaha/pgcdc/internal/cdc"
)

// mockSink records calls and can be configured to fail N times.
type mockSink struct {
	deliverCount atomic.Int64
	failUntil    int64 // fail the first N calls
	lastEvent    cdc.ChangeEvent
}

func (m *mockSink) Deliver(_ context.Context, event cdc.ChangeEvent) error {
	n := m.deliverCount.Add(1)
	m.lastEvent = event
	if n <= m.failUntil {
		return errors.New("simulated failure")
	}
	return nil
}

func (m *mockSink) Close() error { return nil }

func TestRetrySink_SucceedsFirstAttempt(t *testing.T) {
	mock := &mockSink{failUntil: 0}
	retry := NewRetrySink(mock,
		WithMaxAttempts(3),
		WithBaseDelay(1*time.Millisecond),
	)

	event := cdc.ChangeEvent{Action: "INSERT", Table: "users"}
	err := retry.Deliver(context.Background(), event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := mock.deliverCount.Load(); got != 1 {
		t.Errorf("deliver called %d times, want 1", got)
	}
}

func TestRetrySink_SucceedsAfterRetries(t *testing.T) {
	mock := &mockSink{failUntil: 2} // fail first 2, succeed on 3rd
	retry := NewRetrySink(mock,
		WithMaxAttempts(5),
		WithBaseDelay(1*time.Millisecond),
	)

	event := cdc.ChangeEvent{Action: "INSERT", Table: "users"}
	err := retry.Deliver(context.Background(), event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := mock.deliverCount.Load(); got != 3 {
		t.Errorf("deliver called %d times, want 3", got)
	}
}

func TestRetrySink_ExhaustsAllAttempts(t *testing.T) {
	mock := &mockSink{failUntil: 100} // always fail
	retry := NewRetrySink(mock,
		WithMaxAttempts(3),
		WithBaseDelay(1*time.Millisecond),
		WithMaxDelay(5*time.Millisecond),
	)

	event := cdc.ChangeEvent{Action: "INSERT", Table: "users"}
	err := retry.Deliver(context.Background(), event)

	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if got := mock.deliverCount.Load(); got != 3 {
		t.Errorf("deliver called %d times, want 3", got)
	}
}

func TestRetrySink_CustomOnFailure(t *testing.T) {
	mock := &mockSink{failUntil: 100}
	var failedEvent cdc.ChangeEvent
	var failedErr error

	retry := NewRetrySink(mock,
		WithMaxAttempts(2),
		WithBaseDelay(1*time.Millisecond),
		WithOnFailure(func(event cdc.ChangeEvent, err error) error {
			failedEvent = event
			failedErr = err
			return nil // swallow the error (drop the event)
		}),
	)

	event := cdc.ChangeEvent{Action: "DELETE", Table: "orders", LSN: "0/ABC"}
	err := retry.Deliver(context.Background(), event)

	// onFailure returned nil, so Deliver should return nil too
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if failedEvent.Action != "DELETE" {
		t.Errorf("onFailure got action %q, want DELETE", failedEvent.Action)
	}
	if failedErr == nil {
		t.Error("onFailure should have received the last error")
	}
}

func TestRetrySink_RespectsContextCancellation(t *testing.T) {
	mock := &mockSink{failUntil: 100}
	retry := NewRetrySink(mock,
		WithMaxAttempts(10),
		WithBaseDelay(100*time.Millisecond), // long delay so we can cancel
	)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	event := cdc.ChangeEvent{Action: "INSERT", Table: "users"}
	err := retry.Deliver(ctx, event)

	if err == nil {
		t.Fatal("expected error from cancelled context")
	}

	// Should have attempted at most 2 times (first fail + waiting for retry when cancelled)
	if got := mock.deliverCount.Load(); got > 2 {
		t.Errorf("deliver called %d times, expected ≤2 with cancelled context", got)
	}
}

func TestRetrySink_ExponentialBackoff(t *testing.T) {
	mock := &mockSink{failUntil: 3} // fail 3 times, succeed on 4th
	retry := NewRetrySink(mock,
		WithMaxAttempts(5),
		WithBaseDelay(10*time.Millisecond),
		WithMaxDelay(1*time.Second),
	)

	event := cdc.ChangeEvent{Action: "INSERT", Table: "users"}

	start := time.Now()
	err := retry.Deliver(context.Background(), event)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Expected delays: 10ms + 20ms + 40ms = 70ms minimum
	// Allow some slack for scheduling
	if elapsed < 50*time.Millisecond {
		t.Errorf("elapsed %v is too fast, expected backoff delays", elapsed)
	}
}
