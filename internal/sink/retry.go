package sink

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/debdutsaha/pgcdc/internal/cdc"
)

// RetrySink wraps a Sink with retry logic and exponential backoff.
//
// When the inner sink's Deliver fails:
//  1. Wait baseDelay * 2^attempt (with a cap at maxDelay)
//  2. Retry up to maxAttempts times
//  3. If all retries fail, call the onFailure callback
//
// Why exponential backoff? If the downstream is overloaded, hammering it
// with retries makes things worse. Doubling the wait each time gives it
// breathing room to recover. The maxDelay cap prevents absurd waits
// (e.g., 2^10 * 100ms = 102 seconds).
//
// The onFailure callback decides what happens to undeliverable events:
//   - Log and drop (lossy but simple)
//   - Write to a dead-letter file (lossless, manual replay later)
//   - Return the error (stops the stream, events replay from WAL on restart)
type RetrySink struct {
	inner       Sink
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
	onFailure   func(event cdc.ChangeEvent, err error) error
}

// RetryOption configures the RetrySink.
type RetryOption func(*RetrySink)

// WithMaxAttempts sets how many times to retry (default: 5).
func WithMaxAttempts(n int) RetryOption {
	return func(r *RetrySink) { r.maxAttempts = n }
}

// WithBaseDelay sets the initial retry delay (default: 100ms).
func WithBaseDelay(d time.Duration) RetryOption {
	return func(r *RetrySink) { r.baseDelay = d }
}

// WithMaxDelay caps the backoff delay (default: 30s).
func WithMaxDelay(d time.Duration) RetryOption {
	return func(r *RetrySink) { r.maxDelay = d }
}

// WithOnFailure sets the callback for events that fail all retries.
// Default: return the error (which stops the stream).
func WithOnFailure(fn func(event cdc.ChangeEvent, err error) error) RetryOption {
	return func(r *RetrySink) { r.onFailure = fn }
}

// NewRetrySink wraps a Sink with retry logic.
func NewRetrySink(inner Sink, opts ...RetryOption) *RetrySink {
	r := &RetrySink{
		inner:       inner,
		maxAttempts: 5,
		baseDelay:   100 * time.Millisecond,
		maxDelay:    30 * time.Second,
		onFailure: func(event cdc.ChangeEvent, err error) error {
			// Default: propagate the error. This stops the stream,
			// and the event will be re-delivered from WAL on restart.
			return fmt.Errorf("all retries exhausted for %s on %s.%s: %w",
				event.Action, event.Schema, event.Table, err)
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Deliver attempts to deliver the event with retries.
func (r *RetrySink) Deliver(ctx context.Context, event cdc.ChangeEvent) error {
	var lastErr error

	for attempt := range r.maxAttempts {
		err := r.inner.Deliver(ctx, event)
		if err == nil {
			if attempt > 0 {
				log.Printf("retry succeeded on attempt %d for %s %s.%s",
					attempt+1, event.Action, event.Schema, event.Table)
			}
			return nil
		}

		lastErr = err

		// Don't retry if context is cancelled (shutdown).
		if ctx.Err() != nil {
			return fmt.Errorf("context cancelled during retry: %w", err)
		}

		// Calculate backoff delay: baseDelay * 2^attempt, capped at maxDelay.
		delay := time.Duration(float64(r.baseDelay) * math.Pow(2, float64(attempt)))
		if delay > r.maxDelay {
			delay = r.maxDelay
		}

		log.Printf("delivery failed (attempt %d/%d), retrying in %s: %v",
			attempt+1, r.maxAttempts, delay, err)

		select {
		case <-time.After(delay):
			// Continue to next attempt.
		case <-ctx.Done():
			return fmt.Errorf("context cancelled waiting for retry: %w", ctx.Err())
		}
	}

	// All retries exhausted — call the failure handler.
	return r.onFailure(event, lastErr)
}

// Close delegates to the inner sink's Close.
func (r *RetrySink) Close() error {
	return r.inner.Close()
}
