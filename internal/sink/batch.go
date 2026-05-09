package sink

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/debdutsaha/pgcdc/internal/cdc"
)

// BatchSink collects events and delivers them in bulk.
//
// Why batch? Two reasons:
//
//  1. Throughput — one HTTP POST with 100 events is much cheaper than
//     100 individual POSTs. Network round-trip dominates at low payload sizes.
//
//  2. Atomicity — if the downstream wants to process events transactionally
//     (e.g., write a batch to a database in one transaction), batching
//     aligns our delivery with that pattern.
//
// Flush triggers (whichever comes first):
//   - Batch reaches MaxSize events
//   - MaxWait duration elapses since the first event entered the batch
//
// Thread safety: Deliver can be called from the stream goroutine while
// the flush timer fires from a background goroutine. All access to the
// buffer is protected by a mutex.
type BatchSink struct {
	// inner is the actual destination. It receives batches via DeliverBatch.
	inner BatchDeliverer

	maxSize int
	maxWait time.Duration

	mu      sync.Mutex
	buffer  []cdc.ChangeEvent
	timer   *time.Timer
	flushed chan struct{} // signals when a timer-triggered flush completes
}

// BatchDeliverer is the interface for sinks that can receive a batch of events.
// This is separate from Sink because batch delivery has different semantics —
// either the whole batch succeeds or it fails.
type BatchDeliverer interface {
	// DeliverBatch sends a batch of events to the destination.
	// If it returns an error, none of the events are considered delivered.
	DeliverBatch(ctx context.Context, events []cdc.ChangeEvent) error

	// Close performs cleanup.
	Close() error
}

// NewBatchSink wraps a BatchDeliverer with batching logic.
//
// maxSize: flush when this many events are buffered (e.g., 100)
// maxWait: flush after this duration even if batch isn't full (e.g., 500ms)
func NewBatchSink(inner BatchDeliverer, maxSize int, maxWait time.Duration) *BatchSink {
	return &BatchSink{
		inner:   inner,
		maxSize: maxSize,
		maxWait: maxWait,
		buffer:  make([]cdc.ChangeEvent, 0, maxSize),
		flushed: make(chan struct{}, 1),
	}
}

// Deliver adds an event to the batch buffer.
// If the buffer reaches maxSize, it flushes immediately (synchronously).
// On the first event, a timer starts — if maxWait elapses before maxSize
// is reached, the timer triggers a flush.
func (b *BatchSink) Deliver(ctx context.Context, event cdc.ChangeEvent) error {
	b.mu.Lock()

	b.buffer = append(b.buffer, event)

	// Start the flush timer on first event in a new batch.
	if len(b.buffer) == 1 {
		b.timer = time.AfterFunc(b.maxWait, func() {
			b.mu.Lock()
			batch := b.takeBatchLocked()
			b.mu.Unlock()

			if len(batch) > 0 {
				if err := b.inner.DeliverBatch(ctx, batch); err != nil {
					log.Printf("batch flush (timer) failed: %v", err)
				}
			}

			// Signal that timer flush completed.
			select {
			case b.flushed <- struct{}{}:
			default:
			}
		})
	}

	// If we've hit maxSize, flush immediately (don't wait for timer).
	if len(b.buffer) >= b.maxSize {
		batch := b.takeBatchLocked()
		if b.timer != nil {
			b.timer.Stop()
			b.timer = nil
		}
		b.mu.Unlock()
		return b.inner.DeliverBatch(ctx, batch)
	}

	b.mu.Unlock()
	return nil
}

// Close flushes any remaining events and closes the inner sink.
func (b *BatchSink) Close() error {
	b.mu.Lock()
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	batch := b.takeBatchLocked()
	b.mu.Unlock()

	if len(batch) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := b.inner.DeliverBatch(ctx, batch); err != nil {
			log.Printf("final batch flush failed: %v", err)
		}
	}
	return b.inner.Close()
}

// takeBatchLocked swaps the buffer for a fresh one and returns the old batch.
// Caller must hold b.mu.
func (b *BatchSink) takeBatchLocked() []cdc.ChangeEvent {
	batch := b.buffer
	b.buffer = make([]cdc.ChangeEvent, 0, b.maxSize)
	return batch
}
