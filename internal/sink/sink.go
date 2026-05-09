package sink

import (
	"context"

	"github.com/debdutsaha/pgcdc/internal/cdc"
)

// Sink is the interface every CDC destination must implement.
//
// Why an interface and not just a function?
// Because sinks may need setup (HTTP clients, Kafka producers, file handles)
// and cleanup (flushing buffers, closing connections). The interface makes
// this lifecycle explicit.
//
// The contract:
//   - Deliver may be called concurrently from multiple goroutines in the
//     future (e.g., per-table parallelism), so implementations must be
//     safe for concurrent use.
//   - If Deliver returns an error, the stream will NOT acknowledge this
//     LSN to Postgres. On restart, the same event will be re-delivered.
//     This gives us at-least-once delivery semantics.
//   - Close is called once during shutdown. Flush any buffered data here.
type Sink interface {
	// Deliver sends a single change event to the destination.
	// Returns an error if the delivery fails — the event will be retried.
	Deliver(ctx context.Context, event cdc.ChangeEvent) error

	// Close performs cleanup (flush buffers, close connections).
	Close() error
}
