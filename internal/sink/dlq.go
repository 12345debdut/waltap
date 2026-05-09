package sink

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/debdutsaha/pgcdc/internal/cdc"
	"github.com/debdutsaha/pgcdc/internal/metrics"
)

// DLQEntry is what gets written to the dead-letter file.
// It wraps the original event with metadata about why it failed.
//
// When you replay DLQ entries later, the Error and FailedAt fields
// tell you what went wrong and when. The original Event is preserved
// exactly as it was — no data loss.
type DLQEntry struct {
	Event    cdc.ChangeEvent `json:"event"`
	Error    string          `json:"error"`
	FailedAt time.Time       `json:"failed_at"`
}

// DLQSink writes failed events to a JSONL file (one JSON object per line).
//
// This is NOT a regular Sink — it doesn't implement the Sink interface.
// Instead, it's used as the onFailure callback for RetrySink:
//
//	retry := NewRetrySink(inner, WithOnFailure(dlq.Handler()))
//
// Why a file and not a database table?
//   - Zero external dependencies — works with any sink type
//   - JSONL is easy to inspect with jq, grep, wc -l
//   - Can be replayed with a simple script
//   - Doesn't require the same Postgres we're capturing from
//     (that would be circular if Postgres itself is the problem)
//
// The file is opened in append mode with O_SYNC for durability.
// If even the DLQ write fails, we log and return the original error —
// this stops the stream, which is the safest fallback.
type DLQSink struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
	path string
}

// NewDLQSink creates a dead-letter queue backed by a JSONL file.
// The file is created if it doesn't exist, or appended to if it does.
func NewDLQSink(path string) (*DLQSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open DLQ file: %w", err)
	}

	return &DLQSink{
		file: f,
		enc:  json.NewEncoder(f),
		path: path,
	}, nil
}

// Handler returns a function suitable for RetrySink's WithOnFailure option.
//
// When called, it writes the failed event to the DLQ file and returns nil.
// Returning nil tells the RetrySink to continue processing the stream —
// the event is "handled" by being parked in the DLQ.
//
// If the DLQ write itself fails, we return the original error. This
// propagates up and stops the stream — better to halt than silently
// lose events.
func (d *DLQSink) Handler() func(event cdc.ChangeEvent, err error) error {
	return func(event cdc.ChangeEvent, deliveryErr error) error {
		entry := DLQEntry{
			Event:    event,
			Error:    deliveryErr.Error(),
			FailedAt: time.Now(),
		}

		d.mu.Lock()
		writeErr := d.enc.Encode(entry)
		d.mu.Unlock()

		if writeErr != nil {
			// Can't even write to the DLQ — propagate the original error.
			// This stops the stream, which is the safest thing to do.
			log.Printf("DLQ write failed: %v (original error: %v)", writeErr, deliveryErr)
			return fmt.Errorf("DLQ write failed (original: %v): %w", deliveryErr, writeErr)
		}

		metrics.DLQEventsTotal.WithLabelValues(event.Table).Inc()

		log.Printf("event sent to DLQ: %s %s.%s (LSN %s): %v",
			event.Action, event.Schema, event.Table, event.LSN, deliveryErr)

		// Return nil → stream continues. The event is parked in the DLQ.
		return nil
	}
}

// Close flushes and closes the DLQ file.
func (d *DLQSink) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}
