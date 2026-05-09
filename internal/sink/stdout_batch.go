package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/debdutsaha/pgcdc/internal/cdc"
)

// StdoutBatchSink writes each batch as a JSON array to stdout.
//
// Each flush produces one line:
//
//	{"batch_size":3,"events":[{...},{...},{...}]}
//
// This makes it easy to see when batches flush and how many events
// they contain — useful for testing the batching logic.
type StdoutBatchSink struct {
	encoder *json.Encoder
}

// batchOutput is the JSON structure written per flush.
type batchOutput struct {
	BatchSize int              `json:"batch_size"`
	Events    []cdc.ChangeEvent `json:"events"`
}

func NewStdoutBatchSink() *StdoutBatchSink {
	return &StdoutBatchSink{
		encoder: json.NewEncoder(os.Stdout),
	}
}

func (s *StdoutBatchSink) DeliverBatch(_ context.Context, events []cdc.ChangeEvent) error {
	fmt.Fprintf(os.Stderr, "[batch] flushing %d events\n", len(events))
	return s.encoder.Encode(batchOutput{
		BatchSize: len(events),
		Events:    events,
	})
}

func (s *StdoutBatchSink) Close() error {
	return nil
}
