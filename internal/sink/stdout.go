package sink

import (
	"context"
	"encoding/json"
	"os"

	"github.com/debdutsaha/pgcdc/internal/cdc"
)

// StdoutSink writes each change event as a JSON line to stdout.
//
// This is the simplest possible sink — one JSON object per line,
// compatible with `jq`, UNIX pipes, log aggregators, and anything
// that reads newline-delimited JSON (NDJSON / JSON Lines).
//
// Example output:
//
//	{"action":"INSERT","schema":"public","table":"users","new":{"id":1,"name":"alice"},...}
//	{"action":"UPDATE","schema":"public","table":"users","old":{...},"new":{...},...}
type StdoutSink struct {
	encoder *json.Encoder
}

// NewStdoutSink creates a sink that writes NDJSON to stdout.
func NewStdoutSink() *StdoutSink {
	return &StdoutSink{
		encoder: json.NewEncoder(os.Stdout),
	}
}

// Deliver writes a single event as a JSON line to stdout.
func (s *StdoutSink) Deliver(_ context.Context, event cdc.ChangeEvent) error {
	return s.encoder.Encode(event)
}

// Close is a no-op — stdout doesn't need closing.
func (s *StdoutSink) Close() error {
	return nil
}
