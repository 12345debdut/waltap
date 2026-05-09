package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/debdutsaha/pgcdc/internal/cdc"
)

// WebhookBatchSink delivers batches of events as a JSON array via HTTP POST.
//
// The POST body looks like:
//
//	[
//	  {"action":"INSERT","schema":"public","table":"users","new":{...},...},
//	  {"action":"UPDATE","schema":"public","table":"orders","new":{...},...}
//	]
//
// Headers:
//
//	Content-Type: application/json
//	X-PGCDC-Batch-Size: 5
//
// This is used as the inner deliverer for BatchSink when the destination
// is a webhook endpoint.
type WebhookBatchSink struct {
	url    string
	client *http.Client
}

// NewWebhookBatchSink creates a batch sink that POSTs JSON arrays to the given URL.
func NewWebhookBatchSink(url string) *WebhookBatchSink {
	return &WebhookBatchSink{
		url: url,
		client: &http.Client{
			Timeout: 30 * time.Second, // longer timeout for batch payloads
		},
	}
}

// DeliverBatch sends a batch of events as a JSON array via HTTP POST.
func (s *WebhookBatchSink) DeliverBatch(ctx context.Context, events []cdc.ChangeEvent) error {
	body, err := json.Marshal(events)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-PGCDC-Batch-Size", fmt.Sprintf("%d", len(events)))

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", s.url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s returned %d", s.url, resp.StatusCode)
	}
	return nil
}

// Close drains idle HTTP connections.
func (s *WebhookBatchSink) Close() error {
	s.client.CloseIdleConnections()
	return nil
}
