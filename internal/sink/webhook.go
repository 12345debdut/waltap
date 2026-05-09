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

// WebhookSink delivers each change event as an HTTP POST with a JSON body.
//
// This is how you'd integrate with any external system: a webhook receiver,
// a serverless function, a REST API. The receiver gets a POST request with:
//
//	Content-Type: application/json
//	X-PGCDC-Action: INSERT
//	X-PGCDC-Table: public.users
//
// If the endpoint returns a non-2xx status, Deliver returns an error,
// which means the stream won't acknowledge this LSN and the event
// will be re-delivered on restart (at-least-once semantics).
type WebhookSink struct {
	url    string
	client *http.Client
}

func NewWebhookSink(url string) *WebhookSink {
	return &WebhookSink{
		url: url,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (s *WebhookSink) Deliver(ctx context.Context, event cdc.ChangeEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-PGCDC-Action", event.Action)
	req.Header.Set("X-PGCDC-Table", event.Schema+"."+event.Table)

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

func (s *WebhookSink) Close() error {
	s.client.CloseIdleConnections()
	return nil
}
