package sink

import (
	"context"
	"time"

	"github.com/debdutsaha/pgcdc/internal/cdc"
	"github.com/debdutsaha/pgcdc/internal/metrics"
)

// MetricsSink wraps any Sink and records Prometheus metrics on every delivery.
//
// It's a transparent decorator — insert it anywhere in the sink chain:
//
//	MetricsSink → RetrySink → WebhookSink
//
// This way metrics capture the full delivery latency including retries.
type MetricsSink struct {
	inner    Sink
	sinkName string
}

// NewMetricsSink wraps a Sink with Prometheus instrumentation.
// sinkName is used as a label value to distinguish metrics from different sink types.
func NewMetricsSink(inner Sink, sinkName string) *MetricsSink {
	return &MetricsSink{inner: inner, sinkName: sinkName}
}

// Deliver records replication lag, delivery duration, and event counts,
// then delegates to the inner sink.
func (m *MetricsSink) Deliver(ctx context.Context, event cdc.ChangeEvent) error {
	start := time.Now()

	// Track replication lag — how far behind real-time we are.
	if !event.Timestamp.IsZero() {
		lag := time.Since(event.Timestamp).Seconds()
		metrics.ReplicationLag.Set(lag)
	}

	err := m.inner.Deliver(ctx, event)
	duration := time.Since(start).Seconds()

	// Record delivery latency
	metrics.DeliveryDuration.WithLabelValues(m.sinkName).Observe(duration)

	// Record event count with status
	status := "delivered"
	if err != nil {
		status = "error"
	}
	metrics.EventsTotal.WithLabelValues(event.Action, event.Table, status).Inc()

	return err
}

// Close closes the inner sink.
func (m *MetricsSink) Close() error {
	return m.inner.Close()
}
