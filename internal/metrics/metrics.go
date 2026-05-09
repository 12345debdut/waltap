package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// CDC pipeline metrics.
//
// Naming follows Prometheus conventions:
//   - pgcdc_ prefix for all metrics
//   - _total suffix for counters
//   - _seconds suffix for durations
//   - Labels for dimensions (action, table, status)
//
// What to monitor with these:
//
//   Events processed rate:
//     rate(pgcdc_events_total[5m])
//
//   Error rate:
//     rate(pgcdc_events_total{status="error"}[5m])
//
//   Delivery latency p99:
//     histogram_quantile(0.99, rate(pgcdc_delivery_duration_seconds_bucket[5m]))
//
//   Replication lag:
//     pgcdc_replication_lag_seconds
//
//   Events currently buffered (for batch sinks):
//     pgcdc_batch_buffer_size

var (
	// EventsTotal counts the total number of change events processed,
	// labeled by action (INSERT/UPDATE/DELETE), table, and delivery status.
	EventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pgcdc_events_total",
			Help: "Total number of change events processed.",
		},
		[]string{"action", "table", "status"}, // status: "delivered", "filtered", "error"
	)

	// DeliveryDuration tracks how long each sink delivery takes.
	// This includes retry time — a delivery with 3 retries will report
	// the total wall time from first attempt to final success/failure.
	DeliveryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "pgcdc_delivery_duration_seconds",
			Help: "Time taken to deliver a change event to the sink.",
			// Buckets from 1ms to 30s — covers fast stdout to slow webhooks.
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5, 10, 30},
		},
		[]string{"sink"},
	)

	// ReplicationLag tracks the delay between when a transaction committed
	// in Postgres and when we processed it. High lag means we're falling behind.
	ReplicationLag = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "pgcdc_replication_lag_seconds",
			Help: "Seconds between transaction commit time and processing time.",
		},
	)

	// RetriesTotal counts retry attempts, labeled by outcome.
	RetriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pgcdc_retries_total",
			Help: "Total retry attempts for event delivery.",
		},
		[]string{"outcome"}, // "success", "exhausted"
	)

	// SnapshotRowsTotal counts rows emitted during the snapshot phase.
	SnapshotRowsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pgcdc_snapshot_rows_total",
			Help: "Total rows emitted during initial snapshot.",
		},
		[]string{"table"},
	)

	// BatchFlushTotal counts batch flushes, labeled by trigger.
	BatchFlushTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pgcdc_batch_flush_total",
			Help: "Total batch flushes by trigger type.",
		},
		[]string{"trigger"}, // "size", "time", "close"
	)

	// BatchSize tracks the distribution of batch sizes at flush time.
	BatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "pgcdc_batch_size",
			Help:    "Number of events per batch flush.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
	)

	// DLQEventsTotal counts events written to the dead-letter queue.
	DLQEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pgcdc_dlq_events_total",
			Help: "Total events written to the dead-letter queue.",
		},
		[]string{"table"},
	)
)
