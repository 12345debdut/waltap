package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/debdutsaha/pgcdc/internal/cdc"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaSink publishes each change event to a Kafka topic.
//
// Topic naming: by default, each table gets its own topic following the
// Debezium convention: "pgcdc.<schema>.<table>" (e.g., "pgcdc.public.users").
// This makes it easy for consumers to subscribe to changes for specific tables.
//
// Message key: the event's primary key columns serialized as JSON. This ensures
// all changes for the same row go to the same partition, preserving per-row
// ordering. If the key can't be determined, we use "<schema>.<table>.<lsn>".
//
// Message value: the full ChangeEvent as JSON.
//
// Why franz-go instead of confluent-kafka-go?
//   - Pure Go — no CGo, no librdkafka dependency, cross-compiles cleanly
//   - Modern API with context support throughout
//   - Good performance (benchmarks comparable to librdkafka)
//   - Used by Redpanda, WarpStream, and other serious Go Kafka projects
type KafkaSink struct {
	client      *kgo.Client
	topicPrefix string
}

// NewKafkaSink creates a Kafka producer sink.
//
// brokers: comma-separated list of broker addresses (e.g., "localhost:9092")
// topicPrefix: prepended to topic names (e.g., "pgcdc" → topic "pgcdc.public.users")
func NewKafkaSink(brokers string, topicPrefix string) (*KafkaSink, error) {
	brokerList := strings.Split(brokers, ",")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerList...),

		// Wait up to 10ms to batch messages before sending — trades a tiny
		// bit of latency for much better throughput (fewer network round-trips).
		kgo.ProducerLinger(10*time.Millisecond),

		// Batch up to 64KB per partition before sending.
		kgo.ProducerBatchMaxBytes(64*1024),

		// Require acks from all in-sync replicas for durability.
		// For a dev setup with replication-factor=1 this doesn't matter,
		// but it's the right default for production.
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	// Verify connectivity
	if err := client.Ping(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka ping failed: %w", err)
	}
	log.Printf("connected to kafka brokers: %s", brokers)

	return &KafkaSink{
		client:      client,
		topicPrefix: topicPrefix,
	}, nil
}

func (s *KafkaSink) Deliver(ctx context.Context, event cdc.ChangeEvent) error {
	// Build the topic name: prefix.schema.table
	topic := fmt.Sprintf("%s.%s.%s", s.topicPrefix, event.Schema, event.Table)

	// Build the message key from the event.
	// For ordering: all changes to the same row must go to the same partition.
	key := s.buildKey(event)

	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Produce synchronously — we block until Kafka acks.
	// This is simpler and safer than async produce + callback.
	// The franz-go client batches internally via ProducerLinger,
	// so we still get good throughput despite synchronous calls.
	result := s.client.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	})

	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("kafka produce to %s: %w", topic, err)
	}
	return nil
}

func (s *KafkaSink) Close() error {
	s.client.Close()
	return nil
}

// buildKey creates a message key for partitioning.
//
// We want all changes to the same row to land on the same partition
// so consumers see them in order. We use whatever identifies the row:
//   - For INSERT/UPDATE: use the "id" field from New (if present)
//   - For DELETE: use the "id" field from Old
//   - Fallback: schema.table.lsn
func (s *KafkaSink) buildKey(event cdc.ChangeEvent) []byte {
	// Try to find an "id" column in the row data
	source := event.New
	if source == nil {
		source = event.Old
	}

	if source != nil {
		if id, ok := source["id"]; ok {
			key, _ := json.Marshal(map[string]any{"id": id})
			return key
		}
	}

	// Fallback: use table + LSN
	return []byte(fmt.Sprintf("%s.%s.%s", event.Schema, event.Table, event.LSN))
}
