package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/SharedCode/sop"
	"github.com/Shopify/sarama"
)

// Queue specifies methods used for managing persisted queue, e.g. - in kafka.
type Queue[T any] interface {
	// Dequeue takes out 'count' number of elements from the queue.
	Dequeue(ctx context.Context, count int) ([]T, error)
	// Enqueue add elements to the queue.
	Enqueue(ctx context.Context, items ...T) []sop.KeyValuePair[string, error]
	// // Subscribe will block and dequeue messages from Kafka queue on a given topic.
	// Subscribe(topic string, callback func(items T))
}

type kafkaQueue[T any] struct {
	producer sarama.SyncProducer
	consumer sarama.Consumer
}

type Config struct {
	Brokers []string
	Topic string
}

var DefaultConfig Config = Config {
	Brokers: []string{"127.0.0.1:9092"},
	Topic: "sop-deleted-data",
}

var globalConfig Config = DefaultConfig

// Set Kafka brokers & topic globally.
func Initialize(config Config) error {
	if len(config.Brokers) == 0 {
		return fmt.Errorf("Can't initialize Kafka with no broker")
	}
	if config.Topic == "" {
		return fmt.Errorf("Can't initialize Kafka with no topic")
	}
	// Simple assignment of brokers. Next newProducer call will then start using the
	// newly assigned brokers.
	globalConfig = config

	return nil
}

// Returns true if Kafka brokers and topic are set.
func IsInitialized() bool {
	return len(globalConfig.Brokers) > 0 && globalConfig.Topic != ""
}

// NewQueue instantiates a Queue that uses Kafka for enqueuing and
// dequeueing of messages.
func NewQueue[T any]() (Queue[T], error) {
	p, err := newProducer()
	if err != nil {
		return nil, fmt.Errorf("Could not create producer: %v", err)
	}
	c, err := sarama.NewConsumer(globalConfig.Brokers, nil)
	if err != nil {
		return nil, fmt.Errorf("Could not create consumer: %v", err)
	}

	return &kafkaQueue[T] {
		producer: p,
		consumer: c,
	}, nil
}

func (q *kafkaQueue[T])Enqueue(ctx context.Context, items ...T) []sop.KeyValuePair[string, error] {
	results := make([]sop.KeyValuePair[string, error], len(items))
	for i := range items {
		ba, err := json.Marshal(items[i])
		if err != nil {
			results[i] = sop.KeyValuePair[string, error]{
				Value: fmt.Errorf("Error detected marhaling item %d, detail: %v", i, err),
			}
			continue
		}
		msg := prepareMessage(globalConfig.Topic, string(ba))
		partition, offset, err := q.producer.SendMessage(msg)
		if err != nil {
			results[i] = sop.KeyValuePair[string, error]{
				Value: fmt.Errorf("Error detected sending item %d, detail: %v", i, err),
			}
			continue
		}
		results[i] = sop.KeyValuePair[string, error]{
			Key: fmt.Sprintf("Item %d msg was saved to partion: %d, offset is: %d", i, partition, offset),
		}
	}
	return nil
}

func (q *kafkaQueue[T])Dequeue(ctx context.Context, count int) ([]T, error) {
	return nil, nil
}

// // Subscribe will block and dequeue messages from Kafka queue on a given topic.
// func (q *kafkaQueue[T])Subscribe(topic string, callback func(msg string)) {

// }

func saveMessage(text string) {
	// Parse text then issue deletes in Cassandra using Registry and BlobRepository.
	// fakeDB = text
}
