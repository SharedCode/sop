package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
)

type QueueProducer struct {
	producer sarama.SyncProducer
}

// Package global producer.
var producer *QueueProducer
var mux sync.Mutex

func prepareMessage(topic, message string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(message),
	}
	return msg
}

// GetProducer will return the singleton instance of the producer.
func GetProducer(config *sarama.Config) (*QueueProducer, error) {
	if producer != nil {
		return producer, nil
	}
	mux.Lock()
	defer mux.Unlock()
	if producer != nil {
		return producer, nil
	}
	if config == nil {
		config = sarama.NewConfig()
		config.Version = sarama.V2_6_0_0
		config.Producer.Partitioner = sarama.NewRandomPartitioner
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Return.Successes = true
		// Default 1 MB buffer size on producer.
		config.Producer.Flush.Bytes = 2 * 1024 * 1024
	}
	p, err := sarama.NewSyncProducer(globalConfig.Brokers, config)
	if err != nil {
		return nil, err
	}
	producer = &QueueProducer{producer: p}
	return producer, nil
}

// Close the singleton instance producer.
func CloseProducer() {
	if producer != nil {
		mux.Lock()
		defer mux.Unlock()
		if producer == nil {
			return
		}
		producer.producer.Close()
		producer = nil
	}
}

var lastEngueueSucceeded bool

// Returns true if it is known that last Enqueue to Kafka succeeded or not.
func LastEnqueueSucceeded() bool {
	return lastEngueueSucceeded
}

// Enqueue will send message to the Kafka queue of the configured topic.
func Enqueue[T any](ctx context.Context, items ...T) ([]string, error) {
	var err error
	if producer == nil {
		if !IsInitialized() {
			lastEngueueSucceeded = false
			return nil, fmt.Errorf("Kafka is not initialized, please set kafka package's brokers & topic config")
		}
		producer, err = GetProducer(nil)
		if err != nil {
			lastEngueueSucceeded = false
			return nil, fmt.Errorf("Can't send %d messages as can't open a Producer, details: %v", len(items), err)
		}
	}
	var lastErr error
	results := make([]string, 0, len(items))
	for i := range items {
		ba, err := json.Marshal(items[i])
		if err != nil {
			lastErr = fmt.Errorf("Item #%d. Error detected marhaling item, detail: %v", i, err)
			continue
		}
		msg := prepareMessage(globalConfig.Topic, string(ba))
		partition, offset, err := producer.producer.SendMessage(msg)
		if err != nil {
			lastErr = fmt.Errorf("Item #%d. Error detected sending item, detail: %v", i, err)
			continue
		}
		results = append(results, fmt.Sprintf("Item %d. Message was saved to partion: %d, offset is: %d", i, partition, offset))
	}
	lastEngueueSucceeded = lastErr == nil
	return results, lastErr
}
