package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"sync"

	"github.com/Shopify/sarama"
)

// Kafka send will be sampled
const successfulSendCountSamplerCount = 5

// QueueProducer struct contains the sarama producer instance & other necessary artifacts
// required to achieve our producer functionalities, e.g. - error tracking, successful send sampling...
type QueueProducer struct {
	producer            sarama.AsyncProducer
	errorsReceived      []error
	quit                chan struct{}
	successfulSendCount int
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
	}
	p, err := sarama.NewAsyncProducer(globalConfig.Brokers, config)
	if err != nil {
		return nil, err
	}
	producer = &QueueProducer{
		producer: p,
		quit:     make(chan struct{}),
	}
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
		// Signal producer error listener to quit.
		producer.producer.AsyncClose()
		producer.quit <- struct{}{}
		producer = nil
		log.Debug("Successful CloseProducer.")
	}
}

var lastEngueueSucceeded bool

// Returns true if it is known that last Enqueue to Kafka succeeded or not.
func LastEnqueueSucceeded() bool {
	return lastEngueueSucceeded
}

// Enqueue will send message to the Kafka queue of the configured topic.
func Enqueue[T any](ctx context.Context, items ...T) (bool, error) {
	var err error
	var lastErr error
	if producer == nil {
		if !IsInitialized() {
			lastEngueueSucceeded = false
			return false, fmt.Errorf("Kafka is not initialized, please set kafka package's brokers & topic config")
		}
		producer, err = GetProducer(nil)
		if err != nil {
			lastEngueueSucceeded = false
			return false, fmt.Errorf("Can't send %d messages as can't open a Producer, details: %v", len(items), err)
		}
		// Setup the error channel listener to collect errors from Kafka brokers.
		go func() {
			for {
				select {
				case err := <-producer.producer.Errors():
					producer.errorsReceived = append(producer.errorsReceived, err)
					producer.successfulSendCount = 0
				case <-producer.quit:
					log.Debug("Exiting the AsyncProducer error listener.")
					return
				}
			}
		}()
	}
	// Send the messages to Kafka topic.
	for i := range items {
		ba, err := json.Marshal(items[i])
		if err != nil {
			lastErr = fmt.Errorf("Item #%d. Error detected marhaling item, detail: %v", i, err)
			continue
		}
		msg := prepareMessage(globalConfig.Topic, string(ba))
		producer.producer.Input() <- msg
	}

	if lastErr == nil && producer.successfulSendCount <= successfulSendCountSamplerCount {
		producer.successfulSendCount++
	}

	// Only tell SOP transaction that Kafka producer is ready if successfully sending messages surpassing sampler count.
	lastEngueueSucceeded = lastErr == nil && producer.successfulSendCount >= successfulSendCountSamplerCount &&
		len(producer.errorsReceived) == 0

	if len(producer.errorsReceived) > 0 && lastErr == nil {
		// Return the last known error if there is one detected by the kafka Error listener.
		lastErr = producer.errorsReceived[len(producer.errorsReceived)-1]
		producer.errorsReceived = nil
	}

	return lastEngueueSucceeded, lastErr
}
