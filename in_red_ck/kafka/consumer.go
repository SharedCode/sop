package kafka

import (
	"context"
	"fmt"
	"time"
	"sync"
	"sync/atomic"
	"encoding/json"

	"github.com/Shopify/sarama"
)

type QueueConsumer struct {
	consumer sarama.Consumer
}

var consumer *QueueConsumer

// Returns the singleton instance consumer.
func GetConsumer(config *sarama.Config) (*QueueConsumer, error) {
	if consumer != nil {
		return consumer, nil
	}
	mux.Lock()
	if consumer != nil {
		return consumer, nil
	}
	if config == nil {
		config = sarama.NewConfig()
		// 5 MB default fetch size.
		config.Consumer.Fetch.Default = 5 * 1024 * 1024
		config.Consumer.MaxWaitTime = 500 * time.Millisecond
	}
	c, err := sarama.NewConsumer(globalConfig.Brokers, config)
	if err != nil {
		return nil, err
	}
	consumer = &QueueConsumer{consumer: c}
	return consumer, nil
}
// Close the singleton instance consumer.
func CloseConsumer() {
	if consumer != nil {
		mux.Lock()
		defer mux.Unlock()
		if consumer == nil {
			return
		}
		consumer.consumer.Close()
		consumer = nil
	}
}

// Dequeue fetches count number of messages from kafka queue on the configured topic.
func Dequeue[T any](ctx context.Context, count int) ([]T, []error) {
	if consumer == nil {
		return nil, []error{fmt.Errorf("Can't fetch %d messages as Consumer is not open", count) }
	}

	messages := make([]T, 0, count)
	receiverChannel := make(chan string, 1)
	var wg sync.WaitGroup
	
	if err := Subscribe(globalConfig.Topic, consumer.consumer, count, receiverChannel, &wg); err != nil {
		return nil, []error{err}
	}
	errors := make([]error, 0, count)
	for msg := range receiverChannel {
		var o T
		if err := json.Unmarshal([]byte(msg), &o); err != nil {
			errors = append(errors, err)
			continue
		}
		messages = append(messages, o)
	}
	wg.Wait()
	return messages, errors
}

// Subscribe to a kafka topic and send messages received via a dispatcher.
func Subscribe(topic string, consumer sarama.Consumer, fetchCount int, dispatcher chan<- string, wg *sync.WaitGroup) error {
	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		return fmt.Errorf("Error retrieving partitionList, details: %v", err)
	}
	initialOffset := sarama.OffsetOldest //get offset for the oldest message on the topic

	var i atomic.Int32
	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				dispatcher <- string(message.Value)
				if i.Add(1) >= int32(fetchCount) {
					wg.Done()
					return
				}
			}
		}(pc)
	}
	return nil
}
