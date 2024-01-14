package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
)

// Simple reference implementation of Kafka consumer for SOP. Please feel
// free to do adjustments and to integrate with your application cluster.
// Perhaps you need to use Consumer Group and/or use Subscribe model.
//
// SOP in_red_ck can operate though even if Kafka Consumer is not implemented.
// The transaction commit is capable of managing transaction "work area"/
// "for merging" leftover Nodes in time, but it is recommended to integrate
// with Kafka (producer & consumer) so your system can manage these leftover
// Nodes in a schedule basis, e.g. - once a night when traffic is not heavy.
//
// See the kafka DefaultConfig & Initialize function how to setup the kafka
// brokers & the topic.
//
// See the kafka producer.go "Enqueue" function for details how SOP sends the
// deleted Nodes' IDs and tables. BUT you only need to set the "brokers" & "topic"
// variables and the producer should start to "sense" that kafka sendmessage
// is working and then will start to skip issuing deletes for succeeding commits.
//
// ** Then make sure to setup the kafka consumer side, so you can schedule deletes
// on your desired intervals, and using your application to do that.
//
// You can use below simple Consumer if needed, but not necessary. Just provided as
// an example.

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
		config.Consumer.Fetch.Default = 4 * 1024 * 1024
		config.Consumer.MaxWaitTime = 500 * time.Millisecond
		config.Version = sarama.V2_6_0_0
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
func Dequeue[T any](ctx context.Context, count int) ([]T, error) {
	if !IsInitialized() {
		return nil, fmt.Errorf("Kafka is not initialized, please set kafka package's brokers & topic config")
	}
	var err error
	if consumer == nil {
		consumer, err = GetConsumer(nil)
		if err != nil {
			return nil, fmt.Errorf("Can't fetch %d messages as can't open a Consumer, details: %v", count, err)
		}
	}

	messages := make([]T, 0, count)
	receiverChannel := make(chan string, 1)
	var wg sync.WaitGroup

	if err := Subscribe(globalConfig.Topic, consumer.consumer, count, receiverChannel, &wg); err != nil {
		return nil, err
	}
	var lastErr error
	for msg := range receiverChannel {
		var o T
		if err := json.Unmarshal([]byte(msg), &o); err != nil {
			lastErr = err
			continue
		}
		messages = append(messages, o)
	}
	wg.Wait()
	close(receiverChannel)
	consumer.consumer.Close()
	consumer = nil
	return messages, lastErr
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

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				dispatcher <- string(message.Value)
				if i.Add(1) >= int32(fetchCount) {
					pc.Close()
					return
				}
			}
		}(pc)
	}
	return nil
}
