package kafka

import (
	"fmt"
)

type Config struct {
	Brokers []string
	Topic string
}

var DefaultConfig Config = Config {
	Brokers: []string{"127.0.0.1:9093"},
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
