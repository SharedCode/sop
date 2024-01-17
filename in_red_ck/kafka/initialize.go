package kafka

import (
	"fmt"
)

// Config contains the global config for this kafka package.
type Config struct {
	// Kafka Brokers.
	Brokers []string
	// Kafka topic.
	Topic string
}

// DefaultConfig contains the default config for this package.
var DefaultConfig Config = Config{
	Brokers: []string{"localhost:9093"},
	Topic:   "sop-deleted-data",
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

// Returns this package's global config.
func GetConfig() Config {
	return globalConfig
}
