package kafka

import (
	"context"
	"testing"
)

var ctx = context.Background()

// Add prefix Test_ if want to test. Commented because we don't want to flood kafka w/ test messages.
func ProducerQuit(t *testing.T) {
	Initialize(DefaultConfig)
	Enqueue[string](ctx, []string{"foo"}...)
	CloseProducer()
	if producer != nil {
		t.Errorf("Singleton producer instance is still available after CloseProducer call, expected nil.")
	}
}
