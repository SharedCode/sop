package cassandra

import (
	"context"
	"testing"
)

var ctx = context.Background()

func TestGetOne(t *testing.T) {

	tl := NewTransactionLog()
	uuid, _, r, err := tl.GetOne(ctx)
	if uuid.IsNil() {

	}
	if r == nil {

	}
	if err == nil {
		
	}
}
