package fs

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sharedcode/sop"
)

// TestRetryIO_RetryableThenSuccess forces retryIO to retry twice then succeed.
func TestRetryIO_RetryableThenSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var attempts int32
	err := retryIO(ctx, func(context.Context) error {
		a := atomic.AddInt32(&attempts, 1)
		if a <= 2 {
			return sop.Error{Code: sop.FileIOError, Err: errors.New("transient")}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryIO unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}
