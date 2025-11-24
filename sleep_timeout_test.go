package sop

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTimedOut_WrapsContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := TimedOut(ctx, "transaction", start, 5*time.Second)
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}

	var te ErrTimeout
	if !errors.As(err, &te) {
		t.Fatalf("expected ErrTimeout, got %T: %v", err, err)
	}
	if te.Name != "transaction" {
		t.Fatalf("unexpected name: %q", te.Name)
	}
	if te.MaxTime != 5*time.Second {
		t.Fatalf("unexpected MaxTime: %v", te.MaxTime)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected errors.Is(err, context.Canceled) to be true; err=%v", err)
	}
}

func TestTimedOut_OperationDurationExceeded(t *testing.T) {
	// Save and restore Now to avoid leaking changes across tests.
	prevNow := Now
	defer func() { Now = prevNow }()

	// Start at a fixed point in time to make Now deterministic.
	start := time.Unix(0, 0)
	max := 100 * time.Millisecond

	// Make Now return a time just beyond start+max to trigger operation timeout.
	Now = func() time.Time { return start.Add(max + time.Millisecond) }

	ctx := context.Background()
	err := TimedOut(ctx, "lockFileBlockRegion", start, max)
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}

	var te ErrTimeout
	if !errors.As(err, &te) {
		t.Fatalf("expected ErrTimeout, got %T: %v", err, err)
	}
	if te.Name != "lockFileBlockRegion" {
		t.Fatalf("unexpected name: %q", te.Name)
	}
	if te.MaxTime != max {
		t.Fatalf("unexpected MaxTime: %v", te.MaxTime)
	}
	// No context cause expected.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("did not expect context cause, got err=%v", err)
	}
}
