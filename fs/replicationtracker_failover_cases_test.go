package fs

import (
	"context"
	"errors"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestReplicationTracker_HandleReplicationRelatedErrorFailover triggers a failover by injecting
// a sop.Error with a qualifying code and rollback failure scenario.
func TestReplicationTracker_HandleReplicationRelatedErrorFailover(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base1 := t.TempDir()
	base2 := t.TempDir()
	// Reset global state to avoid leakage from prior tests.
	GlobalReplicationDetails = nil
	rt, err := NewReplicationTracker(ctx, []string{base1, base2}, true, l2)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	if rt.getActiveBaseFolder() != base1 {
		t.Fatalf("expected base1 active")
	}

	// Inject a failover-qualified sop.Error
	ioErr := sop.Error{Code: sop.FailoverQualifiedError + 1, Err: errors.New("io fail")}
	// Use rollbackSucceeded=false to satisfy logic path when not a restore sector failure.
	rt.HandleReplicationRelatedError(ctx, ioErr, nil, false)

	if rt.getActiveBaseFolder() != base2 {
		t.Fatalf("expected active folder switched to base2")
	}
	if !rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate set after failover")
	}
}

// TestReplicationTracker_HandleFailedToReplicate ensures the flag is set and persisted semantics simulated.
func TestReplicationTracker_HandleFailedToReplicate(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base1 := t.TempDir()
	base2 := t.TempDir()
	// Reset global state to avoid leakage from prior tests.
	GlobalReplicationDetails = nil
	rt, err := NewReplicationTracker(ctx, []string{base1, base2}, true, l2)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	if rt.FailedToReplicate {
		t.Fatalf("should start healthy")
	}

	rt.handleFailedToReplicate(ctx) // first call sets flags
	if !rt.FailedToReplicate {
		t.Fatalf("expected failure flag set")
	}
	// Second call should be no-op (idempotent)
	rt.handleFailedToReplicate(ctx)
}
