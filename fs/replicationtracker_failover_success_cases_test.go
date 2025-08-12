package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestReplicationTracker_FailoverSuccess validates the happy path of failover:
// - Status file written to passive path
// - ActiveFolderToggler flips
// - FailedToReplicate set
// - GlobalReplicationDetails updated and L2 cache sync doesn't regress state.
func TestReplicationTracker_FailoverSuccess(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")
	if err := os.MkdirAll(active, 0o755); err != nil {
		t.Fatalf("mkdir active: %v", err)
	}
	if err := os.MkdirAll(passive, 0o755); err != nil {
		t.Fatalf("mkdir passive: %v", err)
	}

	// Initialize global so local tracker matches and guard clauses don't early-return.
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}

	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}
	// Align local toggler with global preconditions (NewReplicationTracker sets true already, but reiterate).
	rt.ActiveFolderToggler = true

	// Precondition: ensure passive status file absent so we know we wrote it.
	pf := rt.formatPassiveFolderEntity(replicationStatusFilename)
	_ = os.Remove(pf)

	// Trigger failover via handle of failover-qualified error.
	ioErr := sop.Error{Code: sop.FailoverQualifiedError, Err: os.ErrInvalid}
	rt.HandleReplicationRelatedError(ctx, ioErr, nil, false)

	// Expectations.
	if !rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate true")
	}
	if rt.ActiveFolderToggler != false {
		t.Fatalf("expected toggler flipped to false (passive became active)")
	}
	// Global updated to copy.
	if GlobalReplicationDetails == nil || GlobalReplicationDetails.ActiveFolderToggler != rt.ActiveFolderToggler {
		t.Fatalf("global toggler mismatch")
	}
	if _, err := os.Stat(pf); err != nil {
		t.Fatalf("expected passive status file written: %v", err)
	}

	// Subsequent failover attempt should no-op (guard path) and not error.
	if err := rt.failover(ctx); err != nil {
		t.Fatalf("second failover should be guard no-op: %v", err)
	}
}
