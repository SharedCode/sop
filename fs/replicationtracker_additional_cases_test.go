package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// Targets remaining uncovered branches in replicationtracker: handleFailedToReplicate early return paths,
// failover guard clauses when already failed or global toggler mismatch, and syncWithL2Cache pull miss log.
func TestReplicationTrackerAdditionalBranches(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")

	// Preserve original global replication details so we don't leak mutated state
	// (tests that construct a single-folder replication tracker assume ActiveFolderToggler == true).
	origGlobal := GlobalReplicationDetails
	defer func() {
		if origGlobal != nil {
			GlobalReplicationDetails = origGlobal
		} else {
			// Reset to a safe default expected by other tests (single-folder active index 0)
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
		}
	}()

	// Fresh tracker with replication disabled -> handleFailedToReplicate should early return.
	rtNoRep, _ := NewReplicationTracker(ctx, []string{active}, false, cache)
	rtNoRep.handleFailedToReplicate(ctx) // no effect expected

	// Enable replication; simulate already failed state so early return triggers inside handleFailedToReplicate.
	rtFail, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
	rtFail.handleFailedToReplicate(ctx) // should just copy failure state
	if !rtFail.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate true from global")
	}

	// Failover guard: when global already toggled differently or FailedToReplicate, failover returns nil quickly.
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: false, FailedToReplicate: false}
	rtGuard, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	// Set local toggler true so mismatch triggers guard
	rtGuard.ActiveFolderToggler = true
	if err := rtGuard.failover(ctx); err != nil {
		t.Fatalf("failover guard returned error: %v", err)
	}

	// Actual failover path: reset globals to match and not failed and trigger failover.
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}
	rtDo, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	rtDo.ActiveFolderToggler = true // align local with global so guard passes
	// seed status file to active so write to passive occurs in failover
	if err := os.MkdirAll(active, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := rtDo.writeReplicationStatus(ctx, rtDo.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	// Trigger failover via HandleReplicationRelatedError which exercises same internal branch reliably.
	ioErr := sop.Error{Code: sop.FailoverQualifiedError, Err: os.ErrInvalid}
	rtDo.HandleReplicationRelatedError(ctx, ioErr, nil, false)
	if !rtDo.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate true after error-driven failover")
	}

	// syncWithL2Cache pull miss (not found) branch: clear global and ensure no panic.
	GlobalReplicationDetails = nil
	rtPull, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err := rtPull.syncWithL2Cache(ctx, false); err != nil {
		t.Fatalf("pull miss: %v", err)
	}

	// logCommitChanges disabled path (LogCommitChanges false) no-op.
	if err := rtPull.logCommitChanges(ctx, sop.NewUUID(), nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("logCommitChanges disabled: %v", err)
	}
}
