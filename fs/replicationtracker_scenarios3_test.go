package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

type getStructExErrCache struct{ sop.Cache }

func (c getStructExErrCache) GetStructEx(ctx context.Context, key string, v interface{}, ttl time.Duration) (bool, error) {
	return false, errors.New("l2 boom")
}

// Covers failover's early return when r.FailedToReplicate is already true.
func TestReplicationTracker_Failover_EarlyReturn_FailedToReplicate(t *testing.T) {
	ctx := context.Background()
	// Preserve global.
	globalReplicationDetailsLocker.Lock()
	oldGlobal := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	t.Cleanup(func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = oldGlobal
		globalReplicationDetailsLocker.Unlock()
	})

	a := t.TempDir()
	b := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{a, b}, true, getStructExErrCache{Cache: mocks.NewMockClient()})
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	prev := rt.ActiveFolderToggler
	rt.FailedToReplicate = true
	if err := rt.failover(ctx); err != nil {
		t.Fatalf("failover unexpected error: %v", err)
	}
	if rt.ActiveFolderToggler != prev {
		t.Fatalf("expected no toggle when FailedToReplicate true")
	}
	// Ensure no passive status written as we returned early.
	if _, err := os.Stat(rt.formatPassiveFolderEntity(replicationStatusFilename)); err == nil {
		t.Fatalf("expected no passive status file on early return")
	}
}

// Covers failover's early return when global already reflects the opposite toggler.
func TestReplicationTracker_Failover_EarlyReturn_GlobalAlreadyToggled(t *testing.T) {
	ctx := context.Background()
	globalReplicationDetailsLocker.Lock()
	oldGlobal := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	t.Cleanup(func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = oldGlobal
		globalReplicationDetailsLocker.Unlock()
	})

	a := t.TempDir()
	b := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	// Set global to indicate a prior flip already occurred.
	globalReplicationDetailsLocker.Lock()
	GlobalReplicationDetails.ActiveFolderToggler = !rt.ActiveFolderToggler
	globalReplicationDetailsLocker.Unlock()

	prev := rt.ActiveFolderToggler
	if err := rt.failover(ctx); err != nil {
		t.Fatalf("failover unexpected error: %v", err)
	}
	if rt.ActiveFolderToggler != prev {
		t.Fatalf("expected no toggle when global already toggled")
	}
}

// Covers readStatusFromHomeFolder branch where both active and passive status files are missing.
func TestReplicationTracker_ReadStatus_BothFilesMissing(t *testing.T) {
	ctx := context.Background()
	globalReplicationDetailsLocker.Lock()
	oldGlobal := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	t.Cleanup(func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = oldGlobal
		globalReplicationDetailsLocker.Unlock()
	})

	a := t.TempDir()
	b := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	// Both files are missing; ActiveFolderToggler should remain default true.
	if !rt.ActiveFolderToggler {
		t.Fatalf("expected default active folder when no status files present")
	}
}

// Covers readStatusFromHomeFolder branch where both files exist and the passive file is newer,
// which should flip ActiveFolderToggler and read the passive status content.
func TestReplicationTracker_ReadStatus_PassiveNewerFlips(t *testing.T) {
	ctx := context.Background()
	globalReplicationDetailsLocker.Lock()
	oldGlobal := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	t.Cleanup(func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = oldGlobal
		globalReplicationDetailsLocker.Unlock()
	})

	a := t.TempDir()
	b := t.TempDir()

	// Prepare status files on both sides with different modtimes.
	// Active side (older)
	actPath := filepath.Join(a, replicationStatusFilename)
	actContent, _ := encoding.Marshal(ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false, LogCommitChanges: false})
	if err := os.WriteFile(actPath, actContent, 0o644); err != nil {
		t.Fatalf("write active: %v", err)
	}
	// Passive side (newer)
	pasPath := filepath.Join(b, replicationStatusFilename)
	pasContent, _ := encoding.Marshal(ReplicationTrackedDetails{ActiveFolderToggler: false, FailedToReplicate: true, LogCommitChanges: true})
	if err := os.WriteFile(pasPath, pasContent, 0o644); err != nil {
		t.Fatalf("write passive: %v", err)
	}
	// Set modtimes: active older, passive newer.
	older := time.Now().Add(-2 * time.Hour)
	newer := time.Now()
	os.Chtimes(actPath, older, older)
	os.Chtimes(pasPath, newer, newer)

	rt, err := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	// Expect flip to passive (index 1) because passive file is newer, and fields read from passive content.
	if rt.ActiveFolderToggler {
		t.Fatalf("expected toggler flipped to false (passive newer)")
	}
	if !rt.FailedToReplicate || !rt.LogCommitChanges {
		t.Fatalf("expected fields populated from passive status file")
	}
}

// Covers handleFailedToReplicate path that writes replication status when not previously failed.
func TestReplicationTracker_HandleFailedToReplicate_WritesStatus(t *testing.T) {
	ctx := context.Background()
	globalReplicationDetailsLocker.Lock()
	oldGlobal := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	t.Cleanup(func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = oldGlobal
		globalReplicationDetailsLocker.Unlock()
	})

	a := t.TempDir()
	b := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	// Precondition
	if rt.FailedToReplicate {
		t.Fatalf("unexpected pre-failed state")
	}
	rt.handleFailedToReplicate(ctx)
	if !rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate true")
	}
	// Status file written on active side
	if _, err := os.Stat(rt.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
		t.Fatalf("expected replication status file on active: %v", err)
	}
	// Global flag set
	globalReplicationDetailsLocker.Lock()
	gf := GlobalReplicationDetails != nil && GlobalReplicationDetails.FailedToReplicate
	globalReplicationDetailsLocker.Unlock()
	if !gf {
		t.Fatalf("expected global FailedToReplicate true")
	}
}

// Covers NewReplicationTracker readStatusFromHomeFolder error when active status file has invalid JSON.
func TestReplicationTracker_New_ReadStatus_InvalidJSON(t *testing.T) {
	ctx := context.Background()
	globalReplicationDetailsLocker.Lock()
	oldGlobal := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	t.Cleanup(func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = oldGlobal
		globalReplicationDetailsLocker.Unlock()
	})

	a := t.TempDir()
	b := t.TempDir()
	// Write invalid JSON to active repl status file
	if err := os.WriteFile(filepath.Join(a, replicationStatusFilename), []byte("not-json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient()); err == nil {
		t.Fatalf("expected error due to invalid status JSON")
	}
}

// Covers failover path continuing despite syncWithL2Cache pull error; expects flip and status write on passive.
func TestReplicationTracker_Failover_SyncL2ErrorStillFlips(t *testing.T) {
	ctx := context.Background()
	globalReplicationDetailsLocker.Lock()
	oldGlobal := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	t.Cleanup(func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = oldGlobal
		globalReplicationDetailsLocker.Unlock()
	})

	a := t.TempDir()
	b := t.TempDir()
	// Use a cache that errors on GetStructEx to exercise warn branch.
	rt, err := NewReplicationTracker(ctx, []string{a, b}, true, getStructExErrCache{Cache: mocks.NewMockClient()})
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}

	// Trigger failover-qualified error with rollbackSucceeded=false.
	ioErr := sop.Error{Code: sop.FailoverQualifiedError, Err: errors.New("boom")}
	rt.HandleReplicationRelatedError(ctx, ioErr, nil, false)

	// Expect toggler flipped to passive (false), and status file written on passive.
	if rt.ActiveFolderToggler {
		t.Fatalf("expected ActiveFolderToggler flipped to false")
	}
	if _, err := os.Stat(filepath.Join(b, replicationStatusFilename)); err != nil {
		t.Fatalf("expected passive status file written: %v", err)
	}
}

// Covers handleFailedToReplicate early return when GlobalReplicationDetails is already marked failed.
func TestReplicationTracker_HandleFailedToReplicate_GlobalAlreadyFailed(t *testing.T) {
	ctx := context.Background()

	// Save and set global replication details to an already-failed state.
	globalReplicationDetailsLocker.Lock()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: true}
	globalReplicationDetailsLocker.Unlock()
	t.Cleanup(func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = prev
		globalReplicationDetailsLocker.Unlock()
	})

	a := t.TempDir()
	b := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}

	// Invoke handler; should set local flag and return without writing repl status file.
	rt.handleFailedToReplicate(ctx)
	if !rt.FailedToReplicate {
		t.Fatalf("expected local FailedToReplicate set to true")
	}
	if _, err := os.Stat(rt.formatActiveFolderEntity(replicationStatusFilename)); err == nil {
		t.Fatalf("unexpected replication status file written to active")
	}
	if _, err := os.Stat(rt.formatPassiveFolderEntity(replicationStatusFilename)); err == nil {
		t.Fatalf("unexpected replication status file written to passive")
	}
}

// Covers failover error path when writeReplicationStatus fails because the passive status filename collides with a directory.
func TestReplicationTracker_Failover_WriteReplicationStatusError(t *testing.T) {
	ctx := context.Background()

	// Ensure clean global state.
	globalReplicationDetailsLocker.Lock()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	t.Cleanup(func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = prev
		globalReplicationDetailsLocker.Unlock()
	})

	a := t.TempDir()
	b := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}

	// Create a directory at the passive status filename to force write error in failover.
	pasStat := filepath.Join(rt.getPassiveBaseFolder(), replicationStatusFilename)
	if err := os.MkdirAll(pasStat, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Trigger failover by calling HandleReplicationRelatedError with a failover-qualified code and rollbackSucceeded=false.
	ioErr := sop.Error{Code: sop.FailoverQualifiedError, Err: errors.New("invalid argument")}
	rt.HandleReplicationRelatedError(ctx, ioErr, nil, false)
	// Expect that ActiveFolderToggler did not change due to error writing status file.
	if rt.ActiveFolderToggler != true {
		t.Fatalf("expected no toggler change on failover error")
	}
}

// Table-driven tests for readStatusFromHomeFolder covering cases:
// - neither file exists (keeps default toggler)
// - only passive exists (reads and flips to passive)
// - both exist with passive newer (flips to passive)
func Test_readStatusFromHomeFolder_Table(t *testing.T) {
	ctx := context.Background()
	baseA := filepath.Join(t.TempDir(), "a")
	baseB := filepath.Join(t.TempDir(), "b")

	// Helper to write a minimal replication status file
	writeStat := func(dir string) error {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		f := filepath.Join(dir, replicationStatusFilename)
		return os.WriteFile(f, []byte("{}"), 0o644)
	}

	// Case 1: no files present
	// Isolate global state used by replication tracker
	globalReplicationDetailsLocker.Lock()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	defer func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = prev
		globalReplicationDetailsLocker.Unlock()
	}()

	rt1, _ := NewReplicationTracker(ctx, []string{baseA, baseB}, true, mocks.NewMockClient())
	// Ensure no files exist
	_ = os.Remove(filepath.Join(baseA, replicationStatusFilename))
	_ = os.Remove(filepath.Join(baseB, replicationStatusFilename))
	if err := rt1.readStatusFromHomeFolder(ctx); err != nil {
		t.Fatalf("case1 read: %v", err)
	}
	if !rt1.ActiveFolderToggler {
		t.Fatalf("case1: expected default active folder toggler true")
	}

	// Case 2: only passive has status -> flip to passive
	rt2, _ := NewReplicationTracker(ctx, []string{baseA, baseB}, true, mocks.NewMockClient())
	if err := writeStat(baseB); err != nil {
		t.Fatalf("write passive: %v", err)
	}
	if err := rt2.readStatusFromHomeFolder(ctx); err != nil {
		t.Fatalf("case2 read: %v", err)
	}
	if rt2.ActiveFolderToggler {
		t.Fatalf("case2: expected toggler flipped to passive (false)")
	}

	// Case 3: both exist; make passive newer -> flip
	rt3, _ := NewReplicationTracker(ctx, []string{baseA, baseB}, true, mocks.NewMockClient())
	if err := writeStat(baseA); err != nil {
		t.Fatalf("write active: %v", err)
	}
	if err := writeStat(baseB); err != nil {
		t.Fatalf("write passive: %v", err)
	}
	// Ensure passive mtime is later by touching it after active
	later := time.Now().Add(2 * time.Second)
	_ = os.Chtimes(filepath.Join(baseB, replicationStatusFilename), later, later)
	if err := rt3.readStatusFromHomeFolder(ctx); err != nil {
		t.Fatalf("case3 read: %v", err)
	}
	if rt3.ActiveFolderToggler {
		t.Fatalf("case3: expected toggler flipped to passive (false)")
	}
}
