package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop/encoding"
)

// Helper to write a replication status file with provided details.
func writeStatus(t *testing.T, dir string, rtd ReplicationTrackedDetails) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	ba, _ := encoding.DefaultMarshaler.Marshal(rtd)
	if err := os.WriteFile(filepath.Join(dir, replicationStatusFilename), ba, 0o644); err != nil {
		t.Fatalf("write status: %v", err)
	}
}

// Active missing, passive present -> tracker should flip ActiveFolderToggler to passive (false) during initialization.
func TestReplicationTracker_readStatus_ActiveMissingPassivePresent(t *testing.T) {
	ctx := context.Background()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	t.Cleanup(func() { GlobalReplicationDetails = prev })

	active := t.TempDir()
	passive := t.TempDir()
	writeStatus(t, passive, ReplicationTrackedDetails{FailedToReplicate: false, ActiveFolderToggler: false})

	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, nil)
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	// Ensure we loaded replication status (FailedToReplicate should remain false) regardless of toggler final state.
	if rt.ReplicationTrackedDetails.FailedToReplicate != false {
		t.Fatalf("expected status read from passive file")
	}
}

// Both status files exist; passive newer than active -> should flip to passive.
func TestReplicationTracker_readStatus_PassiveNewer(t *testing.T) {
	ctx := context.Background()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	t.Cleanup(func() { GlobalReplicationDetails = prev })

	active := t.TempDir()
	passive := t.TempDir()
	writeStatus(t, active, ReplicationTrackedDetails{FailedToReplicate: false, ActiveFolderToggler: true})
	writeStatus(t, passive, ReplicationTrackedDetails{FailedToReplicate: false, ActiveFolderToggler: false})

	// Make passive file newer.
	pf := filepath.Join(passive, replicationStatusFilename)
	af := filepath.Join(active, replicationStatusFilename)
	past := time.Now().Add(-2 * time.Minute)
	if err := os.Chtimes(af, past, past); err != nil {
		t.Fatalf("chtimes active: %v", err)
	}
	time.Sleep(15 * time.Millisecond) // ensure modtime difference

	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, nil)
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	if rt.ActiveFolderToggler {
		t.Fatalf("expected flip to passive newer file; got active=true pf mod=%v af mod=%v", fileMod(pf), fileMod(af))
	}
}

// TestReplicationTracker_readStatus_ActiveExistsPassiveMissing covers the branch
// where only the active folder has a replication status file; toggler should remain unchanged.
func TestReplicationTracker_readStatus_ActiveExistsPassiveMissing(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	active := filepath.Join(base, "act")
	passive := filepath.Join(base, "pas")
	if err := os.MkdirAll(active, 0o755); err != nil {
		t.Fatalf("mkdir active: %v", err)
	}
	if err := os.MkdirAll(passive, 0o755); err != nil {
		t.Fatalf("mkdir passive: %v", err)
	}

	// Ensure we don't reuse global state from prior tests so readStatusFromHomeFolder logic executes.
	GlobalReplicationDetails = nil

	// Create only the active replication status file.
	content := []byte(`{"FailedToReplicate":false,"ActiveFolderToggler":true,"LogCommitChanges":false}`)
	if err := os.WriteFile(filepath.Join(active, replicationStatusFilename), content, 0o644); err != nil {
		t.Fatalf("write active replstat: %v", err)
	}

	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, nil)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}

	if !rt.ActiveFolderToggler {
		t.Fatalf("expected ActiveFolderToggler to remain true (active file present, passive missing)")
	}
}

// TestReplicationTracker_readStatus_BothExistPassiveOlder covers the branch where both folders
// have status files but the passive copy is older, so the toggler should remain pointing to the first folder.
func TestReplicationTracker_readStatus_BothExistPassiveOlder(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	active := filepath.Join(base, "act")
	passive := filepath.Join(base, "pas")
	if err := os.MkdirAll(active, 0o755); err != nil {
		t.Fatalf("mkdir active: %v", err)
	}
	if err := os.MkdirAll(passive, 0o755); err != nil {
		t.Fatalf("mkdir passive: %v", err)
	}

	GlobalReplicationDetails = nil

	content := []byte(`{"FailedToReplicate":false,"ActiveFolderToggler":true,"LogCommitChanges":false}`)
	// Create passive first so its mod time is earlier.
	if err := os.WriteFile(filepath.Join(passive, replicationStatusFilename), content, 0o644); err != nil {
		t.Fatalf("write passive replstat: %v", err)
	}
	// Small delay to ensure different mod time ordering (FS timestamp granularity can be 1s on some systems).
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(active, replicationStatusFilename), content, 0o644); err != nil {
		t.Fatalf("write active replstat: %v", err)
	}

	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, nil)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}

	if !rt.ActiveFolderToggler {
		t.Fatalf("expected ActiveFolderToggler to remain true (passive older than active)")
	}
}

func fileMod(fn string) time.Time {
	fi, _ := os.Stat(fn)
	if fi == nil {
		return time.Time{}
	}
	return fi.ModTime()
}
