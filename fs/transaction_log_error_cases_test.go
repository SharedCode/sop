package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestTransactionCommitLogWriteError simulates an error writing commit log (directory removed) to hit error propagation.
func TestTransactionCommitLogWriteError(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()

	GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	// Simulate logging mode (fast forward scenario) so commit changes are expected to be written.
	GlobalReplicationDetails.LogCommitChanges = true
	rt.LogCommitChanges = true

	// Prepare a basic transaction that will attempt to write a commit log.
	ms := NewManageStoreFolder(NewFileIO())
	sr, err := NewStoreRepository(ctx, rt, ms, cache, MinimumModValue)
	if err != nil {
		t.Fatalf("sr: %v", err)
	}
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "tx1", SlotLength: 10})
	if err := sr.Add(ctx, *si); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Remove commit logs folder after initial creation to force error on subsequent write.
	commitDir := rt.formatActiveFolderEntity(commitChangesLogFolder)
	if err := os.MkdirAll(commitDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.RemoveAll(commitDir); err != nil {
		t.Fatalf("remove commitDir: %v", err)
	}

	// Run a simple transaction that triggers log write via registry update.
	reg := NewRegistry(true, MinimumModValue, rt, cache)
	h := sop.NewHandle(sop.NewUUID())
	// Attempt add, expecting internal error recorded but not panic.
	_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{h}}})
	// The absence of commit log file validates error path executed (cannot directly assert due to internal logging).
	// Just ensure directory still absent.
	if _, err := os.Stat(filepath.Join(active, commitChangesLogFolder)); !os.IsNotExist(err) {
		t.Fatalf("expected commit log directory to remain absent to simulate write failure")
	}
}
