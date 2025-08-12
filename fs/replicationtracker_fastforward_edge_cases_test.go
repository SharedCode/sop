package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// TestFastForwardSkipsStoreReplication covers the branch where logData.First == nil so store replication is skipped.
// Ensures registry replicate executes with nil slices and log file is consumed.
func TestFastForwardSkipsStoreReplication(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()

	active := t.TempDir()
	passive := t.TempDir()

	GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	// Ensure registry hash-mod value file exists so fastForward can read it via StoreRepository helper.
	if _, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue); err != nil {
		t.Fatalf("NewStoreRepository init: %v", err)
	}

	// Craft commit log with First=nil (no stores) and all registry slices nil.
	payload := sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]{First: nil, Second: [][]sop.RegistryPayload[sop.Handle]{nil, nil, nil, nil}}
	ba, _ := encoding.DefaultMarshaler.Marshal(payload)
	commitDir := rt.formatActiveFolderEntity(commitChangesLogFolder)
	if err := NewFileIO().MkdirAll(ctx, commitDir, 0o755); err != nil {
		t.Fatalf("mkdir commitDir: %v", err)
	}
	fn := filepath.Join(commitDir, sop.NewUUID().String()+logFileExtension)
	if err := NewFileIO().WriteFile(ctx, fn, ba, permission); err != nil {
		t.Fatalf("write log: %v", err)
	}

	found, err := rt.fastForward(ctx)
	if err != nil {
		t.Fatalf("fastForward: %v", err)
	}
	if !found {
		t.Fatalf("expected to find log file")
	}
	// Store list should not be created in passive side since no stores replicated.
	if NewFileIO().Exists(ctx, filepath.Join(passive, storeListFilename)) {
		t.Fatalf("unexpected store list created in passive")
	}
	if _, err := os.Stat(fn); err == nil {
		t.Fatalf("log file not removed")
	}
}
