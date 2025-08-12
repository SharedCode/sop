package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// Creates a minimal commit log payload and exercises fastForward + ReinstateFailedDrives happy path & guard errors.
func TestReplicationTrackerReinstateWorkflow(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()

	// Preserve original global state.
	orig := GlobalReplicationDetails
	defer func() { GlobalReplicationDetails = orig }()

	GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	rt.FailedToReplicate = true

	store := sop.StoreInfo{Name: "s1", Count: 1}
	hNew := sop.NewHandle(sop.NewUUID())
	hAdd := sop.NewHandle(sop.NewUUID())
	hUpd := sop.NewHandle(sop.NewUUID())
	hDel := hAdd // removed must exist
	payload := sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]{First: []sop.StoreInfo{store}, Second: [][]sop.RegistryPayload[sop.Handle]{
		{{RegistryTable: "reg", IDs: []sop.Handle{hNew}}},
		{{RegistryTable: "reg", IDs: []sop.Handle{hAdd}}},
		{{RegistryTable: "reg", IDs: []sop.Handle{hUpd}}},
		{{RegistryTable: "reg", IDs: []sop.Handle{hDel}}},
	}}
	ba, _ := encoding.DefaultMarshaler.Marshal(payload)
	commitDir := rt.formatActiveFolderEntity(commitChangesLogFolder)
	if err := os.MkdirAll(commitDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	logFn := filepath.Join(commitDir, sop.NewUUID().String()+logFileExtension)
	if err := os.WriteFile(logFn, ba, 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	if err := rt.ReinstateFailedDrives(ctx); err != nil {
		t.Fatalf("ReinstateFailedDrives: %v", err)
	}
	if rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate cleared")
	}
	if rt.LogCommitChanges {
		t.Fatalf("expected LogCommitChanges disabled after turnOnReplication")
	}
	if _, err := os.Stat(logFn); !os.IsNotExist(err) {
		t.Fatalf("expected log consumed & deleted")
	}

	// Guard errors separate
	rt2, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
	rt2.FailedToReplicate = false
	if err := rt2.ReinstateFailedDrives(ctx); err == nil {
		t.Fatalf("expected error when not failed")
	}
	rt3, _ := NewReplicationTracker(ctx, []string{active, passive}, false, l2)
	rt3.FailedToReplicate = true
	if err := rt3.ReinstateFailedDrives(ctx); err == nil {
		t.Fatalf("expected error when replicate flag off")
	}

	GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, LogCommitChanges: true, ActiveFolderToggler: true}
	rt4, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
	if err := rt4.turnOnReplication(ctx); err != nil {
		t.Fatalf("turnOnReplication: %v", err)
	}
	if GlobalReplicationDetails.FailedToReplicate || GlobalReplicationDetails.LogCommitChanges {
		t.Fatalf("expected globals cleared")
	}
	_ = time.Now()
}

// Separate fastForward test to exercise direct processing without full reinstate flow.
func TestReplicationTrackerFastForwardSingleLog(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()
	GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	rt.FailedToReplicate = true
	store := sop.StoreInfo{Name: "s2", Count: 1}
	h := sop.NewHandle(sop.NewUUID())
	payload := sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]{First: []sop.StoreInfo{store}, Second: [][]sop.RegistryPayload[sop.Handle]{
		{{RegistryTable: "reg2", IDs: []sop.Handle{h}}},
		{}, {}, {},
	}}
	ba, _ := encoding.DefaultMarshaler.Marshal(payload)
	commitDir := rt.formatActiveFolderEntity(commitChangesLogFolder)
	if err := os.MkdirAll(commitDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	logFn := filepath.Join(commitDir, sop.NewUUID().String()+logFileExtension)
	if err := os.WriteFile(logFn, ba, 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
	found, err := rt.fastForward(ctx)
	if err != nil {
		t.Fatalf("fastForward: %v", err)
	}
	if !found {
		t.Fatalf("expected found processed")
	}
	if _, err := os.Stat(logFn); !os.IsNotExist(err) {
		t.Fatalf("expected log deleted")
	}
}
