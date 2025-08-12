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

// TestReinstateFailedDrives_Preconditions exercises early error returns.
func TestReinstateFailedDrives_Preconditions(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	base := t.TempDir()
	// replicate=false -> expect replicate flag error
	rtNoRep, _ := NewReplicationTracker(ctx, []string{base}, false, cache)
	rtNoRep.FailedToReplicate = true
	if err := rtNoRep.ReinstateFailedDrives(ctx); err == nil || !contains(err.Error(), "replicate flag is off") {
		t.Fatalf("expected replicate flag error, got %v", err)
	}
	// replicate true but FailedToReplicate false -> expect failedToReplicate error
	base2 := t.TempDir()
	base3 := t.TempDir()
	GlobalReplicationDetails = nil
	rtHealthy, _ := NewReplicationTracker(ctx, []string{base2, base3}, true, cache)
	if err := rtHealthy.ReinstateFailedDrives(ctx); err == nil || !contains(err.Error(), "FailedToReplicate is false") {
		t.Fatalf("expected FailedToReplicate precondition error, got %v", err)
	}
}

// TestReinstateFailedDrives_HappyFlow creates minimal state to walk through all stages with one empty commit log set.
func TestReinstateFailedDrives_HappyFlow(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()
	GlobalReplicationDetails = nil
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	rt.FailedToReplicate = true
	GlobalReplicationDetails.FailedToReplicate = true
	// Seed a store and registry plus a single commit log to force fastForward path.
	// 1. Create a store repository with one store and a registry hash mod value.
	storeRepo, _ := NewStoreRepository(ctx, rt, nil, cache, 64)
	store := sop.StoreInfo{Name: "s1", RegistryTable: "c1_r"}
	if err := storeRepo.Add(ctx, store); err != nil {
		t.Fatalf("Add store: %v", err)
	}
	// Create a dummy registry segment file so copyStores finds source registry directory.
	regSegDir := filepath.Join(active, store.RegistryTable)
	if err := os.MkdirAll(regSegDir, 0o755); err != nil {
		t.Fatalf("mkdir reg dir: %v", err)
	}
	segFile := filepath.Join(regSegDir, store.RegistryTable+"-1"+registryFileExtension)
	if err := os.WriteFile(segFile, []byte("segment"), 0o644); err != nil {
		t.Fatalf("write seg: %v", err)
	}
	// Create a registry and write a commit log entry.
	reg := NewRegistry(true, 64, rt, cache)
	// Prepare commit log folder and a log file containing empty payload arrays.
	logDir := filepath.Join(active, commitChangesLogFolder)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkdir logDir: %v", err)
	}
	payload, _ := encoding.DefaultMarshaler.Marshal(sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]{First: []sop.StoreInfo{store}, Second: [][]sop.RegistryPayload[sop.Handle]{nil, nil, nil, nil}})
	if err := os.WriteFile(filepath.Join(logDir, "0001"+logFileExtension), payload, 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
	_ = reg // ensure registry referenced
	if err := rt.ReinstateFailedDrives(ctx); err != nil {
		t.Fatalf("ReinstateFailedDrives: %v", err)
	}
	if rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate cleared")
	}
	if rt.LogCommitChanges {
		t.Fatalf("expected LogCommitChanges false after turnOnReplication")
	}
}

// TestReinstateFailedDrives_FastForwardError simulates a malformed commit log to cover error branch inside fastForward.
func TestReinstateFailedDrives_FastForwardError(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()
	GlobalReplicationDetails = nil
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	rt.FailedToReplicate = true
	GlobalReplicationDetails.FailedToReplicate = true
	// Seed commit log with invalid content so fastForward fails on Unmarshal.
	logDir := filepath.Join(active, commitChangesLogFolder)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkdir logDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "0002"+logFileExtension), []byte("bad"), 0o644); err != nil {
		t.Fatalf("write bad log: %v", err)
	}
	if err := rt.ReinstateFailedDrives(ctx); err == nil {
		t.Fatalf("expected error due to malformed log file")
	}
}
