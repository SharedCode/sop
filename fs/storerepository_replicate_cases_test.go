package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

func TestStoreRepositoryReplicateWritesPassive(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()

	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
	sr, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue)
	if err != nil { t.Fatalf("NewStoreRepository: %v", err) }

	si := sop.NewStoreInfo(sop.StoreOptions{Name: "r1", SlotLength: 10})
	if err := sr.Add(ctx, *si); err != nil { t.Fatalf("Add: %v", err) }

	// Now replicate the updated store info explicitly
	si.CountDelta = 3
	si.Count = 3
	if err := sr.Replicate(ctx, []sop.StoreInfo{*si}); err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	// Expect file in passive side
	if _, err := os.Stat(filepath.Join(passive, "r1", storeInfoFilename)); err != nil {
		t.Fatalf("expected replicated storeinfo in passive: %v", err)
	}
}

func TestStoreRepositoryReplicateReturnsErrorOnWriteFailure(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()

	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
	sr, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue)
	if err != nil { t.Fatalf("NewStoreRepository: %v", err) }

	si := sop.NewStoreInfo(sop.StoreOptions{Name: "r2", SlotLength: 10})
	if err := sr.Add(ctx, *si); err != nil { t.Fatalf("Add: %v", err) }

	// Make the passive store directory read-only to force WriteFile failure.
	passiveStore := filepath.Join(passive, si.Name)
	// Ensure directory exists then remove any existing file and chmod the directory.
	if err := os.MkdirAll(passiveStore, 0o755); err != nil { t.Fatalf("mkdir passive store: %v", err) }
	_ = os.Remove(filepath.Join(passiveStore, storeInfoFilename))
	if err := os.Chmod(passiveStore, 0o555); err != nil { t.Fatalf("chmod passive store ro: %v", err) }

	si.Count = 1
	err = sr.Replicate(ctx, []sop.StoreInfo{*si})
	if err == nil {
		t.Fatalf("expected Replicate to fail due to read-only passive folder")
	}
	// Restore permissions so TempDir cleanup can proceed.
	_ = os.Chmod(passiveStore, 0o755)
}
