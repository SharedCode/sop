package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// Focused table-driven tests to elevate coverage for StoreRepository lesser-hit branches.
func TestStoreRepositoryAdditionalPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("GetRegistryHashModValueReadsFromDiskWhenZero", func(t *testing.T) {
		base := t.TempDir()
		// replication disabled for simplicity
		rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
		sr, err := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
		if err != nil {
			t.Fatalf("NewStoreRepository: %v", err)
		}
		// Manually write the registry hash mod value file
		val := 777
		fn := filepath.Join(base, registryHashModValueFilename)
		if err := os.WriteFile(fn, []byte("777"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		got, err := sr.GetRegistryHashModValue(ctx)
		if err != nil || got != val {
			t.Fatalf("GetRegistryHashModValue got %d err %v", got, err)
		}
	})

	t.Run("GetAllReturnsNilWhenMissingAndAddCreatesListThenRemove", func(t *testing.T) {
		base := t.TempDir()
		rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
		sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
		// Initially no storelist file
		if sl, err := sr.GetAll(ctx); err != nil || sl != nil {
			t.Fatalf("expected nil, got %v err %v", sl, err)
		}
		// Add one store
		si := *sop.NewStoreInfo(sop.StoreOptions{Name: "sA", SlotLength: 10})
		if err := sr.Add(ctx, si); err != nil {
			t.Fatalf("Add: %v", err)
		}
		names, err := sr.GetAll(ctx)
		if err != nil || len(names) != 1 {
			t.Fatalf("GetAll after add: %v %v", names, err)
		}
		// Remove store
		if err := sr.Remove(ctx, "sA"); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		// Removing again should no-op (warn path)
		if err := sr.Remove(ctx, "sA"); err != nil {
			t.Fatalf("Remove second no-op: %v", err)
		}
	})

	t.Run("ReplicateSkipsWhenDisabledOrFailed", func(t *testing.T) {
		base := t.TempDir()
		rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
		sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
		si := *sop.NewStoreInfo(sop.StoreOptions{Name: "sB", SlotLength: 10})
		si2 := *sop.NewStoreInfo(sop.StoreOptions{Name: "sC", SlotLength: 10})
		// Replicate should be no-op when tracker.replicate=false
		if err := sr.Replicate(ctx, []sop.StoreInfo{si}); err != nil {
			t.Fatalf("Replicate disabled: %v", err)
		}
		// Enable replication and mark failed to replicate to test skip condition
		rt.replicate = true
		rt.FailedToReplicate = true
		if err := sr.Replicate(ctx, []sop.StoreInfo{si2}); err != nil {
			t.Fatalf("Replicate failed flag skip: %v", err)
		}
	})

	t.Run("UpdateUndoPathWhenStoreMissingReturnsNilPerImpl", func(t *testing.T) {
		base := t.TempDir()
		rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
		sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
		// Provide a store to update that does not yet exist -> expect error and empty result
		pay := []sop.StoreInfo{{Name: "ghost", CountDelta: 5, CacheConfig: sop.StoreCacheConfig{StoreInfoCacheDuration: time.Minute}}}
		if got, err := sr.Update(ctx, pay); err != nil || got != nil {
			t.Fatalf("expected nil result & nil err, got %v err %v", got, err)
		}
	})
}
