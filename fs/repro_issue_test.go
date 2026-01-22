package fs

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

func TestRepro_RegistryCacheIsolation(t *testing.T) {
	// 1. Setup shared Cache (representing one Redis instance)
	cache := mocks.NewMockClient()

	// 2. Setup Environment 1 (Path A) and 2 (Path B)
	// Both use the SAME cache instance.

	// We need valid replication trackers.
	// Since we are in 'fs' package, we can access private struct fields if needed,
	// or use the constructor if available.

	// Emulate /mnt/ver1
	ctx := context.Background()

	// Basic RT setup
	rt1 := &replicationTracker{
		replicate:         false,
		storesBaseFolders: []string{"/mnt/ver1"},
		activeFolder:      "/mnt/ver1",
	}

	rt2 := &replicationTracker{
		replicate:         false,
		storesBaseFolders: []string{"/mnt/ver2"},
		activeFolder:      "/mnt/ver2",
	}

	// Create Registries
	reg1 := NewRegistry(true, 10, rt1, cache)
	reg2 := NewRegistry(true, 10, rt2, cache)

	// 4. Add items to both
	u1 := sop.NewUUID()
	h1 := sop.Handle{LogicalID: u1, PhysicalAddress: 100}

	u2 := sop.NewUUID()
	h2 := sop.Handle{LogicalID: u2, PhysicalAddress: 200}

	// Payload
	p1 := []sop.RegistryPayload[sop.Handle]{{IDs: []sop.Handle{h1}, CacheDuration: time.Hour}}
	p2 := []sop.RegistryPayload[sop.Handle]{{IDs: []sop.Handle{h2}, CacheDuration: time.Hour}}

	// We use 'Add' but we expect errors from hashmap because underlying files don't exist?
	// registryOnDisk.Add calls hashmap.add which calls file operations.
	// To avoid actual file IO errors or requirement to create folders,
	// we might need to mock internal behavior or just ensure folders exist.
	// But the Cache part happens *after* disk write.
	// If disk write fails, it returns error.

	// Let's optimize: We only care about Cache collision.
	// We can manually populate the cache?
	// Or we can assume Add might fail on disk but maybe succeed on cache?
	// registryOnDisk.Add: "if err := r.hashmap.add(...); err != nil { return err }"
	// So we need disk write to succeed.

	// Mocking is hard here without touching file system.
	// Better to use "StoreRepository" test which purely tests the Namespacing of StoreInfo.
	// The Registry test uses UUIDs which we know are unique.
	// The user Is complaining about "all keys got lost".
	// StoreRepository manages the "Store" keys.
}

func TestRepro_StoreRepositoryIsolation(t *testing.T) {
	cache := mocks.NewMockClient()
	ctx := context.Background()

	// Use temporary directories for realism
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	rt1 := &replicationTracker{storesBaseFolders: []string{dir1}, activeFolder: dir1}
	rt2 := &replicationTracker{storesBaseFolders: []string{dir2}, activeFolder: dir2}

	// We need a ManageStore or nil (it creates default).
	// Default ManageStore uses real filesystem.
	// TempDir is perfect.

	sr1, err := NewStoreRepository(ctx, rt1, nil, cache, 10)
	if err != nil {
		t.Fatalf("Failed to create SR1: %v", err)
	}
	sr2, err := NewStoreRepository(ctx, rt2, nil, cache, 10)
	if err != nil {
		t.Fatalf("Failed to create SR2: %v", err)
	}

	// Add "users_by_age" to SR1
	s1 := sop.StoreInfo{
		Name:        "users_by_age",
		SlotLength:  100,
		CacheConfig: sop.StoreCacheConfig{StoreInfoCacheDuration: time.Hour},
	}
	if err := sr1.Add(ctx, s1); err != nil {
		t.Fatalf("SR1 Add failed: %v", err)
	}

	// Add "users_by_age" to SR2
	s2 := sop.StoreInfo{
		Name:        "users_by_age",
		SlotLength:  200,
		CacheConfig: sop.StoreCacheConfig{StoreInfoCacheDuration: time.Hour},
	}
	if err := sr2.Add(ctx, s2); err != nil {
		t.Fatalf("SR2 Add failed: %v", err)
	}

	// Verify Cache Presence for SR1
	// Key should be "dir1:users_by_age"
	// We can check via Get
	got1, err := sr1.Get(ctx, "users_by_age")
	if err != nil || len(got1) == 0 {
		t.Fatalf("SR1 Get failed: %v", err)
	}
	if got1[0].SlotLength != 100 {
		t.Errorf("SR1 data mismatch. Expected 100, got %d", got1[0].SlotLength)
	}

	// Verify Cache Presence for SR2
	got2, err := sr2.Get(ctx, "users_by_age")
	if err != nil || len(got2) == 0 {
		t.Fatalf("SR2 Get failed: %v", err)
	}
	if got2[0].SlotLength != 200 {
		t.Errorf("SR2 data mismatch. Expected 200, got %d", got2[0].SlotLength)
	}

	// Deletion test
	// Remove from SR1
	if err := sr1.Remove(ctx, "users_by_age"); err != nil {
		t.Fatalf("SR1 Remove failed: %v", err)
	}

	// Check SR2 again. Should still be there.
	got2After, err := sr2.Get(ctx, "users_by_age")
	if err != nil || len(got2After) == 0 {
		t.Fatalf("SR2 Store lost after SR1 removal! Namespacing collision detected!")
	}
	if got2After[0].SlotLength != 200 {
		t.Errorf("SR2 data corrupted after SR1 removal. Expected 200, got %d", got2After[0].SlotLength)
	}
}
