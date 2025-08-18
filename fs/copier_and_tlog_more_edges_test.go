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

// Ensures CopyToPassiveFolders returns an error when registry source directory cannot be read.
func Test_CopyToPassiveFolders_MissingRegistrySourceDir(t *testing.T) {
	ctx := context.Background()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")
	cache := mocks.NewMockClient()

	// Isolate global to keep toggler deterministic
	globalReplicationDetailsLocker.Lock()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	defer func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = prev
		globalReplicationDetailsLocker.Unlock()
	}()

	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	sr, err := NewStoreRepository(ctx, rt, nil, cache, 0)
	if err != nil {
		t.Fatalf("sr: %v", err)
	}

	// Seed a store so copier will attempt to copy its registry table
	si := sop.StoreInfo{Name: "s1", RegistryTable: sop.FormatRegistryTable("s1")}
	if err := sr.Add(ctx, si); err != nil {
		t.Fatalf("add: %v", err)
	}
	// Do NOT create the registry source directory on active, so copyFilesByExtension's ReadDir fails.
	if err := sr.CopyToPassiveFolders(ctx); err == nil {
		t.Fatalf("expected error when source registry directory is missing")
	}
}

// Ensures TransactionLog.GetOne returns nil when lock isn't acquired (another process holds it).
func Test_TransactionLog_GetOne_LockMissReturnsNil(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	rt, _ := NewReplicationTracker(ctx, []string{filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "b")}, true, cache)
	tl := NewTransactionLog(cache, rt)

	// Pre-acquire the hour lock using a different LockKey (different LockID) to simulate another worker owning it.
	other := cache.CreateLockKeys([]string{"HBP"})
	ok, _, err := cache.Lock(ctx, time.Hour, other)
	if err != nil || !ok {
		t.Fatalf("failed to acquire pre-lock: %v ok=%v", err, ok)
	}
	// GetOne should return nils without error when lock isn't available.
	tid, hour, recs, err := tl.GetOne(ctx)
	if err != nil || !tid.IsNil() || hour != "" || recs != nil {
		t.Fatalf("expected nil result on lock miss; got tid=%v hour=%q recs=%v err=%v", tid, hour, recs, err)
	}
}

// Validates CopyToPassiveFolders successfully copies store list, store info, and registry segment files.
func Test_CopyToPassiveFolders_Success(t *testing.T) {
	ctx := context.Background()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")
	cache := mocks.NewMockClient()

	// Isolate global toggler
	globalReplicationDetailsLocker.Lock()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	t.Cleanup(func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = prev
		globalReplicationDetailsLocker.Unlock()
	})

	// Initialize tracker and a store repo with a known registry table
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	sr, err := NewStoreRepository(ctx, rt, nil, cache, MinimumModValue)
	if err != nil {
		t.Fatalf("sr: %v", err)
	}

	// Seed one store and its registry segment file in active
	store := sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 8})
	if err := sr.Add(ctx, *store); err != nil {
		t.Fatalf("add: %v", err)
	}
	// Create registry segment file in active registry table folder
	regDir := filepath.Join(active, store.RegistryTable)
	os.MkdirAll(regDir, 0o755)
	segPath := filepath.Join(regDir, store.RegistryTable+"-1"+registryFileExtension)
	os.WriteFile(segPath, []byte("data"), 0o644)

	// Run copy
	if err := sr.CopyToPassiveFolders(ctx); err != nil {
		t.Fatalf("CopyToPassiveFolders: %v", err)
	}

	// Validate passive now has store list, store info, and the registry segment file
	if _, err := os.Stat(filepath.Join(passive, storeListFilename)); err != nil {
		t.Fatalf("passive missing store list: %v", err)
	}
	if _, err := os.Stat(filepath.Join(passive, store.Name, storeInfoFilename)); err != nil {
		t.Fatalf("passive missing store info: %v", err)
	}
	if _, err := os.Stat(filepath.Join(passive, store.RegistryTable, store.RegistryTable+"-1"+registryFileExtension)); err != nil {
		t.Fatalf("passive missing registry segment: %v", err)
	}
}
