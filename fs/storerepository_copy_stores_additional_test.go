package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestStoreRepository_CopyToPassiveFolders_Success exercises multi-store copy including registry segment copying.
func TestStoreRepository_CopyToPassiveFolders_Success(t *testing.T) {
	ctx := context.Background()
	active := t.TempDir()
	passive := t.TempDir()
	cache := mocks.NewMockClient()
	GlobalReplicationDetails = nil

	// Create replication tracker & repo.
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	sr, err := NewStoreRepository(ctx, rt, nil, cache, 4)
	if err != nil {
		t.Fatalf("repo: %v", err)
	}

	// Define two stores.
	stores := []*sop.StoreInfo{
		sop.NewStoreInfo(sop.StoreOptions{Name: "sA", SlotLength: 4}),
		sop.NewStoreInfo(sop.StoreOptions{Name: "sB", SlotLength: 4}),
	}

	// Use repo Add API to create store list + storeinfo (ensures GetAll & Get succeed) then augment with registry segments.
	if err := sr.Add(ctx, *stores[0], *stores[1]); err != nil {
		t.Fatalf("sr.Add: %v", err)
	}

	for _, st := range stores {
		// Create registry segment directory & file (plus a subdir and unrelated file to exercise skip branches).
		regDir := filepath.Join(active, st.RegistryTable)
		if err := os.MkdirAll(regDir, 0o755); err != nil {
			t.Fatalf("mkdir reg dir: %v", err)
		}
		segFile := filepath.Join(regDir, st.RegistryTable+"-1"+registryFileExtension)
		if err := os.WriteFile(segFile, []byte("segment"), 0o644); err != nil {
			t.Fatalf("write segment: %v", err)
		}
		// Unrelated extension file and a subdirectory.
		_ = os.WriteFile(filepath.Join(regDir, "ignore.tmp"), []byte("x"), 0o644)
		_ = os.MkdirAll(filepath.Join(regDir, "subdir"), 0o755)
	}

	orig := rt.ActiveFolderToggler
	// Execute copy.
	if err := sr.CopyToPassiveFolders(ctx); err != nil {
		t.Fatalf("CopyToPassiveFolders: %v", err)
	}
	// Ensure toggler restored.
	if rt.ActiveFolderToggler != orig {
		t.Fatalf("expected ActiveFolderToggler restored")
	}
	// Validate passive side now has store list & storeinfo & segment files.
	if _, err := os.Stat(filepath.Join(passive, storeListFilename)); err != nil {
		t.Fatalf("passive store list missing: %v", err)
	}
	for _, st := range stores {
		if _, err := os.Stat(filepath.Join(passive, st.Name, storeInfoFilename)); err != nil {
			t.Fatalf("passive storeinfo missing: %v", err)
		}
		if _, err := os.Stat(filepath.Join(passive, st.RegistryTable, st.RegistryTable+"-1"+registryFileExtension)); err != nil {
			t.Fatalf("passive registry segment missing: %v", err)
		}
	}
}

// TestStoreRepository_CopyToPassiveFolders_SourceMissingSegmentDir covers error path when registry segment source dir missing.
func TestStoreRepository_CopyToPassiveFolders_SourceMissingSegmentDir(t *testing.T) {
	ctx := context.Background()
	active := t.TempDir()
	passive := t.TempDir()
	cache := mocks.NewMockClient()
	GlobalReplicationDetails = nil

	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	sr, _ := NewStoreRepository(ctx, rt, nil, cache, 4)

	st := sop.NewStoreInfo(sop.StoreOptions{Name: "sErr", SlotLength: 4})
	if err := sr.Add(ctx, *st); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Remove registry segment directory if any (should not exist) and deliberately pass missing directory scenario.
	regDir := filepath.Join(active, st.RegistryTable)
	os.RemoveAll(regDir) // ensure missing
	if err := sr.CopyToPassiveFolders(ctx); err == nil {
		t.Fatalf("expected error due to missing registry segment dir")
	}
}

// TestStoreRepository_CopyToPassiveFolders_TogglerFalse covers branch where original active folder toggler is false
// exercising the alternate src/dst path computation in copyStores.
func TestStoreRepository_CopyToPassiveFolders_TogglerFalse(t *testing.T) {
	ctx := context.Background()
	f1 := t.TempDir() // will be passive during seeding
	f2 := t.TempDir() // will be active during seeding since toggler false => active is index 1
	cache := mocks.NewMockClient()
	GlobalReplicationDetails = nil
	rt, err := NewReplicationTracker(ctx, []string{f1, f2}, true, cache)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	// Force active folder toggler to false so active base folder is f2.
	rt.ActiveFolderToggler = false
	sr, err := NewStoreRepository(ctx, rt, nil, cache, 4)
	if err != nil {
		t.Fatalf("repo: %v", err)
	}

	stores := []*sop.StoreInfo{
		sop.NewStoreInfo(sop.StoreOptions{Name: "sa", SlotLength: 4}),
		sop.NewStoreInfo(sop.StoreOptions{Name: "sb", SlotLength: 4}),
	}
	if err := sr.Add(ctx, *stores[0], *stores[1]); err != nil {
		t.Fatalf("sr.Add: %v", err)
	}
	for _, si := range stores {
		regDir := filepath.Join(f2, si.RegistryTable)
		if err := os.MkdirAll(regDir, 0o755); err != nil {
			t.Fatalf("mkdir regdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(regDir, si.RegistryTable+"-1"+registryFileExtension), []byte("seg"), 0o644); err != nil {
			t.Fatalf("segment: %v", err)
		}
	}

	if err := sr.CopyToPassiveFolders(ctx); err != nil {
		t.Fatalf("CopyToPassiveFolders: %v", err)
	}
	// After copy, destination (original passive f1) should now have the list, storeinfo & segment files.
	if _, err := os.Stat(filepath.Join(f1, storeListFilename)); err != nil {
		t.Fatalf("expected storelist in f1: %v", err)
	}
	for _, si := range stores {
		if _, err := os.Stat(filepath.Join(f1, si.Name, storeInfoFilename)); err != nil {
			t.Fatalf("missing storeinfo in f1: %v", err)
		}
		if _, err := os.Stat(filepath.Join(f1, si.RegistryTable, si.RegistryTable+"-1"+registryFileExtension)); err != nil {
			t.Fatalf("missing segment in f1: %v", err)
		}
	}
}

// TestStoreRepository_GetRegistryHashModValue_InvalidContent exercises the error path when the
// registry hash mod value file contains invalid (non-integer) content.
func TestStoreRepository_GetRegistryHashModValue_InvalidContent(t *testing.T) {
	ctx := context.Background()
	active := t.TempDir()
	passive := t.TempDir()
	GlobalReplicationDetails = nil
	// Seed invalid content file in active folder.
	if err := os.WriteFile(filepath.Join(active, registryHashModValueFilename), []byte("not-an-int"), 0o644); err != nil {
		t.Fatalf("seed invalid: %v", err)
	}
	// Create tracker and repo (registryHashModVal=0 so constructor won't overwrite file).
	cache := mocks.NewMockClient()
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	sr, err := NewStoreRepository(ctx, rt, nil, cache, 0)
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if _, err := sr.GetRegistryHashModValue(ctx); err == nil {
		t.Fatalf("expected Atoi error for invalid content")
	}
}

// TestStoreRepository_Add_DuplicateName covers duplicate detection branch in Add.
func TestStoreRepository_Add_DuplicateName(t *testing.T) {
	ctx := context.Background()
	active := t.TempDir()
	passive := t.TempDir()
	GlobalReplicationDetails = nil
	cache := mocks.NewMockClient()
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	sr, err := NewStoreRepository(ctx, rt, nil, cache, 0)
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	store := sop.StoreInfo{Name: "dup", RegistryTable: "dup_r"}
	if err := sr.Add(ctx, store); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if err := sr.Add(ctx, store); err == nil {
		t.Fatalf("expected duplicate add error")
	}
}
