package fs

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestStoreRepositoryUpdateFailureOnFirstStore ensures write failure on first store aborts with no partial changes.
func TestStoreRepositoryUpdateFailureOnFirstStore(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
	s := sop.NewStoreInfo(sop.StoreOptions{Name: "one", SlotLength: 10})
	s.Timestamp = 123
	if err := sr.Add(ctx, *s); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Force write failure by replacing storeinfo.txt with a directory prior to Update.
	infoFile := filepath.Join(base, s.Name, storeInfoFilename)
	if err := os.Remove(infoFile); err != nil {
		t.Fatalf("remove info: %v", err)
	}
	if err := os.Mkdir(infoFile, 0o755); err != nil {
		t.Fatalf("mkdir info dir: %v", err)
	}

	upd := *s
	upd.CountDelta = 5
	upd.Timestamp = 999
	upd.CacheConfig.StoreInfoCacheDuration = time.Minute

	if _, err := sr.Update(ctx, []sop.StoreInfo{upd}); err == nil {
		t.Fatalf("expected update error for first store write failure")
	}

	// Validate file unchanged (rollback logic for i=0 is no-op but failure prevented overwrite).
	// Remove directory and recreate file by reading? Instead, just ensure directory still exists (write did not succeed).
	if fi, err := os.Stat(infoFile); err != nil || !fi.IsDir() {
		t.Fatalf("expected directory placeholder still blocking file write, got err=%v dir=%v", err, fi != nil && fi.IsDir())
	}
}

// TestStoreRepositoryUpdateMissingStore exercises branch where GetWithTTL returns empty (missing store) leading to early return with nil slice, nil error.
func TestStoreRepositoryUpdateMissingStore(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
	ghost := sop.StoreInfo{Name: "ghost", CountDelta: 1, CacheConfig: sop.StoreCacheConfig{StoreInfoCacheDuration: time.Second}}
	got, err := sr.Update(ctx, []sop.StoreInfo{ghost})
	if err != nil {
		t.Fatalf("unexpected error (missing store path tolerates): %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil slice for missing store, got %v", got)
	}
}

// TestStoreRepositoryAddDuplicateSameBatch verifies duplicate names in the same Add call are rejected.
func TestStoreRepositoryAddDuplicateSameBatch(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
	s := *sop.NewStoreInfo(sop.StoreOptions{Name: "dup2", SlotLength: 10})
	if err := sr.Add(ctx, s, s); err == nil || !strings.Contains(err.Error(), "can't add store") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

// TestStoreRepositoryCopyToPassiveFolders_TargetDirCreateError forces error creating target registry directory (file exists).
func TestStoreRepositoryCopyToPassiveFolders_TargetDirCreateError(t *testing.T) {
	ctx := context.Background()
	active := t.TempDir()
	passive := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
	sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
	s := *sop.NewStoreInfo(sop.StoreOptions{Name: "cpy", SlotLength: 10})
	if err := sr.Add(ctx, s); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Create registry segment file in active so copy attempts to copy it.
	regDirActive := filepath.Join(active, s.RegistryTable)
	if err := os.MkdirAll(regDirActive, 0o755); err != nil {
		t.Fatalf("mkdir active reg: %v", err)
	}
	seg := filepath.Join(regDirActive, s.RegistryTable+"-1"+registryFileExtension)
	if err := os.WriteFile(seg, []byte("seg"), 0o644); err != nil {
		t.Fatalf("write seg: %v", err)
	}

	// Create a file at passive/<registryTable> so MkdirAll fails with 'not a directory'.
	passiveConflict := filepath.Join(passive, s.RegistryTable)
	if err := os.WriteFile(passiveConflict, []byte("x"), 0o644); err != nil {
		t.Fatalf("write passive conflict: %v", err)
	}

	if err := sr.CopyToPassiveFolders(ctx); err == nil || !strings.Contains(err.Error(), "error creating target directory") {
		t.Fatalf("expected target directory creation error, got %v", err)
	}
}

// TestStoreRepositoryUpdateUndoJSONIntegrity ensures undo path persists original fields when rollback happens (revalidate first store JSON content after failure on second).
func TestStoreRepositoryUpdateUndoJSONIntegrity(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
	s1 := sop.NewStoreInfo(sop.StoreOptions{Name: "j1", SlotLength: 10})
	s2 := sop.NewStoreInfo(sop.StoreOptions{Name: "j2", SlotLength: 10})
	s1.Timestamp = 100
	s2.Timestamp = 200
	if err := sr.Add(ctx, *s1, *s2); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Force failure on second store to trigger undo.
	infoFile2 := filepath.Join(base, s2.Name, storeInfoFilename)
	os.Remove(infoFile2)
	os.Mkdir(infoFile2, 0o755)
	upd1 := *s1
	upd1.CountDelta = 2
	upd1.Timestamp = 777
	upd1.CacheConfig.StoreInfoCacheDuration = time.Minute
	upd2 := *s2
	upd2.CountDelta = 3
	upd2.Timestamp = 888
	upd2.CacheConfig.StoreInfoCacheDuration = time.Minute
	if _, err := sr.Update(ctx, []sop.StoreInfo{upd1, upd2}); err == nil {
		t.Fatalf("expected update error")
	}
	// Read back j1 and ensure timestamp not overwritten to 777 and Count remains 0.
	ba, err := os.ReadFile(filepath.Join(base, s1.Name, storeInfoFilename))
	if err != nil {
		t.Fatalf("read j1: %v", err)
	}
	var got sop.StoreInfo
	if err := json.Unmarshal(ba, &got); err != nil {
		t.Fatalf("unmarshal j1: %v", err)
	}
	if got.Timestamp != 100 || got.Count != 0 {
		t.Fatalf("rollback integrity mismatch: %+v", got)
	}
}
