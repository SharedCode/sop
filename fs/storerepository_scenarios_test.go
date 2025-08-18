package fs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// More coverage for CopyToPassiveFolders: copies registry segments and skips non-matching files.
func Test_StoreRepository_CopyToPassiveFolders_CopiesSegments(t *testing.T) {
	ctx := context.Background()
	active := t.TempDir()
	passive := t.TempDir()
	cache := mocks.NewMockClient()

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
	sr, err := NewStoreRepository(ctx, rt, nil, cache, 64)
	if err != nil {
		t.Fatalf("sr: %v", err)
	}

	// Seed one store via Add
	si := sop.StoreInfo{Name: "s1", RegistryTable: sop.FormatRegistryTable("s1")}
	if err := sr.Add(ctx, si); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Create registry segment and a non-matching file in active registry folder
	regDir := filepath.Join(active, si.RegistryTable)
	if err := os.MkdirAll(regDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seg := filepath.Join(regDir, si.RegistryTable+"-1"+registryFileExtension)
	if err := os.WriteFile(seg, []byte("seg"), 0o644); err != nil {
		t.Fatalf("write seg: %v", err)
	}
	junk := filepath.Join(regDir, "skip.tmp")
	_ = os.WriteFile(junk, []byte("x"), 0o644)

	if err := sr.CopyToPassiveFolders(ctx); err != nil {
		t.Fatalf("CopyToPassiveFolders: %v", err)
	}

	// Validate passive contents
	if _, err := os.Stat(filepath.Join(passive, storeListFilename)); err != nil {
		t.Fatalf("missing passive store list: %v", err)
	}
	if _, err := os.Stat(filepath.Join(passive, "s1", storeInfoFilename)); err != nil {
		t.Fatalf("missing passive storeinfo: %v", err)
	}
	// Registry segment copied
	if _, err := os.Stat(filepath.Join(passive, si.RegistryTable, si.RegistryTable+"-1"+registryFileExtension)); err != nil {
		t.Fatalf("missing passive segment: %v", err)
	}
	// Non-matching file should not be copied
	if _, err := os.Stat(filepath.Join(passive, si.RegistryTable, "skip.tmp")); err == nil {
		t.Fatalf("unexpected junk file copied")
	}
}

func Test_StoreRepository_GetAll_NoListFileAndError(t *testing.T) {
	ctx := context.Background()
	baseA := filepath.Join(t.TempDir(), "a")
	baseB := filepath.Join(t.TempDir(), "b")
	rt, err := NewReplicationTracker(ctx, []string{baseA, baseB}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	sr, err := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
	if err != nil {
		t.Fatalf("sr: %v", err)
	}
	// No store list file -> returns nil, nil
	sl, err := sr.GetAll(ctx)
	if err != nil || sl != nil {
		t.Fatalf("expected nil,nil when list file absent; got %v,%v", sl, err)
	}

	// Create a non-JSON file to trigger Unmarshal error path
	dl := newFileIOWithReplication(rt, sr.manageStore, true)
	if err := dl.createStore(ctx, ""); err != nil {
		t.Fatalf("create store folder: %v", err)
	}
	f := rt.formatActiveFolderEntity(storeListFilename)
	if err := os.WriteFile(f, []byte("not-json"), 0o644); err != nil {
		t.Fatalf("write storelist: %v", err)
	}
	if _, err := sr.GetAll(ctx); err == nil {
		t.Fatalf("expected unmarshal error")
	}
}

func Test_copyFile_Success(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	if err := os.WriteFile(src, []byte("abc"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	if b, _ := os.ReadFile(dst); string(b) != "abc" {
		t.Fatalf("unexpected content: %q", string(b))
	}
}

func Test_copyFile_Errors(t *testing.T) {
	dir := t.TempDir()
	// Non-existent source -> error
	if err := copyFile(filepath.Join(dir, "missing.txt"), filepath.Join(dir, "out.txt")); err == nil {
		t.Fatalf("expected error for missing source")
	}
	// Create a directory as destination parent but provide a directory path as file to force create error.
	src := filepath.Join(dir, "src.txt")
	if err := os.WriteFile(src, []byte("x"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	dstDir := filepath.Join(dir, "dstdir")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Attempt to create a file where a directory already exists should error
	if err := copyFile(src, dstDir); err == nil {
		t.Fatalf("expected error when target is an existing directory")
	}
}

func Test_StoreRepository_Add_Duplicate_And_CopyToPassive(t *testing.T) {
	ctx := context.Background()
	baseA := filepath.Join(t.TempDir(), "a")
	baseB := filepath.Join(t.TempDir(), "b")
	// Isolate from global replication state so active folder is deterministic in this test.
	globalReplicationDetailsLocker.Lock()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	defer func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = prev
		globalReplicationDetailsLocker.Unlock()
	}()

	rt, err := NewReplicationTracker(ctx, []string{baseA, baseB}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	sr, err := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
	if err != nil {
		t.Fatalf("sr: %v", err)
	}

	// Add one store
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 2})
	if err := sr.Add(ctx, *si); err != nil {
		t.Fatalf("add: %v", err)
	}
	// Duplicate add should error
	if err := sr.Add(ctx, *si); err == nil {
		t.Fatalf("expected duplicate add error")
	}

	// CopyToPassiveFolders should succeed even if no registry files yet
	// Write store info to ensure src registry table exists
	if _, err := sr.Update(ctx, []sop.StoreInfo{{
		Name: "s1", RegistryTable: sop.FormatRegistryTable("s1"), CacheConfig: *sop.NewStoreCacheConfig(0, false), Timestamp: 1,
	}}); err != nil {
		// Update may fail depending on cache lookups; write minimal store info file directly to simulate
		storeWriter := newFileIOWithReplication(rt, nil, true)
		ba, _ := encoding.Marshal(sop.StoreInfo{Name: "s1", RegistryTable: sop.FormatRegistryTable("s1")})
		_ = storeWriter.createStore(ctx, "s1")
		_ = storeWriter.write(ctx, fmt.Sprintf("%c%s%c%s", os.PathSeparator, "s1", os.PathSeparator, storeInfoFilename), ba)
	}
	// Create a registry segment file in the active source directory so copyFilesByExtension finds something to copy.
	srcDir := filepath.Join(rt.storesBaseFolders[0], sop.FormatRegistryTable("s1"))
	_ = os.MkdirAll(srcDir, 0o755)
	_ = os.WriteFile(filepath.Join(srcDir, sop.FormatRegistryTable("s1")+"-1"+registryFileExtension), []byte("seg"), 0o644)
	if err := sr.CopyToPassiveFolders(ctx); err != nil {
		t.Fatalf("CopyToPassiveFolders: %v", err)
	}
}

func Test_StoreRepository_Remove_ReplicateError(t *testing.T) {
	ctx := context.Background()
	baseA := filepath.Join(t.TempDir(), "a")
	baseB := filepath.Join(t.TempDir(), "b")
	rt, err := NewReplicationTracker(ctx, []string{baseA, baseB}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	sr, err := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
	if err != nil {
		t.Fatalf("sr: %v", err)
	}

	// Add two stores so remove updates list
	s1 := sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 2})
	s2 := sop.NewStoreInfo(sop.StoreOptions{Name: "s2", SlotLength: 2})
	if err := sr.Add(ctx, *s1, *s2); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Induce replicate error by creating a directory at passive storelist path
	passiveStoreList := filepath.Join(rt.getPassiveBaseFolder(), storeListFilename)
	_ = os.Remove(passiveStoreList)
	_ = os.MkdirAll(passiveStoreList, 0o755)
	if err := sr.Remove(ctx, "s2"); err == nil {
		t.Fatalf("expected replicate error during remove")
	}
}

func Test_StoreRepository_Add_Remove_LockFailures(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	// Pre-hold the store list lock
	lk := cache.CreateLockKeys([]string{"infs_sr"})
	ok, _, _ := cache.Lock(ctx, time.Hour, lk)
	if !ok {
		t.Fatalf("failed to pre-lock store list")
	}

	rt, _ := NewReplicationTracker(ctx, []string{t.TempDir(), t.TempDir()}, true, cache)
	sr, _ := NewStoreRepository(ctx, rt, nil, cache, 0)
	if err := sr.Add(ctx, *sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 2})); err == nil {
		t.Fatalf("expected lock failure on Add")
	}

	if err := sr.Remove(ctx, "s1"); err == nil {
		t.Fatalf("expected lock failure on Remove")
	}
	_ = cache.Unlock(ctx, lk)
}

func Test_StoreRepository_Update_UndoOnWriteError(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()

	active := filepath.Join(t.TempDir(), "active")
	passive := filepath.Join(t.TempDir(), "passive")

	// Fresh tracker with replication enabled
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("rt: %v", err)
	}

	sr, err := NewStoreRepository(ctx, rt, nil, cache, 0)
	if err != nil {
		t.Fatalf("sr: %v", err)
	}

	// Seed two stores via Add so files exist and cache is populated.
	sA := sop.StoreInfo{Name: "A", RegistryTable: sop.FormatRegistryTable("A")}
	sB := sop.StoreInfo{Name: "B", RegistryTable: sop.FormatRegistryTable("B")}
	if err := sr.Add(ctx, sA, sB); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Induce write error for B by replacing its storeinfo file with a directory.
	bInfoPath := filepath.Join(active, "B", storeInfoFilename)
	_ = os.Remove(bInfoPath)
	if err := os.MkdirAll(bInfoPath, 0o755); err != nil {
		t.Fatalf("mkdir collide: %v", err)
	}

	// Attempt update that will succeed for A then fail for B and trigger undo for A.
	deltas := []sop.StoreInfo{{Name: "A", CountDelta: 1}, {Name: "B", CountDelta: 2}}
	if _, err := sr.Update(ctx, deltas); err == nil {
		t.Fatalf("expected error from Update due to write collision on B")
	}

	// Validate that A's count was undone (remains 0) on disk.
	aInfoPath := filepath.Join(active, "A", storeInfoFilename)
	ba, err := os.ReadFile(aInfoPath)
	if err != nil {
		t.Fatalf("read A info: %v", err)
	}
	var gotA sop.StoreInfo
	if err := json.Unmarshal(ba, &gotA); err != nil {
		t.Fatalf("unmarshal A info: %v", err)
	}
	if gotA.Count != 0 {
		t.Fatalf("expected A.Count undone to 0, got %d", gotA.Count)
	}
}

// Happy-path Update on two stores covering straight-line write + cache set.
func Test_StoreRepository_Update_Success_Multi(t *testing.T) {
	// t.Parallel() removed to avoid racing with other tracker-using tests
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "a")
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)

	a := *sop.NewStoreInfo(sop.StoreOptions{Name: "sA", SlotLength: 5})
	b := *sop.NewStoreInfo(sop.StoreOptions{Name: "sB", SlotLength: 5})
	if err := sr.Add(ctx, a, b); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Apply deltas and verify persisted counts.
	a.CountDelta, b.CountDelta = 2, 3
	got, err := sr.Update(ctx, []sop.StoreInfo{a, b})
	if err != nil || len(got) != 2 {
		t.Fatalf("update err=%v got=%v", err, got)
	}

	// Read back
	stores, err := sr.Get(ctx, "sA", "sB")
	if err != nil || len(stores) != 2 {
		t.Fatalf("get err=%v stores=%v", err, stores)
	}
	var ca, cb int64
	for _, s := range stores {
		if s.Name == "sA" {
			ca = s.Count
		} else if s.Name == "sB" {
			cb = s.Count
		}
	}
	if ca != 2 || cb != 3 {
		t.Fatalf("unexpected counts a=%d b=%d", ca, cb)
	}
}
