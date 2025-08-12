package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// mockCacheDeleteWarn forces Delete failure (warning path) but supports other operations.
type mockCacheDeleteWarn struct{ sop.Cache }

func (m mockCacheDeleteWarn) Delete(ctx context.Context, keys []string) (bool, error) {
	return false, errors.New("fail delete")
}

// TestStoreRepositoryRemoveCacheDeleteWarning covers warning path when cache.Delete fails and ensures removal proceeds.
func TestStoreRepositoryRemoveCacheDeleteWarning(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	cache := mockCacheDeleteWarn{Cache: mocks.NewMockClient()}
	sr, _ := NewStoreRepository(ctx, rt, nil, cache, 0)
	s := *sop.NewStoreInfo(sop.StoreOptions{Name: "rmw", SlotLength: 10})
	if err := sr.Add(ctx, s); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := sr.Remove(ctx, s.Name); err != nil {
		t.Fatalf("Remove with cache delete warn: %v", err)
	}
	// Ensure store folder gone on disk.
	if _, err := os.Stat(filepath.Join(base, s.Name)); !os.IsNotExist(err) {
		t.Fatalf("expected store folder removed, stat err=%v", err)
	}
}

// TestFileIOReplicateRemoveStoreError simulates failure removing passive store during replicate actions.
// failingRemoveAll implements FileIO override to fail RemoveAll during replication to passive.
type failingRemoveAll struct {
	FileIO
	passiveRoot string
}

// Fail only when path is under passiveRoot and ends with x1 (simulate passive removal failure during replicate replay).
func (f failingRemoveAll) RemoveAll(ctx context.Context, path string) error {
	if strings.HasPrefix(path, f.passiveRoot) && strings.HasSuffix(path, string(os.PathSeparator)+"x1") {
		return errors.New("remove all fail")
	}
	return f.FileIO.RemoveAll(ctx, path)
}

func TestFileIOReplicateRemoveStoreError(t *testing.T) {
	ctx := context.Background()
	active := t.TempDir()
	passive := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())

	orig := FileIOSim
	FileIOSim = failingRemoveAll{FileIO: NewFileIO(), passiveRoot: passive}
	defer func() { FileIOSim = orig }()

	ms := NewManageStoreFolder(NewFileIO())
	fio := newFileIOWithReplication(rt, ms, true)
	if err := fio.createStore(ctx, "x1"); err != nil {
		t.Fatalf("createStore: %v", err)
	}
	if err := fio.removeStore(ctx, "x1"); err != nil {
		t.Fatalf("removeStore active: %v", err)
	}
	if err := fio.replicate(ctx); err == nil {
		t.Fatalf("expected replicate remove store error")
	}
}
