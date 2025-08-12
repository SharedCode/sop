package fs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// mockCacheHashmap extends mock redis client to simulate specific failure scenarios for hashmap branches.
type mockCacheHashmap struct {
	base           sop.Cache
	lockFail       bool
	isLockedAlways bool
}

func (m *mockCacheHashmap) Lock(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, sop.UUID, error) {
	if m.lockFail {
		return false, sop.NilUUID, nil
	}
	return m.base.Lock(ctx, d, lk)
}
func (m *mockCacheHashmap) IsLocked(ctx context.Context, lk []*sop.LockKey) (bool, error) {
	if m.isLockedAlways {
		return true, nil
	}
	return m.base.IsLocked(ctx, lk)
}

// Delegate other methods via embedding-like forwarders.
func (m *mockCacheHashmap) Set(ctx context.Context, key, value string, exp time.Duration) error {
	return m.base.Set(ctx, key, value, exp)
}
func (m *mockCacheHashmap) Get(ctx context.Context, key string) (bool, string, error) {
	return m.base.Get(ctx, key)
}
func (m *mockCacheHashmap) GetEx(ctx context.Context, key string, exp time.Duration) (bool, string, error) {
	return m.base.GetEx(ctx, key, exp)
}
func (m *mockCacheHashmap) Ping(ctx context.Context) error { return m.base.Ping(ctx) }
func (m *mockCacheHashmap) SetStruct(ctx context.Context, key string, v interface{}, exp time.Duration) error {
	return m.base.SetStruct(ctx, key, v, exp)
}
func (m *mockCacheHashmap) GetStruct(ctx context.Context, key string, target interface{}) (bool, error) {
	return m.base.GetStruct(ctx, key, target)
}
func (m *mockCacheHashmap) GetStructEx(ctx context.Context, key string, target interface{}, exp time.Duration) (bool, error) {
	return m.base.GetStructEx(ctx, key, target, exp)
}
func (m *mockCacheHashmap) Delete(ctx context.Context, keys []string) (bool, error) {
	return m.base.Delete(ctx, keys)
}
func (m *mockCacheHashmap) FormatLockKey(k string) string { return m.base.FormatLockKey(k) }
func (m *mockCacheHashmap) CreateLockKeys(keys []string) []*sop.LockKey {
	return m.base.CreateLockKeys(keys)
}
func (m *mockCacheHashmap) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	return m.base.CreateLockKeysForIDs(keys)
}
func (m *mockCacheHashmap) IsLockedTTL(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, error) {
	return m.base.IsLockedTTL(ctx, d, lk)
}
func (m *mockCacheHashmap) IsLockedByOthers(ctx context.Context, names []string) (bool, error) {
	return m.base.IsLockedByOthers(ctx, names)
}
func (m *mockCacheHashmap) Unlock(ctx context.Context, lk []*sop.LockKey) error {
	return m.base.Unlock(ctx, lk)
}
func (m *mockCacheHashmap) Clear(ctx context.Context) error { return m.base.Clear(ctx) }

// failingDirectIO allows inducing partial reads/writes and open errors.
type failingDirectIO struct {
	openErr      error
	partialRead  bool
	partialWrite bool
}

func (f *failingDirectIO) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) {
	if f.openErr != nil {
		return nil, f.openErr
	}
	if dir := filepath.Dir(filename); dir != "." {
		_ = os.MkdirAll(dir, perm)
	}
	return os.OpenFile(filename, flag, perm)
}
func (f *failingDirectIO) WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	if f.partialWrite {
		return len(block) - 10, nil
	}
	return file.WriteAt(block, offset)
}
func (f *failingDirectIO) ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	if f.partialRead {
		return len(block) - 20, nil
	}
	return file.ReadAt(block, offset)
}
func (f *failingDirectIO) Close(file *os.File) error { return file.Close() }

// TestHashmapErrorBranches targets guardrails and error paths not covered by main tests.
func TestHashmapErrorBranches(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())

	// 1. setupNewFile lock acquisition failure.
	mc := &mockCacheHashmap{base: mocks.NewMockClient(), lockFail: true}
	hm := newHashmap(true, 16, rt, mc)
	old := DirectIOSim
	DirectIOSim = &failingDirectIO{}
	t.Cleanup(func() { DirectIOSim = old })
	_, err := hm.findOneFileRegion(ctx, true, "tblA", sop.NewUUID())
	if err == nil || !contains(err.Error(), "can't acquire a lock") {
		t.Fatalf("expected lock failure, got %v", err)
	}

	// 2. updateFileBlockRegion partial read & write error branches.
	mc2 := &mockCacheHashmap{base: mocks.NewMockClient(), isLockedAlways: true}
	hm2 := newHashmap(true, 16, rt, mc2)
	fd := &failingDirectIO{partialRead: true}
	DirectIOSim = fd
	id := sop.NewUUID()
	frd, err := hm2.findOneFileRegion(ctx, true, "tblB", id)
	if err != nil {
		t.Fatalf("prep findOneFileRegion: %v", err)
	}
	// Force updateFileBlockRegion read partial
	if err := hm2.updateFileRegion(ctx, []fileRegionDetails{{dio: frd.dio, blockOffset: frd.blockOffset, handleInBlockOffset: frd.handleInBlockOffset, handle: sop.NewHandle(id)}}); err == nil || !contains(err.Error(), "partially") {
		t.Fatalf("expected partial read error, got %v", err)
	}
	// Now trigger partial write
	fd.partialRead = false
	fd.partialWrite = true
	if err := hm2.updateFileRegion(ctx, []fileRegionDetails{{dio: frd.dio, blockOffset: frd.blockOffset, handleInBlockOffset: frd.handleInBlockOffset, handle: sop.NewHandle(id)}}); err == nil || !contains(err.Error(), "partially") {
		t.Fatalf("expected partial write error, got %v", err)
	}

	// 3. findOneFileRegion read path encountering not found after EOF on short file (simulate via forcing openErr then non-write).
	fd2 := &failingDirectIO{openErr: errors.New("boom")}
	DirectIOSim = fd2
	hm3 := newHashmap(false, 8, rt, mocks.NewMockClient())
	if _, err := hm3.findOneFileRegion(ctx, false, "tblC", sop.NewUUID()); err == nil {
		t.Fatalf("expected error from open failure")
	}

	// 4. Loop guard (i>1000) - Not practically tested to avoid long run time.
	_ = fmt.Sprintf("")
}
