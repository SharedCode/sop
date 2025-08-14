package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"

	"errors"
	"time"
)

type fakeDirectIO struct{}

func (f *fakeDirectIO) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) {
	if dir := filepath.Dir(filename); dir != "." {
		_ = os.MkdirAll(dir, perm)
	}
	return os.OpenFile(filename, flag, perm)
}
func (f *fakeDirectIO) WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.WriteAt(block, offset)
}
func (f *fakeDirectIO) ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.ReadAt(block, offset)
}
func (f *fakeDirectIO) Close(file *os.File) error { return file.Close() }

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

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}
func indexOf(s, sub string) int {
	if len(sub) == 0 {
		return 0
	}
	max := len(s) - len(sub) + 1
	for i := 0; i < max; i++ {
		if s[i] == sub[0] && s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

var _ = errors.New


// Consolidated hashmap scenarios covering: find/update/delete, scan branch, error branches
// (lock fail, partial read/write, open fail), helper functions (offset calc, getIDs), fetch flow,
// mark delete + not found, setupNewFile lock failure, and isZeroData.
func TestHashmap_AllScenarios(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	cache := mocks.NewMockClient()
	hm := newHashmap(true, 32, rt, cache)

	// --- Scenario: write + read + delete lifecycle ---
	table := "regcase"
	id := sop.NewUUID()
	old := DirectIOSim
	DirectIOSim = &fakeDirectIO{}
	t.Cleanup(func() { DirectIOSim = old })
	frd, err := hm.findOneFileRegion(ctx, true, table, id)
	if err != nil {
		t.Fatalf("findOneFileRegion(write): %v", err)
	}
	frd.handle = sop.NewHandle(id)
	if err := hm.updateFileRegion(ctx, []fileRegionDetails{frd}); err != nil {
		t.Fatalf("updateFileRegion: %v", err)
	}
	frdRead, err := hm.findOneFileRegion(ctx, false, table, id)
	if err != nil || frdRead.handle.LogicalID != id {
		t.Fatalf("read locate mismatch: %v", err)
	}
	if err := hm.markDeleteFileRegion(ctx, []fileRegionDetails{frdRead}); err != nil {
		t.Fatalf("markDelete: %v", err)
	}
	if _, err := hm.findOneFileRegion(ctx, false, table, id); err == nil {
		t.Fatalf("expected not found after delete")
	}

	// --- Scenario: scan branch (handle in non-ideal slot) ---
	hmScan := newHashmap(true, 64, rt, cache)
	id2 := sop.NewUUID()
	bo, ideal := hmScan.getBlockOffsetAndHandleInBlockOffset(id2)
	// Place the handle at a non-ideal offset to force a scan. Ensure it stays within block bounds.
	var alt int64 = ideal + sop.HandleSizeInBytes
	if alt+int64(sop.HandleSizeInBytes) > blockSize {
		alt = 0 // wrap to start if it would overflow the block
	}
	blk := make([]byte, blockSize)
	m := sop.NewHandle(id2)
	// marshal via encoding marshaler
	mar := encoding.NewHandleMarshaler()
	hb, _ := mar.Marshal(m, make([]byte, 0, sop.HandleSizeInBytes))
	copy(blk[alt:alt+int64(len(hb))], hb)
	scanDir := filepath.Join(base, "scanmm")
	os.MkdirAll(scanDir, 0o755)
	segFile := filepath.Join(scanDir, "scanmm-1"+registryFileExtension)
	f, _ := os.OpenFile(segFile, os.O_CREATE|os.O_RDWR, 0o644)
	f.Truncate(hmScan.getSegmentFileSize())
	f.WriteAt(blk, bo)
	f.Close()
	if r, err := hmScan.fetch(ctx, "scanmm", []sop.UUID{id2}); err != nil || len(r) != 1 {
		t.Fatalf("scan fetch: %v", err)
	}

	// --- Scenario: setupNewFile lock acquisition failure ---
	mcLockFail := &mockCacheHashmap{base: mocks.NewMockClient(), lockFail: true}
	hmLock := newHashmap(true, 16, rt, mcLockFail)
	DirectIOSim = &failingDirectIO{}
	if _, err := hmLock.findOneFileRegion(ctx, true, "tblLock", sop.NewUUID()); err == nil {
		t.Fatalf("expected lock fail")
	}

	// --- Scenario: partial read & partial write in updateFileRegion ---
	mcAlways := &mockCacheHashmap{base: mocks.NewMockClient(), isLockedAlways: true}
	hmPart := newHashmap(true, 16, rt, mcAlways)
	fd := &failingDirectIO{partialRead: true}
	DirectIOSim = fd
	id3 := sop.NewUUID()
	frd3, err := hmPart.findOneFileRegion(ctx, true, "tblP", id3)
	if err != nil {
		t.Fatalf("prep partial: %v", err)
	}
	if err := hmPart.updateFileRegion(ctx, []fileRegionDetails{{dio: frd3.dio, blockOffset: frd3.blockOffset, handleInBlockOffset: frd3.handleInBlockOffset, handle: sop.NewHandle(id3)}}); err == nil {
		t.Fatalf("expected partial read err")
	}
	fd.partialRead = false
	fd.partialWrite = true
	if err := hmPart.updateFileRegion(ctx, []fileRegionDetails{{dio: frd3.dio, blockOffset: frd3.blockOffset, handleInBlockOffset: frd3.handleInBlockOffset, handle: sop.NewHandle(id3)}}); err == nil {
		t.Fatalf("expected partial write err")
	}

	// --- Scenario: write error in updateFileBlockRegion (err != nil branch) ---
	// Covered separately from partial write above which exercises (err == nil && n != blockSize).
	dioWriteErr := newFileDirectIO()
	segFileWE := filepath.Join(base, "writeerr-seg.reg")
	if err := dioWriteErr.open(ctx, segFileWE, os.O_CREATE|os.O_RDWR, permission); err != nil {
		t.Fatalf("open seg: %v", err)
	}
	// Seed a full block so read succeeds (avoids partial read branch).
	seed := make([]byte, blockSize)
	if n, err := dioWriteErr.file.WriteAt(seed, 0); err != nil || n != blockSize {
		t.Fatalf("seed full block: n=%d err=%v", n, err)
	}
	dioWriteErr.file.Close()
	dioRO := newFileDirectIO()
	if err := dioRO.open(ctx, segFileWE, os.O_RDONLY, permission); err != nil {
		t.Fatalf("ro open: %v", err)
	}
	handleData := make([]byte, sop.HandleSizeInBytes)
	if err := hm.updateFileBlockRegion(ctx, dioRO, 0, 0, handleData); err == nil {
		t.Fatalf("expected write error branch")
	}

	// --- Scenario: open failure path ---
	fd2 := &failingDirectIO{openErr: os.ErrPermission}
	DirectIOSim = fd2
	if _, err := newHashmap(false, 8, rt, mocks.NewMockClient()).findOneFileRegion(ctx, false, "tblOpen", sop.NewUUID()); err == nil {
		t.Fatalf("expected open error")
	}

	// --- Scenario: helpers getBlockOffset, getIDs, isZeroData ---
	high, low := id.Split()
	_ = high
	_ = low
	if boCalc, _ := hm.getBlockOffsetAndHandleInBlockOffset(id); boCalc < 0 {
		t.Fatalf("offset negative")
	}
	ids := getIDs([]sop.Handle{sop.NewHandle(id)})
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("getIDs mismatch")
	}
	if !isZeroData(make([]byte, sop.HandleSizeInBytes)) {
		t.Fatalf("isZeroData false")
	}

	// Close to ensure close path executed.
	if err := hm.close(); err != nil {
		t.Fatalf("close err: %v", err)
	}
}

// Additional: fetch returns only existing IDs when mixed (existing + missing)
func TestHashmap_Fetch_MixedIDs_Scenario(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	hm := newHashmap(true, 32, rt, mocks.NewMockClient())
	idExisting := sop.NewUUID()
	frd, err := hm.findOneFileRegion(ctx, true, "tblmix", idExisting)
	if err != nil { t.Fatalf("prep frd: %v", err) }
	frd.handle = sop.NewHandle(idExisting)
	if err := hm.updateFileRegion(ctx, []fileRegionDetails{frd}); err != nil { t.Fatalf("seed write: %v", err) }
	missing := sop.NewUUID()
	r, err := hm.fetch(ctx, "tblmix", []sop.UUID{idExisting, missing})
	if err != nil || len(r) != 1 || r[0].LogicalID != idExisting { t.Fatalf("unexpected fetch result: %v %+v", err, r) }
}


