package fs

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// fakeStuckCache implements minimal locking subset to force lock timeout.
type fakeStuckCache struct{}

func (f fakeStuckCache) CreateLockKeys(keys []string) []*sop.LockKey {
	lk := make([]*sop.LockKey, len(keys))
	for i, k := range keys {
		lk[i] = &sop.LockKey{Key: k}
	}
	return lk
}
func (f fakeStuckCache) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	lk := make([]*sop.LockKey, len(keys))
	for i, k := range keys {
		lk[i] = &sop.LockKey{Key: k.First, LockID: k.Second}
	}
	return lk
}
func (f fakeStuckCache) FormatLockKey(k string) string { return k }
func (f fakeStuckCache) Lock(ctx context.Context, d time.Duration, l []*sop.LockKey) (bool, sop.UUID, error) {
	return false, sop.NilUUID, nil
}
func (f fakeStuckCache) IsLocked(ctx context.Context, l []*sop.LockKey) (bool, error) {
	return false, nil
}
func (f fakeStuckCache) Unlock(ctx context.Context, l []*sop.LockKey) error { return nil }

// Unused methods
func (f fakeStuckCache) Set(context.Context, string, string, time.Duration) error {
	return errors.New("unused")
}
func (f fakeStuckCache) Get(context.Context, string) (bool, string, error) {
	return false, "", errors.New("unused")
}
func (f fakeStuckCache) GetEx(context.Context, string, time.Duration) (bool, string, error) {
	return false, "", errors.New("unused")
}
func (f fakeStuckCache) SetStruct(context.Context, string, interface{}, time.Duration) error {
	return errors.New("unused")
}
func (f fakeStuckCache) GetStruct(context.Context, string, interface{}) (bool, error) {
	return false, errors.New("unused")
}
func (f fakeStuckCache) GetStructEx(context.Context, string, interface{}, time.Duration) (bool, error) {
	return false, errors.New("unused")
}
func (f fakeStuckCache) Delete(context.Context, []string) (bool, error) {
	return false, errors.New("unused")
}
func (f fakeStuckCache) Ping(context.Context) error { return errors.New("unused") }
func (f fakeStuckCache) IsLockedTTL(context.Context, time.Duration, []*sop.LockKey) (bool, error) {
	return false, nil
}
func (f fakeStuckCache) IsLockedByOthers(context.Context, []string) (bool, error) { return false, nil }
func (f fakeStuckCache) Clear(context.Context) error                              { return nil }

func TestUpdateFileBlockRegion_LockTimeout(t *testing.T) {
	t.Skip("cannot shorten production lock timeout without altering source; skipping long 30s timeout test")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	rt, _ := NewReplicationTracker(ctx, []string{t.TempDir()}, false, mocks.NewMockClient())
	hm := newHashmap(true, 4, rt, fakeStuckCache{})
	dio := newFileDirectIO()
	fn := t.TempDir() + "/seg-1.reg"
	if err := dio.open(ctx, fn, os.O_CREATE|os.O_RDWR, permission); err != nil {
		t.Fatalf("open: %v", err)
	}
	blk := dio.createAlignedBlock()
	if _, err := dio.writeAt(ctx, blk, 0); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	data := make([]byte, sop.HandleSizeInBytes)
	err := hm.updateFileBlockRegion(ctx, dio, 0, 0, data)
	if err == nil || !strings.Contains(err.Error(), "updateFileBlockRegion failed") {
		t.Fatalf("expected timeout error, got %v", err)
	}
}
