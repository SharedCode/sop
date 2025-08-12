package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// mockCacheWarn implements sop.Cache subset to trigger warning paths (SetStruct/Delete failures) while allowing rest.
type mockCacheWarn struct{ inner sop.Cache }

func newMockCacheWarn() mockCacheWarn { return mockCacheWarn{inner: mocks.NewMockClient()} }
func (m mockCacheWarn) Set(ctx context.Context, key, value string, d time.Duration) error {
	return m.inner.Set(ctx, key, value, d)
}
func (m mockCacheWarn) Get(ctx context.Context, key string) (bool, string, error) {
	return m.inner.Get(ctx, key)
}
func (m mockCacheWarn) GetEx(ctx context.Context, key string, d time.Duration) (bool, string, error) {
	return m.inner.GetEx(ctx, key, d)
}
func (m mockCacheWarn) SetStruct(ctx context.Context, key string, value interface{}, d time.Duration) error {
	return errors.New("fail setstruct")
}
func (m mockCacheWarn) GetStruct(ctx context.Context, key string, target interface{}) (bool, error) {
	return m.inner.GetStruct(ctx, key, target)
}
func (m mockCacheWarn) GetStructEx(ctx context.Context, key string, target interface{}, d time.Duration) (bool, error) {
	return m.inner.GetStructEx(ctx, key, target, d)
}
func (m mockCacheWarn) Delete(ctx context.Context, keys []string) (bool, error) {
	return false, errors.New("fail delete")
}
func (m mockCacheWarn) Ping(ctx context.Context) error { return m.inner.Ping(ctx) }
func (m mockCacheWarn) FormatLockKey(k string) string  { return m.inner.FormatLockKey(k) }
func (m mockCacheWarn) CreateLockKeys(keys []string) []*sop.LockKey {
	return m.inner.CreateLockKeys(keys)
}
func (m mockCacheWarn) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	return m.inner.CreateLockKeysForIDs(keys)
}
func (m mockCacheWarn) IsLockedTTL(ctx context.Context, d time.Duration, lockKeys []*sop.LockKey) (bool, error) {
	return m.inner.IsLockedTTL(ctx, d, lockKeys)
}
func (m mockCacheWarn) Lock(ctx context.Context, d time.Duration, lockKeys []*sop.LockKey) (bool, sop.UUID, error) {
	return m.inner.Lock(ctx, d, lockKeys)
}
func (m mockCacheWarn) IsLocked(ctx context.Context, lockKeys []*sop.LockKey) (bool, error) {
	return m.inner.IsLocked(ctx, lockKeys)
}
func (m mockCacheWarn) IsLockedByOthers(ctx context.Context, lockKeyNames []string) (bool, error) {
	return m.inner.IsLockedByOthers(ctx, lockKeyNames)
}
func (m mockCacheWarn) Unlock(ctx context.Context, lockKeys []*sop.LockKey) error {
	return m.inner.Unlock(ctx, lockKeys)
}
func (m mockCacheWarn) Clear(ctx context.Context) error { return m.inner.Clear(ctx) }

// fakeManageStore to simulate createStore error for Add branch after lock.
// (unused manage store failure path skipped to keep focus on uncovered branches)

func TestStoreRepositoryMoreBranches(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()

	// 1. Duplicate Add rejection (isolated rt/cache).
	rt1, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	sr1, _ := NewStoreRepository(ctx, rt1, nil, mocks.NewMockClient(), 0)
	s := *sop.NewStoreInfo(sop.StoreOptions{Name: "dup", SlotLength: 10})
	if err := sr1.Add(ctx, s); err != nil {
		t.Fatalf("Add first: %v", err)
	}
	if err := sr1.Add(ctx, s); err == nil {
		t.Fatalf("expected duplicate add error")
	}

	// 2. Add path triggers SetStruct warning (mock cache failure) but succeeds overall; then remove twice to exercise warning path without lock contention.
	rt2, _ := NewReplicationTracker(ctx, []string{t.TempDir()}, false, mocks.NewMockClient())
	sr2, _ := NewStoreRepository(ctx, rt2, nil, newMockCacheWarn(), 0)
	s2 := *sop.NewStoreInfo(sop.StoreOptions{Name: "warn", SlotLength: 10})
	if err := sr2.Add(ctx, s2); err != nil {
		t.Fatalf("Add warn: %v", err)
	}
	if err := sr2.Remove(ctx, s2.Name); err != nil {
		t.Fatalf("Remove warn: %v", err)
	}
	if err := sr2.Remove(ctx, s2.Name); err != nil {
		t.Fatalf("Remove warn second (expected warn path only): %v", err)
	}

	// 4. GetWithTTL partial cache miss load path (one cached, one from disk).
	rt3, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	sr3, _ := NewStoreRepository(ctx, rt3, nil, mocks.NewMockClient(), 0)
	a := *sop.NewStoreInfo(sop.StoreOptions{Name: "a1", SlotLength: 5})
	b := *sop.NewStoreInfo(sop.StoreOptions{Name: "b1", SlotLength: 5})
	if err := sr3.Add(ctx, a, b); err != nil {
		t.Fatalf("Add ab: %v", err)
	}
	// Prime cache only for a by Get call.
	if _, err := sr3.Get(ctx, a.Name); err != nil {
		t.Fatalf("prime get: %v", err)
	}
	got, err := sr3.GetWithTTL(ctx, false, 0, a.Name, b.Name)
	if err != nil || len(got) != 2 {
		t.Fatalf("GetWithTTL mixed path got %v err %v", got, err)
	}

	// 5. Update path write failure simulation by placing a directory where file should be to force write error.
	rt4, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	sr4, _ := NewStoreRepository(ctx, rt4, nil, mocks.NewMockClient(), 0)
	c := *sop.NewStoreInfo(sop.StoreOptions{Name: "c1", SlotLength: 5})
	if err := sr4.Add(ctx, c); err != nil {
		t.Fatalf("Add c: %v", err)
	}
	// Remove file and create directory with same name to cause write failure.
	infoFile := filepath.Join(base, c.Name, "storeinfo.txt")
	os.Remove(infoFile)
	os.Mkdir(infoFile, 0o755)
	upd := c
	upd.CountDelta = 1
	upd.CacheConfig.StoreInfoCacheDuration = time.Minute
	if _, err := sr4.Update(ctx, []sop.StoreInfo{upd}); err == nil {
		t.Fatalf("expected update write failure")
	}
}
