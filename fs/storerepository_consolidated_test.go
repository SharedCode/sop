package fs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// Helper cache mocks consolidated from prior individual test files.
type mockCacheWarn struct{ inner sop.Cache }

func newMockCacheWarn() mockCacheWarn { return mockCacheWarn{inner: mocks.NewMockClient()} }
func (m mockCacheWarn) Set(ctx context.Context, k, v string, d time.Duration) error {
	return m.inner.Set(ctx, k, v, d)
}
func (m mockCacheWarn) Get(ctx context.Context, k string) (bool, string, error) {
	return m.inner.Get(ctx, k)
}
func (m mockCacheWarn) GetEx(ctx context.Context, k string, d time.Duration) (bool, string, error) {
	return m.inner.GetEx(ctx, k, d)
}
func (m mockCacheWarn) SetStruct(context.Context, string, interface{}, time.Duration) error {
	return errors.New("fail setstruct")
}
func (m mockCacheWarn) GetStruct(ctx context.Context, k string, tgt interface{}) (bool, error) {
	return m.inner.GetStruct(ctx, k, tgt)
}
func (m mockCacheWarn) GetStructEx(ctx context.Context, k string, tgt interface{}, d time.Duration) (bool, error) {
	return m.inner.GetStructEx(ctx, k, tgt, d)
}
func (m mockCacheWarn) Delete(context.Context, []string) (bool, error) {
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
func (m mockCacheWarn) IsLockedTTL(ctx context.Context, d time.Duration, ks []*sop.LockKey) (bool, error) {
	return m.inner.IsLockedTTL(ctx, d, ks)
}
func (m mockCacheWarn) Lock(ctx context.Context, d time.Duration, ks []*sop.LockKey) (bool, sop.UUID, error) {
	return m.inner.Lock(ctx, d, ks)
}
func (m mockCacheWarn) IsLocked(ctx context.Context, ks []*sop.LockKey) (bool, error) {
	return m.inner.IsLocked(ctx, ks)
}
func (m mockCacheWarn) IsLockedByOthers(ctx context.Context, names []string) (bool, error) {
	return m.inner.IsLockedByOthers(ctx, names)
}
func (m mockCacheWarn) Unlock(ctx context.Context, ks []*sop.LockKey) error {
	return m.inner.Unlock(ctx, ks)
}
func (m mockCacheWarn) Clear(ctx context.Context) error { return m.inner.Clear(ctx) }

type mockCacheDeleteWarn struct{ sop.Cache }

func (m mockCacheDeleteWarn) Delete(context.Context, []string) (bool, error) {
	return false, errors.New("fail delete")
}

type mockCacheSetStructWarn struct{ sop.Cache }

func (m mockCacheSetStructWarn) SetStruct(context.Context, string, interface{}, time.Duration) error {
	return errors.New("fail setstruct")
}

// failingRemoveAll triggers RemoveAll failure for passive replicated path ending with /x1.
type failingRemoveAll struct {
	FileIO
	passiveRoot string
}

func (f failingRemoveAll) RemoveAll(ctx context.Context, p string) error {
	if strings.HasPrefix(p, f.passiveRoot) && strings.HasSuffix(p, string(os.PathSeparator)+"x1") {
		return errors.New("remove all fail")
	}
	return f.FileIO.RemoveAll(ctx, p)
}

// Consolidated table-driven StoreRepository + related fileIO replication scenarios.
func TestStoreRepository_Scenarios(t *testing.T) {
	ctx := context.Background()

	scenarios := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"Basic Flow Add/Get/Update/Remove (replication disabled)", func(t *testing.T) {
			l2 := mocks.NewMockClient()
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
			sr, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue)
			if err != nil {
				t.Fatalf("NewStoreRepository: %v", err)
			}
			si := sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 10})
			sj := sop.NewStoreInfo(sop.StoreOptions{Name: "s2", SlotLength: 8})
			if err := sr.Add(ctx, *si, *sj); err != nil {
				t.Fatalf("Add: %v", err)
			}
			names, err := sr.GetAll(ctx)
			if err != nil || len(names) != 2 {
				t.Fatalf("GetAll: %v %v", names, err)
			}
			got, err := sr.Get(ctx, "s1", "s2")
			if err != nil || len(got) != 2 {
				t.Fatalf("Get: %v %v", got, err)
			}
			upd := got[0]
			upd.CountDelta = 5
			if _, err := sr.Update(ctx, []sop.StoreInfo{upd}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			if err := sr.Remove(ctx, "s1"); err != nil {
				t.Fatalf("Remove: %v", err)
			}
			if err := sr.Replicate(ctx, []sop.StoreInfo{upd}); err != nil {
				t.Fatalf("Replicate disabled no-op: %v", err)
			}
		}},
		{"GetRegistryHashModValue reads from existing file when zero", func(t *testing.T) {
			active, passive := t.TempDir(), t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			fn := filepath.Join(active, registryHashModValueFilename)
			if err := os.WriteFile(fn, []byte("777"), 0o644); err != nil {
				t.Fatalf("write: %v", err)
			}
			v, err := sr.GetRegistryHashModValue(ctx)
			if err != nil || v != 777 {
				t.Fatalf("got %d err %v", v, err)
			}
		}},
		{"CopyToPassiveFolders no stores (nil GetAll)", func(t *testing.T) {
			a, p := t.TempDir(), t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{a, p}, true, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 32)
			if err := sr.CopyToPassiveFolders(ctx); err != nil {
				t.Fatalf("CopyToPassiveFolders: %v", err)
			}
		}},
		{"CopyToPassiveFolders with store + segment file", func(t *testing.T) {
			a, p := t.TempDir(), t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{a, p}, true, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 32)
			st := sop.StoreInfo{Name: "s1", RegistryTable: "c1_r"}
			if err := sr.Add(ctx, st); err != nil {
				t.Fatalf("Add: %v", err)
			}
			segDir := filepath.Join(a, st.RegistryTable)
			os.MkdirAll(segDir, 0o755)
			segFile := filepath.Join(segDir, st.RegistryTable+"-1"+registryFileExtension)
			os.WriteFile(segFile, []byte("segment"), 0o644)
			if err := sr.CopyToPassiveFolders(ctx); err != nil {
				t.Fatalf("CopyToPassiveFolders: %v", err)
			}
			if _, err := os.Stat(filepath.Join(p, st.RegistryTable, st.RegistryTable+"-1"+registryFileExtension)); err != nil {
				t.Fatalf("copied seg missing: %v", err)
			}
		}},
		{"CopyToPassiveFolders E2E list + storeinfo + reg", func(t *testing.T) {
			a, p := t.TempDir(), t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{a, p}, true, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), MinimumModValue)
			si := sop.NewStoreInfo(sop.StoreOptions{Name: "c1", SlotLength: 10})
			if err := sr.Add(ctx, *si); err != nil {
				t.Fatalf("Add: %v", err)
			}
			regDir := filepath.Join(a, si.RegistryTable)
			os.MkdirAll(regDir, 0o755)
			os.WriteFile(filepath.Join(regDir, "0000-0000.reg"), []byte("x"), 0o644)
			if err := sr.CopyToPassiveFolders(ctx); err != nil {
				t.Fatalf("CopyToPassiveFolders: %v", err)
			}
			for _, fn := range []string{filepath.Join(p, storeListFilename), filepath.Join(p, si.Name, storeInfoFilename), filepath.Join(p, si.RegistryTable, "0000-0000.reg")} {
				if _, err := os.Stat(fn); err != nil {
					t.Fatalf("expected file: %s err %v", fn, err)
				}
			}
		}},
		{"CopyToPassiveFolders missing registry segment source dir error", func(t *testing.T) {
			a, p := t.TempDir(), t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{a, p}, true, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			store := *sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 10})
			sr.Add(ctx, store)
			if err := sr.CopyToPassiveFolders(ctx); err == nil || !strings.Contains(err.Error(), "error reading source directory") {
				t.Fatalf("expected source directory error, got %v", err)
			}
		}},
		{"CopyToPassiveFolders passive target dir create conflict", func(t *testing.T) {
			a, p := t.TempDir(), t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{a, p}, true, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			s := *sop.NewStoreInfo(sop.StoreOptions{Name: "cpy", SlotLength: 10})
			sr.Add(ctx, s)
			regDirActive := filepath.Join(a, s.RegistryTable)
			os.MkdirAll(regDirActive, 0o755)
			seg := filepath.Join(regDirActive, s.RegistryTable+"-1"+registryFileExtension)
			os.WriteFile(seg, []byte("seg"), 0o644)
			passiveConflict := filepath.Join(p, s.RegistryTable)
			os.WriteFile(passiveConflict, []byte("x"), 0o644)
			if err := sr.CopyToPassiveFolders(ctx); err == nil || !strings.Contains(err.Error(), "error creating target directory") {
				t.Fatalf("expected dir create error, got %v", err)
			}
		}},
		{"Replicate writes passive storeinfo", func(t *testing.T) {
			a, p := t.TempDir(), t.TempDir()
			l2 := mocks.NewMockClient()
			rt, _ := NewReplicationTracker(ctx, []string{a, p}, true, l2)
			sr, _ := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue)
			si := sop.NewStoreInfo(sop.StoreOptions{Name: "r1", SlotLength: 10})
			sr.Add(ctx, *si)
			si.CountDelta, si.Count = 3, 3
			if err := sr.Replicate(ctx, []sop.StoreInfo{*si}); err != nil {
				t.Fatalf("Replicate: %v", err)
			}
			if _, err := os.Stat(filepath.Join(p, si.Name, storeInfoFilename)); err != nil {
				t.Fatalf("missing replicated storeinfo: %v", err)
			}
		}},
		{"Replicate write failure passive path conflict", func(t *testing.T) {
			a, p := t.TempDir(), t.TempDir()
			l2 := mocks.NewMockClient()
			rt, _ := NewReplicationTracker(ctx, []string{a, p}, true, l2)
			sr, _ := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue)
			si := sop.NewStoreInfo(sop.StoreOptions{Name: "r2", SlotLength: 10})
			sr.Add(ctx, *si)
			// Replace passive store directory with a file; replicate should fail writing storeinfo due to ENOTDIR.
			passiveDir := filepath.Join(p, si.Name)
			os.RemoveAll(passiveDir)
			if err := os.WriteFile(passiveDir, []byte("x"), 0o644); err != nil {
				t.Fatalf("prep write file: %v", err)
			}
			si.Count = 1
			if err := sr.Replicate(ctx, []sop.StoreInfo{*si}); err == nil {
				t.Fatalf("expected replicate failure due to path conflict")
			}
		}},
		{"Replicate skips when disabled or failed flag set", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			si := *sop.NewStoreInfo(sop.StoreOptions{Name: "sB", SlotLength: 10})
			si2 := *sop.NewStoreInfo(sop.StoreOptions{Name: "sC", SlotLength: 10})
			if err := sr.Replicate(ctx, []sop.StoreInfo{si}); err != nil {
				t.Fatalf("disabled replicate: %v", err)
			}
			rt.replicate = true
			rt.FailedToReplicate = true
			if err := sr.Replicate(ctx, []sop.StoreInfo{si2}); err != nil {
				t.Fatalf("failed flag replicate skip: %v", err)
			}
		}},
		{"Remove cache delete warning path", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			cache := mockCacheDeleteWarn{Cache: mocks.NewMockClient()}
			sr, _ := NewStoreRepository(ctx, rt, nil, cache, 0)
			s := *sop.NewStoreInfo(sop.StoreOptions{Name: "rmw", SlotLength: 10})
			sr.Add(ctx, s)
			if err := sr.Remove(ctx, s.Name); err != nil {
				t.Fatalf("Remove warn path: %v", err)
			}
			if _, err := os.Stat(filepath.Join(base, s.Name)); !os.IsNotExist(err) {
				t.Fatalf("expected store folder removed, err=%v", err)
			}
		}},
		{"Update cache SetStruct warning", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			failingCache := mockCacheSetStructWarn{Cache: mocks.NewMockClient()}
			sr, _ := NewStoreRepository(ctx, rt, nil, failingCache, 0)
			s := *sop.NewStoreInfo(sop.StoreOptions{Name: "cw", SlotLength: 10})
			sr.Add(ctx, s)
			s.CountDelta = 2
			s.CacheConfig.StoreInfoCacheDuration = time.Second
			if _, err := sr.Update(ctx, []sop.StoreInfo{s}); err != nil {
				t.Fatalf("Update should succeed: %v", err)
			}
		}},
		{"Update undo on second store failure", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			s1 := sop.NewStoreInfo(sop.StoreOptions{Name: "aaa", SlotLength: 10})
			s2 := sop.NewStoreInfo(sop.StoreOptions{Name: "bbb", SlotLength: 10})
			s1.Timestamp, s2.Timestamp = 111, 222
			sr.Add(ctx, *s1, *s2)
			upd1 := *s1
			upd1.CountDelta, upd1.Timestamp = 3, 999
			upd1.CacheConfig.StoreInfoCacheDuration = time.Minute
			upd2 := *s2
			upd2.CountDelta, upd2.Timestamp = 5, 888
			upd2.CacheConfig.StoreInfoCacheDuration = time.Minute
			s2File := filepath.Join(base, upd2.Name, storeInfoFilename)
			os.Remove(s2File)
			os.Mkdir(s2File, 0o755)
			if _, err := sr.Update(ctx, []sop.StoreInfo{upd1, upd2}); err == nil {
				t.Fatalf("expected Update error")
			}
			ba, _ := os.ReadFile(filepath.Join(base, s1.Name, storeInfoFilename))
			var got sop.StoreInfo
			json.Unmarshal(ba, &got)
			if got.Count != 0 || got.Timestamp != 111 {
				t.Fatalf("rollback mismatch %+v", got)
			}
		}},
		{"Update failure on first store write", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			s := sop.NewStoreInfo(sop.StoreOptions{Name: "one", SlotLength: 10})
			s.Timestamp = 123
			sr.Add(ctx, *s)
			infoFile := filepath.Join(base, s.Name, storeInfoFilename)
			os.Remove(infoFile)
			os.Mkdir(infoFile, 0o755)
			upd := *s
			upd.CountDelta, upd.Timestamp = 5, 999
			upd.CacheConfig.StoreInfoCacheDuration = time.Minute
			if _, err := sr.Update(ctx, []sop.StoreInfo{upd}); err == nil {
				t.Fatalf("expected update error first store")
			}
			if fi, err := os.Stat(infoFile); err != nil || !fi.IsDir() {
				t.Fatalf("expected dir placeholder; err=%v", err)
			}
		}},
		{"Update missing store early return", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			ghost := sop.StoreInfo{Name: "ghost", CountDelta: 1, CacheConfig: sop.StoreCacheConfig{StoreInfoCacheDuration: time.Second}}
			got, err := sr.Update(ctx, []sop.StoreInfo{ghost})
			if err != nil || got != nil {
				t.Fatalf("expected nil slice nil err, got %v %v", got, err)
			}
		}},
		{"Add duplicate names in same batch", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			s := *sop.NewStoreInfo(sop.StoreOptions{Name: "dup2", SlotLength: 10})
			if err := sr.Add(ctx, s, s); err == nil || !strings.Contains(err.Error(), "can't add store") {
				t.Fatalf("expected duplicate error, got %v", err)
			}
		}},
		{"Add duplicate second call rejected & cache warn paths", func(t *testing.T) {
			base := t.TempDir()
			rt1, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			sr1, _ := NewStoreRepository(ctx, rt1, nil, mocks.NewMockClient(), 0)
			s := *sop.NewStoreInfo(sop.StoreOptions{Name: "dup", SlotLength: 10})
			sr1.Add(ctx, s)
			if err := sr1.Add(ctx, s); err == nil {
				t.Fatalf("expected duplicate add error")
			}
			rt2, _ := NewReplicationTracker(ctx, []string{t.TempDir()}, false, mocks.NewMockClient())
			sr2, _ := NewStoreRepository(ctx, rt2, nil, newMockCacheWarn(), 0)
			s2 := *sop.NewStoreInfo(sop.StoreOptions{Name: "warn", SlotLength: 10})
			sr2.Add(ctx, s2)
			sr2.Remove(ctx, s2.Name)
			sr2.Remove(ctx, s2.Name)
		}},
		{"GetWithTTL partial cache miss path", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			a := *sop.NewStoreInfo(sop.StoreOptions{Name: "a1", SlotLength: 5})
			b := *sop.NewStoreInfo(sop.StoreOptions{Name: "b1", SlotLength: 5})
			sr.Add(ctx, a, b)
			sr.Get(ctx, a.Name) // prime cache for a
			got, err := sr.GetWithTTL(ctx, false, 0, a.Name, b.Name)
			if err != nil || len(got) != 2 {
				t.Fatalf("GetWithTTL got %v err %v", got, err)
			}
		}},
		{"Update write failure due to directory in place of file", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			c := *sop.NewStoreInfo(sop.StoreOptions{Name: "c1", SlotLength: 5})
			sr.Add(ctx, c)
			infoFile := filepath.Join(base, c.Name, storeInfoFilename)
			os.Remove(infoFile)
			os.Mkdir(infoFile, 0o755)
			upd := c
			upd.CountDelta = 1
			upd.CacheConfig.StoreInfoCacheDuration = time.Minute
			if _, err := sr.Update(ctx, []sop.StoreInfo{upd}); err == nil {
				t.Fatalf("expected update write failure")
			}
		}},
		{"Update undo JSON integrity first store after second fails", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			s1 := sop.NewStoreInfo(sop.StoreOptions{Name: "j1", SlotLength: 10})
			s2 := sop.NewStoreInfo(sop.StoreOptions{Name: "j2", SlotLength: 10})
			s1.Timestamp, s2.Timestamp = 100, 200
			sr.Add(ctx, *s1, *s2)
			infoFile2 := filepath.Join(base, s2.Name, storeInfoFilename)
			os.Remove(infoFile2)
			os.Mkdir(infoFile2, 0o755)
			upd1 := *s1
			upd1.CountDelta, upd1.Timestamp = 2, 777
			upd1.CacheConfig.StoreInfoCacheDuration = time.Minute
			upd2 := *s2
			upd2.CountDelta, upd2.Timestamp = 3, 888
			upd2.CacheConfig.StoreInfoCacheDuration = time.Minute
			if _, err := sr.Update(ctx, []sop.StoreInfo{upd1, upd2}); err == nil {
				t.Fatalf("expected update error")
			}
			ba, _ := os.ReadFile(filepath.Join(base, s1.Name, storeInfoFilename))
			var got sop.StoreInfo
			json.Unmarshal(ba, &got)
			if got.Timestamp != 100 || got.Count != 0 {
				t.Fatalf("rollback integrity mismatch %+v", got)
			}
		}},
		{"GetStoresBaseFolder & getFromCache mixed hit", func(t *testing.T) {
			active := filepath.Join(t.TempDir(), "a")
			passive := filepath.Join(t.TempDir(), "p")
			rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
			rt.ActiveFolderToggler = true
			cache := mocks.NewMockClient()
			sr, _ := NewStoreRepository(ctx, rt, nil, cache, MinimumModValue)
			if sr.GetStoresBaseFolder() != active {
				t.Fatalf("GetStoresBaseFolder mismatch")
			}
			s1 := sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 10})
			cache.SetStruct(ctx, s1.Name, s1, 0)
			res, err := sr.getFromCache(ctx, s1.Name, "missing")
			if err != nil || len(res) != 1 || res[0].Name != s1.Name {
				t.Fatalf("getFromCache mismatch %v %v", res, err)
			}
		}},
		{"GetAll nil -> Add -> Remove -> Remove no-op flow", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			if sl, err := sr.GetAll(ctx); err != nil || sl != nil {
				t.Fatalf("expected nil list: %v %v", sl, err)
			}
			si := *sop.NewStoreInfo(sop.StoreOptions{Name: "sA", SlotLength: 10})
			sr.Add(ctx, si)
			if names, _ := sr.GetAll(ctx); len(names) != 1 {
				t.Fatalf("expected 1 name got %v", names)
			}
			sr.Remove(ctx, "sA")
			sr.Remove(ctx, "sA")
		}},
		{"Update ghost store returns nil slice nil error", func(t *testing.T) {
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
			sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)
			pay := []sop.StoreInfo{{Name: "ghost", CountDelta: 5, CacheConfig: sop.StoreCacheConfig{StoreInfoCacheDuration: time.Minute}}}
			if got, err := sr.Update(ctx, pay); err != nil || got != nil {
				t.Fatalf("expected nil result nil err got %v %v", got, err)
			}
		}},
		{"fileIO replicate removeStore passive failure (action type 3)", func(t *testing.T) {
			active, passive := t.TempDir(), t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
			orig := FileIOSim
			FileIOSim = failingRemoveAll{FileIO: NewFileIO(), passiveRoot: passive}
			defer func() { FileIOSim = orig }()
			fio := newFileIOWithReplication(rt, NewManageStoreFolder(NewFileIO()), true)
			if err := fio.createStore(ctx, "x1"); err != nil {
				t.Fatalf("createStore: %v", err)
			}
			if err := fio.removeStore(ctx, "x1"); err != nil {
				t.Fatalf("removeStore active: %v", err)
			}
			if err := fio.replicate(ctx); err == nil {
				t.Fatalf("expected replicate remove store error")
			}
		}},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, sc.run)
	}
}
