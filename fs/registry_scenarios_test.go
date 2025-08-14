package fs

import (
	"context"
	"fmt"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestRegistry_AllScenarios consolidates registry & registryMap behaviors into one table-driven suite.
// It merges positive flows, error branches, replication edge cases, cache TTL paths, and helper coverage.
// Local helper types for lock behavior.
type testLockFail struct{ sop.Cache }

func (lf testLockFail) Lock(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, sop.UUID, error) {
	return false, sop.UUID{}, nil
}
func (lf testLockFail) Unlock(ctx context.Context, lk []*sop.LockKey) error { return nil }

type testAllLock struct{ sop.Cache }

func (al testAllLock) Lock(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, sop.UUID, error) {
	return true, sop.UUID{}, nil
}
func (al testAllLock) Unlock(ctx context.Context, lk []*sop.LockKey) error { return nil }

// Shared test fixtures (were previously in registry_test.go) still needed by other *_test files.
var uuid, _ = sop.ParseUUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
var hashMod = MinimumModValue

type setFail struct{ registryMap }

func (sf *setFail) set(ctx context.Context, p []sop.RegistryPayload[sop.Handle]) error {
	return errors.New("induced set error")
}

// cacheGetError induces an error on first GetStruct/GetStructEx to exercise registry.Get cache error path.
type cacheGetError struct {
	base    sop.Cache
	tripped bool
}

func newCacheGetError() *cacheGetError { return &cacheGetError{base: mocks.NewMockClient()} }
func (c *cacheGetError) Set(ctx context.Context, k, v string, d time.Duration) error { return c.base.Set(ctx, k, v, d) }
func (c *cacheGetError) Get(ctx context.Context, k string) (bool, string, error)     { return c.base.Get(ctx, k) }
func (c *cacheGetError) GetEx(ctx context.Context, k string, d time.Duration) (bool, string, error) {
	return c.base.GetEx(ctx, k, d)
}
func (c *cacheGetError) Ping(ctx context.Context) error { return nil }
func (c *cacheGetError) SetStruct(ctx context.Context, k string, v interface{}, d time.Duration) error {
	return c.base.SetStruct(ctx, k, v, d)
}
func (c *cacheGetError) GetStruct(ctx context.Context, k string, v interface{}) (bool, error) {
	if !c.tripped {
		c.tripped = true
		return false, fmt.Errorf("induced getstruct error")
	}
	return c.base.GetStruct(ctx, k, v)
}
func (c *cacheGetError) GetStructEx(ctx context.Context, k string, v interface{}, d time.Duration) (bool, error) {
	if !c.tripped {
		c.tripped = true
		return false, fmt.Errorf("induced getstructex error")
	}
	return c.base.GetStructEx(ctx, k, v, d)
}
func (c *cacheGetError) Delete(ctx context.Context, ks []string) (bool, error) { return c.base.Delete(ctx, ks) }
func (c *cacheGetError) FormatLockKey(k string) string                         { return c.base.FormatLockKey(k) }
func (c *cacheGetError) CreateLockKeys(keys []string) []*sop.LockKey           { return c.base.CreateLockKeys(keys) }
func (c *cacheGetError) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	return c.base.CreateLockKeysForIDs(keys)
}
func (c *cacheGetError) IsLockedTTL(ctx context.Context, d time.Duration, lks []*sop.LockKey) (bool, error) {
	return c.base.IsLockedTTL(ctx, d, lks)
}
func (c *cacheGetError) Lock(ctx context.Context, d time.Duration, lks []*sop.LockKey) (bool, sop.UUID, error) {
	return c.base.Lock(ctx, d, lks)
}
func (c *cacheGetError) IsLocked(ctx context.Context, lks []*sop.LockKey) (bool, error) { return c.base.IsLocked(ctx, lks) }
func (c *cacheGetError) IsLockedByOthers(ctx context.Context, ks []string) (bool, error) {
	return c.base.IsLockedByOthers(ctx, ks)
}
func (c *cacheGetError) Unlock(ctx context.Context, lks []*sop.LockKey) error { return c.base.Unlock(ctx, lks) }
func (c *cacheGetError) Clear(ctx context.Context) error                      { return c.base.Clear(ctx) }

// mockCacheImmediateLockFail forces Lock to fail to trigger UpdateNoLocks set error path.
type mockCacheImmediateLockFail struct{ sop.Cache }

func (m *mockCacheImmediateLockFail) Lock(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, sop.UUID, error) {
	return false, sop.NilUUID, errors.New("induced lock fail")
}

func TestRegistry_AllScenarios(t *testing.T) {
	type scenario struct {
		name string
		run  func(t *testing.T)
	}
	ctx := context.Background()
	scenarios := []scenario{
		{name: "AddGetBasic", run: func(t *testing.T) {
			l2 := mocks.NewMockClient()
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
			r := NewRegistry(true, MinimumModValue, rt, l2)
			defer r.Close()
			h := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rg", IDs: []sop.Handle{h}}}); err != nil {
				t.Fatalf("add: %v", err)
			}
			res, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rg", IDs: []sop.UUID{h.LogicalID}}})
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if len(res) == 0 || len(res[0].IDs) == 0 || res[0].IDs[0].LogicalID != h.LogicalID {
				t.Fatalf("unexpected get result: %+v", res)
			}
		}},
		{name: "UpdateVariantsRemove", run: func(t *testing.T) {
			l2 := mocks.NewMockClient()
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
			r := NewRegistry(true, MinimumModValue, rt, l2)
			defer r.Close()
			h1 := sop.NewHandle(sop.NewUUID())
			h2 := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgx", IDs: []sop.Handle{h1, h2}}}); err != nil {
				t.Fatalf("seed add: %v", err)
			}
			h1.Version = 1
			if err := r.Update(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgx", IDs: []sop.Handle{h1}}}); err != nil {
				t.Fatalf("update: %v", err)
			}
			h2.Version = 2
			if err := r.UpdateNoLocks(ctx, true, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgx", IDs: []sop.Handle{h2}}}); err != nil {
				t.Fatalf("update nolocks: %v", err)
			}
			if err := r.Remove(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rgx", IDs: []sop.UUID{h1.LogicalID}}}); err != nil {
				t.Fatalf("remove: %v", err)
			}
		}},
		{name: "ReplicationSuccessPath", run: func(t *testing.T) {
			l2 := mocks.NewMockClient()
			a := t.TempDir()
			b := t.TempDir()
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
			rt, _ := NewReplicationTracker(ctx, []string{a, b}, true, l2)
			r := NewRegistry(true, MinimumModValue, rt, l2)
			defer r.Close()
			hRoot := sop.NewHandle(sop.NewUUID())
			hAdd := sop.NewHandle(sop.NewUUID())
			hUpd := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgrepl", IDs: []sop.Handle{hRoot, hAdd, hUpd}}}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if err := r.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgrepl", IDs: []sop.Handle{hRoot}}}, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgrepl", IDs: []sop.Handle{hAdd}}}, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgrepl", IDs: []sop.Handle{hUpd}}}, nil); err != nil {
				t.Fatalf("replicate: %v", err)
			}
		}},
		{name: "ReplicationEarlyReturnDisabledOrFailed", run: func(t *testing.T) {
			l2 := mocks.NewMockClient()
			a := t.TempDir()
			b := t.TempDir()
			rtNo, _ := NewReplicationTracker(ctx, []string{a, b}, false, l2)
			r1 := NewRegistry(true, MinimumModValue, rtNo, l2)
			defer r1.Close()
			h := sop.NewHandle(sop.NewUUID())
			if err := r1.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "t1", IDs: []sop.Handle{h}}}, nil, nil, nil); err != nil {
				t.Fatalf("disabled replicate: %v", err)
			}
			a2 := t.TempDir()
			b2 := t.TempDir()
			GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
			rtFail, _ := NewReplicationTracker(ctx, []string{a2, b2}, true, l2)
			rtFail.FailedToReplicate = true
			r2 := NewRegistry(true, MinimumModValue, rtFail, l2)
			defer r2.Close()
			if err := r2.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "t2", IDs: []sop.Handle{h}}}, nil, nil, nil); err != nil {
				t.Fatalf("failed replicate early: %v", err)
			}
		}},
		{name: "ReplicationErrorInvalidPassiveRoot", run: func(t *testing.T) {
			l2 := mocks.NewMockClient()
			active := t.TempDir()
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
			passiveFile := filepath.Join(active, "passive-file")
			os.WriteFile(passiveFile, []byte("x"), 0o600)
			rt, _ := NewReplicationTracker(ctx, []string{active, passiveFile}, true, l2)
			r := NewRegistry(true, MinimumModValue, rt, l2)
			defer r.Close()
			h := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgep", IDs: []sop.Handle{h}}}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if err := r.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgep", IDs: []sop.Handle{h}}}, nil, nil, nil); err == nil {
				t.Fatalf("expected error with file passive root")
			}
		}},
		{name: "ReplicationPartialErrorsSetAddRemove", run: func(t *testing.T) {
			if runtime.GOOS == "windows" {
				t.Skip("chmod semantics differ")
			}
			l2 := mocks.NewMockClient()
			a := t.TempDir()
			b := t.TempDir()
			GlobalReplicationDetails = nil
			rt, _ := NewReplicationTracker(ctx, []string{a, b}, true, l2)
			r := NewRegistry(true, MinimumModValue, rt, l2)
			defer r.Close()
			h1 := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "pr", IDs: []sop.Handle{h1}}}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			os.Chmod(b, 0o500)
			h2 := sop.NewHandle(sop.NewUUID())
			err := r.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "pr", IDs: []sop.Handle{h2}}}, nil, nil, nil)
			if err == nil {
				t.Fatalf("expected replication failure")
			}
			if !rt.FailedToReplicate {
				t.Fatalf("expected FailedToReplicate")
			}
			if err := r.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "pr", IDs: []sop.Handle{h2}}}, nil, nil, nil); err != nil {
				t.Fatalf("early return replicate: %v", err)
			}
		}},
		{name: "Get_AllFound_NoFetch", run: func(t *testing.T) {
			ctx := context.Background()
			l2 := mocks.NewMockClient()
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
			r := NewRegistry(true, MinimumModValue, rt, l2)
			defer r.Close()
			h1 := sop.NewHandle(sop.NewUUID())
			h2 := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "geta", IDs: []sop.Handle{h1, h2}}}); err != nil {
				t.Fatalf("add: %v", err)
			}
			res, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "geta", IDs: []sop.UUID{h1.LogicalID, h2.LogicalID}}})
			if err != nil || len(res) != 1 || len(res[0].IDs) != 2 {
				t.Fatalf("unexpected get result: %v %+v", err, res)
			}
		}},
		{name: "Get_ErrorOnCacheGet", run: func(t *testing.T) {
			ctx := context.Background()
			cg := newCacheGetError()
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, cg)
			r := NewRegistry(true, MinimumModValue, rt, cg)
			defer r.Close()
			h := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "geterr", IDs: []sop.Handle{h}}}); err != nil {
				t.Fatalf("add: %v", err)
			}
			res, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "geterr", IDs: []sop.UUID{h.LogicalID}}})
			if err != nil || len(res) != 1 || len(res[0].IDs) != 1 {
				t.Fatalf("unexpected get result after induced cache error: %v %+v", err, res)
			}
		}},
		{name: "Replicate_CloseOverrideErrors", run: func(t *testing.T) {
			ctx := context.Background()
			l2 := mocks.NewMockClient()
			a := t.TempDir()
			b := t.TempDir()
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
			rt, _ := NewReplicationTracker(ctx, []string{a, b}, true, l2)
			r := NewRegistry(true, MinimumModValue, rt, l2)
			defer r.Close()
			h1 := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "replc", IDs: []sop.Handle{h1}}}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			r.rmCloseOverride = func() error { return fmt.Errorf("close override error") }
			if err := r.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "replc", IDs: []sop.Handle{h1}}}, nil, nil, nil); err == nil || err.Error() != "close override error" {
				t.Fatalf("expected close override error, got %v", err)
			}
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
			a2 := t.TempDir()
			passiveFile := filepath.Join(t.TempDir(), "pas-file")
			if err := os.WriteFile(passiveFile, []byte("x"), 0o600); err != nil { t.Fatal(err) }
			rt2, _ := NewReplicationTracker(ctx, []string{a2, passiveFile}, true, l2)
			r2 := NewRegistry(true, MinimumModValue, rt2, l2)
			defer r2.Close()
			h2 := sop.NewHandle(sop.NewUUID())
			if err := r2.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "replcerr", IDs: []sop.Handle{h2}}}); err != nil {
				t.Fatalf("seed2: %v", err)
			}
			r2.rmCloseOverride = func() error { return fmt.Errorf("ignored close error") }
			if err := r2.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "replcerr", IDs: []sop.Handle{h2}}}, nil, nil, nil); err == nil || err.Error() == "ignored close error" {
				t.Fatalf("expected earlier replication add error, got %v", err)
			}
		}},
		{name: "Replicate_LayeredErrors_FirstErrorWins", run: func(t *testing.T) {
			ctx := context.Background()
			cache := mocks.NewMockClient()
			active := t.TempDir()
			passiveDir := t.TempDir()
			passiveFile := filepath.Join(passiveDir, "pasfile")
			if err := os.WriteFile(passiveFile, []byte("x"), 0o600); err != nil { t.Fatalf("seed passive file: %v", err) }
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
			rt, _ := NewReplicationTracker(ctx, []string{active, passiveFile}, true, cache)
			r := NewRegistry(true, MinimumModValue, rt, cache)
			defer r.Close()
			hSeed := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rlay", IDs: []sop.Handle{hSeed}}}); err != nil { t.Fatalf("seed add: %v", err) }
			newRoot := sop.NewHandle(sop.NewUUID())
			updated := hSeed; updated.Version = 2
			remove := sop.NewHandle(sop.NewUUID())
			r.rmCloseOverride = func() error { return errors.New("close override error") }
			if err := r.Replicate(ctx,
				[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rlay", IDs: []sop.Handle{newRoot}}},
				[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rlay", IDs: []sop.Handle{hSeed}}},
				[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rlay", IDs: []sop.Handle{updated}}},
				[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rlay", IDs: []sop.Handle{remove}}},
			); err == nil {
				t.Fatalf("expected primary replication error")
			}
			if !rt.FailedToReplicate { t.Fatalf("expected FailedToReplicate set") }
		}},
		{name: "UpdateNoLocks_SetError", run: func(t *testing.T) {
			ctx := context.Background()
			base := t.TempDir()
			lockFailCache := &mockCacheImmediateLockFail{Cache: mocks.NewMockClient()}
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, lockFailCache)
			r := NewRegistry(true, MinimumModValue, rt, lockFailCache)
			defer r.Close()
			h := sop.NewHandle(sop.NewUUID())
			if err := r.UpdateNoLocks(ctx, false, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "tbl", IDs: []sop.Handle{h}}}); err == nil {
				t.Fatalf("expected UpdateNoLocks error due to lock failure")
			}
		}},
		{name: "UpdateLockFailureEvict", run: func(t *testing.T) {
			l2seed := mocks.NewMockClient()
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2seed)
			r := NewRegistry(true, MinimumModValue, rt, l2seed)
			defer r.Close()
			h := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rg", IDs: []sop.Handle{h}}}); err != nil {
				t.Fatalf("add: %v", err)
			}
			// Force lock failure path (returns ok=false) and ensure Update returns error.
			r.l2Cache = testLockFail{Cache: l2seed}
			if err := r.Update(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rg", IDs: []sop.Handle{h}}}); err == nil {
				t.Fatalf("expected lock fail")
			}
		}},
		{name: "ErrorPathsDuplicateAddRemoveMissingLockFail", run: func(t *testing.T) {
			l2 := mocks.NewMockClient()
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
			r := NewRegistry(true, MinimumModValue, rt, l2)
			defer r.Close()
			h := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regerr", IDs: []sop.Handle{h}}}); err != nil {
				t.Fatalf("seed add: %v", err)
			}
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regerr", IDs: []sop.Handle{h}}}); err == nil || !strings.Contains(err.Error(), "registryMap.add failed") {
				t.Fatalf("expected duplicate add error: %v", err)
			}
			missing := sop.NewUUID()
			if err := r.Remove(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "regerr", IDs: []sop.UUID{missing}}}); err == nil || !strings.Contains(err.Error(), "registryMap.remove failed") {
				t.Fatalf("expected remove missing error: %v", err)
			}
			h2 := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "reglock", IDs: []sop.Handle{h2}}}); err != nil {
				t.Fatalf("lock seed: %v", err)
			}
			lk := r.l2Cache.CreateLockKeys([]string{h2.LogicalID.String()})
			if ok, _, e := r.l2Cache.Lock(ctx, time.Minute, lk); !ok || e != nil {
				t.Fatalf("pre-lock: %v", e)
			}
			h2.Version = 1
			if err := r.Update(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "reglock", IDs: []sop.Handle{h2}}}); err == nil || !strings.Contains(err.Error(), "lock failed") {
				t.Fatalf("expected lock failed error: %v", err)
			}
			_ = r.l2Cache.Unlock(ctx, lk)
		}},
		{name: "CacheTTLFetch", run: func(t *testing.T) {
			l2 := mocks.NewMockClient()
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
			r := NewRegistry(true, MinimumModValue, rt, l2)
			defer r.Close()
			h := sop.NewHandle(sop.NewUUID())
			if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regttl", CacheDuration: time.Minute, IDs: []sop.Handle{h}}}); err != nil {
				t.Fatalf("seed add: %v", err)
			}
			res, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "regttl", CacheDuration: time.Minute, IsCacheTTL: true, IDs: []sop.UUID{h.LogicalID}}})
			if err != nil || len(res) == 0 || len(res[0].IDs) == 0 {
				t.Fatalf("ttl get unexpected: %v %+v", err, res)
			}
		}},
		{name: "RegistryMapBasicOps", run: func(t *testing.T) {
			l2 := mocks.NewMockClient()
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
			rm := newRegistryMap(true, MinimumModValue, rt, l2)
			defer rm.close()
			h := sop.NewHandle(sop.NewUUID())
			if err := rm.add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regmap", IDs: []sop.Handle{h}}}); err != nil {
				t.Fatalf("add: %v", err)
			}
			if err := rm.set(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regmap", IDs: []sop.Handle{h}}}); err != nil {
				t.Fatalf("set: %v", err)
			}
			if _, err := rm.fetch(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "regmap", IDs: []sop.UUID{h.LogicalID}}}); err != nil {
				t.Fatalf("fetch: %v", err)
			}
			if err := rm.remove(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "regmap", IDs: []sop.UUID{h.LogicalID}}}); err != nil {
				t.Fatalf("remove: %v", err)
			}
		}},
		{name: "HelperGetIDs", run: func(t *testing.T) {
			hs := []sop.Handle{{LogicalID: sop.NewUUID()}, {LogicalID: sop.NewUUID()}}
			ids := getIDs(hs)
			if len(ids) != len(hs) {
				t.Fatalf("len mismatch")
			}
		}},
		{name: "ConvertToKvp", run: func(t *testing.T) {
			h1 := sop.NewHandle(sop.NewUUID())
			h2 := sop.NewHandle(sop.NewUUID())
			kv := convertToKvp([]sop.Handle{h1, h2})
			if len(kv) != 2 || kv[0].Key != h1.LogicalID || kv[1].Key != h2.LogicalID {
				t.Fatalf("kv mismatch: %+v", kv)
			}
		}},
	}
	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			prev := GlobalReplicationDetails
			defer func() { GlobalReplicationDetails = prev }()
			sc.run(t)
		})
	}
}

// Consolidated scenario restoring coverage for registry Replicate failure + short-circuit behavior
// previously in replicate_error_branches_cases_test.go.
func TestRegistry_ReplicateErrorBranches_Scenario(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := t.TempDir()
	passiveDir := t.TempDir()
	// Create a file inside passiveDir and then use that file path as passive root to induce errors.
	passiveFile := passiveDir + string(os.PathSeparator) + "passive_as_file"
	if err := os.WriteFile(passiveFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup file: %v", err)
	}

	rt, _ := NewReplicationTracker(ctx, []string{active, passiveFile}, true, cache)
	r := NewRegistry(true, MinimumModValue, rt, cache)
	defer r.Close()

	// Seed active with a handle used in added/updated/removed slices.
	hAdd := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{hAdd}}}); err != nil {
		t.Fatalf("seed add: %v", err)
	}
	del := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{del}}}); err != nil {
		t.Fatalf("seed del add: %v", err)
	}
	upd := hAdd
	upd.Version = 2
	newRoot := sop.NewHandle(sop.NewUUID())

	firstErr := r.Replicate(ctx,
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{newRoot}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{hAdd}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{upd}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{del}}},
	)
	if firstErr == nil && !rt.FailedToReplicate {
		t.Fatalf("expected replicate error or failure flag set on first call")
	}
	if !rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate flag set")
	}

	// Second replicate should short-circuit returning nil.
	if err := r.Replicate(ctx,
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{newRoot}}},
		nil, nil, nil,
	); err != nil {
		t.Fatalf("expected nil on second replicate, got %v", err)
	}
}
