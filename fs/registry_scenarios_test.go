package fs

import (
	"context"
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
