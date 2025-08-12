package fs

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestRegistryMap_ErrorPaths exercises selected error branches in registryMap via the public Registry API.
// Keeps file small while targeting high-yield uncovered statements.
func TestRegistryMap_ErrorPaths(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
	r := NewRegistry(true, MinimumModValue, rt, l2)
	defer r.Close()

	t.Run("duplicate_add", func(t *testing.T) {
		h := sop.NewHandle(sop.NewUUID())
		if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regerr", IDs: []sop.Handle{h}}}); err != nil {
			t.Fatalf("initial add: %v", err)
		}
		if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regerr", IDs: []sop.Handle{h}}}); err == nil || !strings.Contains(err.Error(), "registryMap.add failed") {
			t.Fatalf("expected duplicate add error, got: %v", err)
		}
	})

	t.Run("remove_missing", func(t *testing.T) {
		missing := sop.NewUUID()
		if err := r.Remove(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "regerr", IDs: []sop.UUID{missing}}}); err == nil || !strings.Contains(err.Error(), "registryMap.remove failed") {
			t.Fatalf("expected remove missing error, got: %v", err)
		}
	})

	t.Run("update_lock_fail", func(t *testing.T) {
		h := sop.NewHandle(sop.NewUUID())
		if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "reglock", IDs: []sop.Handle{h}}}); err != nil {
			t.Fatalf("seed add: %v", err)
		}
		// Acquire the lock directly so Update sees it as already held.
		lk := r.l2Cache.CreateLockKeys([]string{h.LogicalID.String()})
		if ok, _, err := r.l2Cache.Lock(ctx, time.Minute, lk); !ok || err != nil {
			t.Fatalf("pre-lock failed: %v", err)
		}
		h.Version = 42
		if err := r.Update(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "reglock", IDs: []sop.Handle{h}}}); err == nil || !strings.Contains(err.Error(), "lock failed") {
			t.Fatalf("expected lock failed error, got: %v", err)
		}
		// Unlock for cleanliness (ignore error).
		_ = r.l2Cache.Unlock(ctx, lk)
	})

	t.Run("get_cache_ttl", func(t *testing.T) {
		h := sop.NewHandle(sop.NewUUID())
		if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regttl", CacheDuration: time.Minute, IDs: []sop.Handle{h}}}); err != nil {
			t.Fatalf("seed add: %v", err)
		}
		// Fetch with TTL flag set to exercise GetStructEx path.
		res, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "regttl", CacheDuration: time.Minute, IsCacheTTL: true, IDs: []sop.UUID{h.LogicalID}}})
		if err != nil || len(res) != 1 || len(res[0].IDs) != 1 {
			t.Fatalf("ttl get unexpected result: %v %+v", err, res)
		}
	})
}
