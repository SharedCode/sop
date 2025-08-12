package fs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// mockCacheLockFail injects a lock acquisition failure to exercise Update early-return branch.
type mockCacheLockFail struct{ sop.Cache }

func (m mockCacheLockFail) Lock(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, sop.UUID, error) {
	return false, sop.UUID{}, nil
}
func (m mockCacheLockFail) Unlock(ctx context.Context, lk []*sop.LockKey) error { return nil }

// mockRegistryMapWriteFail wraps registryMap to fail writes for Update then succeed for later ops.
type mockRegistryMapWriteFail struct{ registryMap }

func (m *mockRegistryMapWriteFail) set(ctx context.Context, stores []sop.RegistryPayload[sop.Handle]) error {
	return errors.New("induced set error")
}

// Test Update lock failure & Replicate early exit when replication disabled or fails.
func TestRegistry_UpdateLockFailure(t *testing.T) {
	ctx := context.Background()
	// Use a normal cache for seeding so file preallocation lock can succeed once.
	seedCache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, seedCache)
	r := NewRegistry(true, MinimumModValue, rt, seedCache)
	defer r.Close()

	h := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rg", IDs: []sop.Handle{h}}}); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Swap in lock-failing cache to trigger early return branch.
	r.l2Cache = mockCacheLockFail{Cache: seedCache}
	if err := r.Update(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rg", IDs: []sop.Handle{h}}}); err == nil {
		t.Fatalf("expected lock failure")
	}
}

// Test Replicate returning early when replicate flag false and then executing when true.
func TestRegistry_ReplicateEarlyReturn(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base, base + "2"}, true, l2)
	rt.replicate = false // force early return path
	r := NewRegistry(true, MinimumModValue, rt, l2)
	defer r.Close()

	h := sop.NewHandle(sop.NewUUID())
	// Should no-op and return nil
	if err := r.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rg", IDs: []sop.Handle{h}}}, nil, nil, nil); err != nil {
		t.Fatalf("early replicate: %v", err)
	}

	// Enable and run replicate (should succeed best-effort with nil lists)
	rt.replicate = true
	if err := r.Replicate(ctx, nil, nil, nil, nil); err != nil {
		t.Fatalf("replicate enabled: %v", err)
	}
}

// Test Update write failure eviction path by inducing set error after lock success; we use a custom cache that always locks.
type mockCacheAllLock struct{ sop.Cache }

func (m mockCacheAllLock) Lock(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, sop.UUID, error) {
	return true, sop.UUID{}, nil
}
func (m mockCacheAllLock) Unlock(ctx context.Context, lk []*sop.LockKey) error { return nil }

func TestRegistry_UpdateWriteFailureEvicts(t *testing.T) {
	ctx := context.Background()
	l2 := mockCacheAllLock{Cache: mocks.NewMockClient()}
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
	r := NewRegistry(true, MinimumModValue, rt, l2)
	defer r.Close()

	// Swap hashmap with failing set implementation.
	failing := &mockRegistryMapWriteFail{registryMap: *r.hashmap}
	r.hashmap = &failing.registryMap

	h := sop.NewHandle(sop.NewUUID())
	// Directly call Update expecting set error (hashmap.set failure) without prior Add (map will attempt write and fail).
	if err := r.Update(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rg", IDs: []sop.Handle{h}}}); err == nil {
		t.Fatalf("expected induced set error")
	}
}
