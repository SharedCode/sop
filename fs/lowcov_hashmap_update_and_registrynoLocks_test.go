package fs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// mockCacheLockTimeout simulates IsLocked always false so updateFileBlockRegion spins until timeout.
type mockCacheLockTimeout struct{ sop.Cache }

func (m *mockCacheLockTimeout) IsLocked(ctx context.Context, lk []*sop.LockKey) (bool, error) {
	return false, nil
}

// mockCacheImmediateLockFail forces lock acquisition failure to hit early error in UpdateNoLocks when hashmap.set invoked.
type mockCacheImmediateLockFail struct{ sop.Cache }

func (m *mockCacheImmediateLockFail) Lock(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, sop.UUID, error) {
	return false, sop.NilUUID, errors.New("induced lock fail")
}

// (Removed lock-timeout exhaustive test: too slow / risk of suite delay.)

// Test covers UpdateNoLocks returning early error from underlying hashmap.set via induced lock failure.
func TestRegistry_UpdateNoLocks_SetError(t *testing.T) {
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
}
