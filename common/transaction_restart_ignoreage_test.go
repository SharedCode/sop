package common

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sharedcode/sop"
)

// plCapture captures whether GetBatch was invoked with the ignore-age context flag.
type plCapture struct {
	gotIgnoreAge  int32
	batchesServed int32
}

func (p *plCapture) IsEnabled() bool                                             { return true }
func (p *plCapture) Add(ctx context.Context, tid sop.UUID, payload []byte) error { return nil }
func (p *plCapture) Remove(ctx context.Context, tid sop.UUID) error              { return nil }
func (p *plCapture) Get(ctx context.Context, tid sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	return nil, nil
}
func (p *plCapture) LogCommitChanges(ctx context.Context, stores []sop.StoreInfo, a, b, c, d []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (p *plCapture) ClearRegistrySectorClaims(ctx context.Context) error { return nil }

// First call returns one fake entry, second call empty -> stops loop.
func (p *plCapture) GetBatch(ctx context.Context, batchSize int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	if v := ctx.Value(sop.ContextPriorityLogIgnoreAge); v != nil {
		if b, ok := v.(bool); ok && b {
			atomic.StoreInt32(&p.gotIgnoreAge, 1)
		}
	}
	atomic.AddInt32(&p.batchesServed, 1)
	return nil, nil
}

type txLogCapture struct{ pl *plCapture }

func (l *txLogCapture) PriorityLog() sop.TransactionPriorityLog { return l.pl }
func (l *txLogCapture) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	return nil
}
func (l *txLogCapture) Remove(ctx context.Context, tid sop.UUID) error { return nil }
func (l *txLogCapture) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, "", nil, nil
}
func (l *txLogCapture) GetOneOfHour(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, nil, nil
}
func (l *txLogCapture) NewUUID() sop.UUID { return sop.NewUUID() }

// TestRestartSweepSetsIgnoreAgeFlag validates that the restart-triggered sweep invokes GetBatch with ContextPriorityLogIgnoreAge=true.
func TestRestartSweepSetsIgnoreAgeFlag(t *testing.T) {
	ctx := context.Background()
	sop.SetRestartCheckInterval(10 * time.Millisecond)
	sop.SetRestartInfoEveryN(1)
	cache := newFakeRestartCache("r1")
	plc := &plCapture{}
	tl := newTransactionLogger(&txLogCapture{pl: plc}, false)
	tr := &Transaction{l2Cache: cache, logger: tl, cacheRestartHelper: sop.NewCacheRestartHelper(cache), maxTime: 2 * time.Second}
	tr.btreesBackend = []btreeBackend{{}}

	// Baseline onIdle (no restart yet) should NOT set ignore-age.
	tr.onIdle(ctx)
	if atomic.LoadInt32(&plc.gotIgnoreAge) != 0 {
		t.Fatalf("expected ignore-age flag not set before restart")
	}

	// Trigger restart.
	cache.setRunID("r2")
	time.Sleep(15 * time.Millisecond)
	tr.onIdle(ctx)

	if atomic.LoadInt32(&plc.gotIgnoreAge) != 1 {
		t.Fatalf("expected ignore-age flag set during restart sweep")
	}
}

// fakeRestartCache provides just enough of sop.Cache for the restart helper + locking calls.
type fakeRestartCache struct{ runID atomic.Value }

func newFakeRestartCache(initial string) *fakeRestartCache {
	f := &fakeRestartCache{}
	f.runID.Store(initial)
	return f
}
func (f *fakeRestartCache) setRunID(v string) { f.runID.Store(v) }

// Cache interface methods (minimal behaviors)
func (f *fakeRestartCache) Set(ctx context.Context, k, v string, d time.Duration) error { return nil }
func (f *fakeRestartCache) Get(ctx context.Context, k string) (bool, string, error) {
	return false, "", nil
}
func (f *fakeRestartCache) GetEx(ctx context.Context, k string, d time.Duration) (bool, string, error) {
	return false, "", nil
}
func (f *fakeRestartCache) SetStruct(ctx context.Context, k string, v interface{}, d time.Duration) error {
	return nil
}
func (f *fakeRestartCache) GetStruct(ctx context.Context, k string, target interface{}) (bool, error) {
	return false, nil
}
func (f *fakeRestartCache) GetStructEx(ctx context.Context, k string, target interface{}, d time.Duration) (bool, error) {
	return false, nil
}
func (f *fakeRestartCache) Delete(ctx context.Context, keys []string) (bool, error) {
	return false, nil
}
func (f *fakeRestartCache) Ping(ctx context.Context) error { return nil }
func (f *fakeRestartCache) FormatLockKey(k string) string  { return k }
func (f *fakeRestartCache) CreateLockKeys(keys []string) []*sop.LockKey {
	l := make([]*sop.LockKey, len(keys))
	for i, k := range keys {
		l[i] = &sop.LockKey{Key: k, LockID: sop.NewUUID(), IsLockOwner: true}
	}
	return l
}
func (f *fakeRestartCache) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	l := make([]*sop.LockKey, len(keys))
	for i, k := range keys {
		l[i] = &sop.LockKey{Key: k.First, LockID: k.Second, IsLockOwner: true}
	}
	return l
}
func (f *fakeRestartCache) IsLockedTTL(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, error) {
	return true, nil
}
func (f *fakeRestartCache) Lock(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, sop.UUID, error) {
	return true, sop.NilUUID, nil
}
func (f *fakeRestartCache) IsLocked(ctx context.Context, lk []*sop.LockKey) (bool, error) {
	return true, nil
}
func (f *fakeRestartCache) IsLockedByOthers(ctx context.Context, names []string) (bool, error) {
	return false, nil
}
func (f *fakeRestartCache) Unlock(ctx context.Context, lk []*sop.LockKey) error { return nil }
func (f *fakeRestartCache) Clear(ctx context.Context) error                     { return nil }
func (f *fakeRestartCache) Info(ctx context.Context, section string) (string, error) {
	return "# Server\nrun_id:" + f.runID.Load().(string) + "\n", nil
}
