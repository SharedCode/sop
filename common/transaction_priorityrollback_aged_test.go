package common

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/teststubs"
)

// priorityLogCounter2 extends earlier counter to expose count.
type priorityLogCounter2 struct{ clearCount int32 }

func (p *priorityLogCounter2) IsEnabled() bool { return true }
func (p *priorityLogCounter2) Add(ctx context.Context, tid sop.UUID, payload []byte) error {
	return nil
}
func (p *priorityLogCounter2) Remove(ctx context.Context, tid sop.UUID) error { return nil }
func (p *priorityLogCounter2) Get(ctx context.Context, tid sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	return nil, nil
}
func (p *priorityLogCounter2) GetBatch(ctx context.Context, batchSize int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	return nil, nil
}
func (p *priorityLogCounter2) LogCommitChanges(context.Context, []sop.StoreInfo, []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (p *priorityLogCounter2) ClearRegistrySectorClaims(ctx context.Context) error {
	atomic.AddInt32(&p.clearCount, 1)
	return nil
}

type txLogCounter2 struct{ pl *priorityLogCounter2 }

func (l *txLogCounter2) PriorityLog() sop.TransactionPriorityLog          { return l.pl }
func (l *txLogCounter2) Add(context.Context, sop.UUID, int, []byte) error { return nil }
func (l *txLogCounter2) Remove(context.Context, sop.UUID) error           { return nil }
func (l *txLogCounter2) GetOne(context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, "", nil, nil
}
func (l *txLogCounter2) GetOneOfHour(context.Context, string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, nil, nil
}
func (l *txLogCounter2) NewUUID() sop.UUID { return sop.NewUUID() }

// TestMultipleRestartsTriggerMultipleSweeps ensures each distinct run_id change triggers a new sweep.
func TestMultipleRestartsTriggerMultipleSweeps(t *testing.T) {
	ctx := context.Background()
	sop.SetRestartCheckInterval(10 * time.Millisecond)
		sop.SetRestartInfoEveryN(1)
	cache := newFakeRestartCache("r1")
	plc := &priorityLogCounter2{}
	tl := newTransactionLogger(&txLogCounter2{pl: plc}, false)
	tr := &Transaction{l2Cache: cache, logger: tl, cacheRestartHelper: sop.NewCacheRestartHelper(cache), maxTime: 2 * time.Second}
	tr.btreesBackend = []btreeBackend{{}}

	tr.onIdle(ctx) // baseline
	if c := atomic.LoadInt32(&plc.clearCount); c != 0 {
		t.Fatalf("expected 0 sweeps baseline, got %d", c)
	}
	cache.setRunID("r2")
	time.Sleep(15 * time.Millisecond)
	tr.onIdle(ctx)
	if c := atomic.LoadInt32(&plc.clearCount); c != 1 {
		t.Fatalf("expected 1 sweep after first restart, got %d", c)
	}
	tr.onIdle(ctx) // no change
	if c := atomic.LoadInt32(&plc.clearCount); c != 1 {
		t.Fatalf("expected still 1 sweep without run_id change, got %d", c)
	}
	cache.setRunID("r3")
	time.Sleep(15 * time.Millisecond)
	tr.onIdle(ctx)
	if c := atomic.LoadInt32(&plc.clearCount); c != 2 {
		t.Fatalf("expected 2 sweeps after second restart, got %d", c)
	}
}

// stubAgingPriorityLog simulates age-based availability: files younger than 'aged' duration are skipped.
type stubAgingPriorityLog struct {
	baseDir      string
	tids         []sop.UUID
	removed      map[sop.UUID]bool
	ageThreshold time.Duration
}

func newStubAgingPriorityLog(dir string, count int, ageThreshold time.Duration) *stubAgingPriorityLog {
	ap := &stubAgingPriorityLog{baseDir: dir, removed: make(map[sop.UUID]bool), ageThreshold: ageThreshold}
	os.MkdirAll(dir, 0o755)
	for i := 0; i < count; i++ {
		tid := sop.NewUUID()
		ap.tids = append(ap.tids, tid)
		f := filepath.Join(dir, tid.String()+".plg")
		os.WriteFile(f, []byte("x"), 0o644)
	}
	return ap
}
func (p *stubAgingPriorityLog) IsEnabled() bool                             { return true }
func (p *stubAgingPriorityLog) Add(context.Context, sop.UUID, []byte) error { return nil }
func (p *stubAgingPriorityLog) Remove(ctx context.Context, tid sop.UUID) error {
	os.Remove(filepath.Join(p.baseDir, tid.String()+".plg"))
	p.removed[tid] = true
	return nil
}
func (p *stubAgingPriorityLog) Get(ctx context.Context, tid sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	return nil, nil
}
func (p *stubAgingPriorityLog) LogCommitChanges(context.Context, []sop.StoreInfo, []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (p *stubAgingPriorityLog) ClearRegistrySectorClaims(context.Context) error { return nil }
func (p *stubAgingPriorityLog) GetBatch(ctx context.Context, batchSize int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	now := time.Now()
	res := []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{}
	for _, tid := range p.tids {
		if p.removed[tid] {
			continue
		}
		fi, err := os.Stat(filepath.Join(p.baseDir, tid.String()+".plg"))
		if err != nil {
			continue
		}
		if now.Sub(fi.ModTime()) < p.ageThreshold {
			continue
		}
		// Provide a dummy handle payload so rollback path can acquire locks without panic.
		res = append(res, sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{Key: tid, Value: []sop.RegistryPayload[sop.Handle]{
			{IDs: []sop.Handle{{LogicalID: tid, Version: 1}}},
		}})
		if len(res) >= batchSize {
			break
		}
	}
	if len(res) == 0 {
		return nil, nil
	}
	return res, nil
}

type txLogAging struct{ pl *stubAgingPriorityLog }

func (l *txLogAging) PriorityLog() sop.TransactionPriorityLog          { return l.pl }
func (l *txLogAging) Add(context.Context, sop.UUID, int, []byte) error { return nil }
func (l *txLogAging) Remove(context.Context, sop.UUID) error           { return nil }
func (l *txLogAging) GetOne(context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, "", nil, nil
}
func (l *txLogAging) GetOneOfHour(context.Context, string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, nil, nil
}
func (l *txLogAging) NewUUID() sop.UUID { return sop.NewUUID() }

// TestPeriodicPriorityRollbackProcessesAgedLog verifies that a fresh priority log is not processed until aged, then processed without a restart (incremental path).
func TestPeriodicPriorityRollbackProcessesAgedLog(t *testing.T) {
	ctx := context.Background()
	// Make restart detection slow so we test periodic path; large interval prevents restart path (no run_id change anyway)
	sop.SetRestartCheckInterval(time.Second)
	cache := newFakeRestartCache("r1")

	tmpDir := t.TempDir()
	// Lower age threshold to speed up test and reduce flakiness.
	pl := newStubAgingPriorityLog(tmpDir, 1, 50*time.Millisecond)
	tl := newTransactionLogger(&txLogAging{pl: pl}, false)
	tr := &Transaction{l2Cache: cache, logger: tl, cacheRestartHelper: sop.NewCacheRestartHelper(cache), maxTime: time.Second}
	// Provide registry stub so rollback path does not panic.
	tr.registry = &teststubs.RegistryStub{}
	tr.btreesBackend = []btreeBackend{{}}

	// Initial onIdle: file too new, should remain.
	tr.onIdle(ctx)
	for tid := range pl.tids {
		_ = tid
	}
	if len(pl.removed) != 0 {
		t.Fatalf("expected 0 removed initially, got %d", len(pl.removed))
	}

	// Age the file by retroactively adjusting mtime.
	for _, tid := range pl.tids {
		fp := filepath.Join(tmpDir, tid.String()+".plg")
		oldTime := time.Now().Add(-time.Second)
		os.Chtimes(fp, oldTime, oldTime)
	}
	// Force the periodic sweep interval to elapse by rewinding lastPriorityOnIdleTime.
	lastPriorityOnIdleTime = 0
	// Wait beyond threshold and run onIdle again.
	time.Sleep(60 * time.Millisecond)
	tr.onIdle(ctx)
	if len(pl.removed) != 1 {
		t.Fatalf("expected 1 removed after aging, got %d", len(pl.removed))
	}
}
