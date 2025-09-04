package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ncw/directio"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// blockingIO simulates slow I/O that either blocks until ctx.Done or delays reads.
type blockingIO struct {
	blockOnRead  bool
	blockOnWrite bool
	readDelay    time.Duration
}

func (b blockingIO) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) {
	if dir := filepath.Dir(filename); dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	// Ensure create flag so tests can seed files easily.
	return os.OpenFile(filename, flag|os.O_CREATE, perm)
}
func (b blockingIO) WriteAt(ctx context.Context, f *os.File, block []byte, off int64) (int, error) {
	if b.blockOnWrite {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return f.WriteAt(block, off)
}
func (b blockingIO) ReadAt(ctx context.Context, f *os.File, block []byte, off int64) (int, error) {
	if b.readDelay > 0 {
		time.Sleep(b.readDelay)
	}
	if b.blockOnRead {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return f.ReadAt(block, off)
}
func (b blockingIO) Close(f *os.File) error { return f.Close() }

func Test_DirectIO_Watchdog_CanceledContext(t *testing.T) {
	t.Parallel()
	d := directIO{}
	dir := t.TempDir()
	fn := filepath.Join(dir, "watchdog-directio.dat")
	f, err := d.Open(context.Background(), fn, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close(f) })

	blk := directio.AlignedBlock(blockSize)
	cctx, cancel := context.WithCancel(context.Background())
	cancel()

	if n, err := d.WriteAt(cctx, f, blk, 0); !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context error, got n=%d err=%v", n, err)
	}

	rb := directio.AlignedBlock(blockSize)
	if n, err := d.ReadAt(cctx, f, rb, 0); !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context error (read), got n=%d err=%v", n, err)
	}
}

func Test_updateFileBlockRegion_AlignedDeadline_ReadTimeout(t *testing.T) {
	// Do not parallelize; adjusts global timing knobs.
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	// Tighten TTL and increase slack to force a small op deadline.
	origTTL, origSlack := LockFileRegionDuration, LockDeadlineSlackPercent
	SetLockFileRegionDuration(200 * time.Millisecond)
	SetLockDeadlineSlackPercent(0.60)
	t.Cleanup(func() { SetLockFileRegionDuration(origTTL); SetLockDeadlineSlackPercent(origSlack) })

	// Hashmap with cache that always reports locked.
	hm := newHashmap(true, 8, rt, &mockCacheHashmap{base: mocks.NewMockClient(), isLockedAlways: true})

	// Seed a segment file with one block so reads would succeed if not blocked.
	seg := filepath.Join(base, "wdread", "wdread-1"+registryFileExtension)
	if err := os.MkdirAll(filepath.Dir(seg), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(seg, make([]byte, blockSize), 0o644); err != nil {
		t.Fatalf("seed seg: %v", err)
	}

	dio := newFileDirectIOInjected(blockingIO{blockOnRead: true})
	if err := dio.open(ctx, seg, os.O_RDWR, permission); err != nil {
		t.Fatalf("open dio: %v", err)
	}
	dio.filename = "wdread-1" + registryFileExtension

	start := time.Now()
	err = hm.updateFileBlockRegion(ctx, dio, 0, 0, make([]byte, sop.HandleSizeInBytes))
	took := time.Since(start)
	if err == nil || (!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled)) {
		t.Fatalf("expected timeout/cancel from read watchdog, got %v", err)
	}
	if took > 2*time.Second {
		t.Fatalf("deadline not enforced; operation took too long: %v", took)
	}
}

func Test_updateFileBlockRegion_AlignedDeadline_WriteTimeout(t *testing.T) {
	// Do not parallelize; adjusts global timing knobs.
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	// Tighten TTL and increase slack to force a small op deadline.
	origTTL, origSlack := LockFileRegionDuration, LockDeadlineSlackPercent
	SetLockFileRegionDuration(200 * time.Millisecond)
	SetLockDeadlineSlackPercent(0.60)
	t.Cleanup(func() { SetLockFileRegionDuration(origTTL); SetLockDeadlineSlackPercent(origSlack) })

	hm := newHashmap(true, 8, rt, &mockCacheHashmap{base: mocks.NewMockClient(), isLockedAlways: true})

	// Seed a segment with one full block so read completes; write will block.
	seg := filepath.Join(base, "wdwrite", "wdwrite-1"+registryFileExtension)
	if err := os.MkdirAll(filepath.Dir(seg), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(seg, make([]byte, blockSize), 0o644); err != nil {
		t.Fatalf("seed seg: %v", err)
	}

	dio := newFileDirectIOInjected(blockingIO{readDelay: 10 * time.Millisecond, blockOnWrite: true})
	if err := dio.open(ctx, seg, os.O_RDWR, permission); err != nil {
		t.Fatalf("open dio: %v", err)
	}
	dio.filename = "wdwrite-1" + registryFileExtension

	start := time.Now()
	err = hm.updateFileBlockRegion(ctx, dio, 0, 0, make([]byte, sop.HandleSizeInBytes))
	took := time.Since(start)
	if err == nil || (!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled)) {
		t.Fatalf("expected timeout/cancel from write watchdog, got %v", err)
	}
	if took > 2*time.Second {
		t.Fatalf("deadline not enforced; operation took too long: %v", took)
	}
}

// Ensures that when the parent context has an earlier deadline than the
// derived lock-aligned budget, the earlier parent deadline is honored.
func Test_updateFileBlockRegion_ParentDeadlinePreemptsLockBudget(t *testing.T) {
	// Not parallel: modifies global knobs.
	baseCtx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(baseCtx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	// Give a generous lock TTL so parent deadline is the limiter.
	origTTL, origSlack := LockFileRegionDuration, LockDeadlineSlackPercent
	SetLockFileRegionDuration(5 * time.Second)
	SetLockDeadlineSlackPercent(0.05)
	t.Cleanup(func() { SetLockFileRegionDuration(origTTL); SetLockDeadlineSlackPercent(origSlack) })

	hm := newHashmap(true, 8, rt, &mockCacheHashmap{base: mocks.NewMockClient(), isLockedAlways: true})

	seg := filepath.Join(base, "pdl", "pdl-1"+registryFileExtension)
	if err := os.MkdirAll(filepath.Dir(seg), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(seg, make([]byte, blockSize), 0o644); err != nil {
		t.Fatalf("seed seg: %v", err)
	}

	// Parent context with tight deadline.
	parentTimeout := 120 * time.Millisecond
	pctx, cancel := context.WithTimeout(baseCtx, parentTimeout)
	defer cancel()

	// Inject blocking read so we hit context deadline while reading.
	dio := newFileDirectIOInjected(blockingIO{blockOnRead: true})
	if err := dio.open(baseCtx, seg, os.O_RDWR, permission); err != nil {
		t.Fatalf("open dio: %v", err)
	}
	dio.filename = "pdl-1" + registryFileExtension

	start := time.Now()
	err = hm.updateFileBlockRegion(pctx, dio, 0, 0, make([]byte, sop.HandleSizeInBytes))
	elapsed := time.Since(start)
	if err == nil || (!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled)) {
		t.Fatalf("expected context deadline error, got %v", err)
	}
	// Expect it to finish not far beyond parentTimeout (allow some scheduler slack).
	if elapsed > 2*parentTimeout {
		t.Fatalf("operation exceeded expected parent deadline window: elapsed=%v parent=%v", elapsed, parentTimeout)
	}
	if elapsed > 500*time.Millisecond { // sanity upper bound to ensure we didn't wait full lock TTL.
		t.Fatalf("elapsed suggests lock TTL used instead of parent: %v", elapsed)
	}
}
