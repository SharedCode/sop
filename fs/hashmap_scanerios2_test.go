package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// lockFailOkFalse is a cache that returns ok=false with no error on Lock to simulate contention.
type lockFailOkFalse struct{ sop.Cache }

func (m lockFailOkFalse) Lock(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, sop.UUID, error) {
	return false, sop.NilUUID, nil
}

// Ensures setupNewFile surfaces lock-acquisition failure and does not create/truncate the segment.
func TestHashmap_setupNewFile_LockFailure_NoCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	hm := newHashmap(true, 8, rt, lockFailOkFalse{Cache: mocks.NewMockClient()})
	dio := newFileDirectIO()

	// Point directly to a would-be segment path; setupNewFile should fail before opening.
	table := "tbl_lockfail"
	seg := filepath.Join(base, table, table+"-1"+registryFileExtension)

	id := sop.NewUUID()
	if _, err := hm.setupNewFile(ctx, true, seg, id, dio); err == nil {
		t.Fatalf("expected lock failure error from setupNewFile")
	}
	if _, err := os.Stat(seg); !os.IsNotExist(err) {
		t.Fatalf("segment should not exist on lock failure; stat err=%v", err)
	}
}

// Ensure EOF on read path returns a valid write location when forWriting=true.
func TestHashmap_findOneFileRegion_EOF_ForWritingReturnsLocation(t *testing.T) {
	// Not parallel: modifies global DirectIOSim
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	hm := newHashmap(true, 16, rt, mocks.NewMockClient())

	// Use a DirectIO that always returns EOF on ReadAt to force EOF branch.
	prev := DirectIOSim
	DirectIOSim = directIOReadEOF{}
	t.Cleanup(func() { DirectIOSim = prev })

	table := "tblEOFWrite"
	id := sop.NewUUID()

	// Pre-create the expected segment file path with size >= segment size so findOneFileRegion
	// takes the read path (not setupNewFile), then ReadAt will return EOF.
	segDir := filepath.Join(rt.getActiveBaseFolder(), table)
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seg := filepath.Join(segDir, table+"-1"+registryFileExtension)
	if f, err := os.OpenFile(seg, os.O_CREATE|os.O_RDWR, 0o644); err != nil {
		t.Fatalf("create seg: %v", err)
	} else {
		_ = f.Close()
	}
	if err := os.Truncate(seg, hm.getSegmentFileSize()); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	frd, err := hm.findOneFileRegion(ctx, true, table, id)
	if err != nil {
		t.Fatalf("find(write): %v", err)
	}
	bo, so := hm.getBlockOffsetAndHandleInBlockOffset(id)
	if frd.blockOffset != bo || frd.handleInBlockOffset != so || frd.dio == nil {
		t.Fatalf("unexpected location: %+v want offsets (%d,%d)", frd, bo, so)
	}
}

// Ensures fetch surfaces non-notfound errors from findOneFileRegion (e.g., read error),
// rather than silently skipping as a missing ID.
func TestHashmap_Fetch_PropagatesNonNotFoundError(t *testing.T) {
	// Not parallel: modifies global DirectIOSim
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	hm := newHashmap(true, 16, rt, mocks.NewMockClient())

	// Inject DirectIO that returns a generic error on ReadAt, which should be propagated by fetch.
	prev := DirectIOSim
	DirectIOSim = directIOError{}
	t.Cleanup(func() { DirectIOSim = prev })

	table := "tblReadErr"
	// Pre-create segment file at the expected size so findOneFileRegion uses the read path.
	dir := filepath.Join(base, table)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seg := filepath.Join(dir, table+"-1"+registryFileExtension)
	if f, err := os.OpenFile(seg, os.O_CREATE|os.O_RDWR, 0o644); err != nil {
		t.Fatalf("create seg: %v", err)
	} else {
		_ = f.Close()
	}
	if err := os.Truncate(seg, hm.getSegmentFileSize()); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	_, err = hm.fetch(ctx, table, []sop.UUID{sop.NewUUID()})
	if err == nil {
		t.Fatalf("expected propagated error from fetch due to read failure")
	}
}

// fakeDirectIOCloseErr wraps os operations but forces Close to return an error.
type fakeDirectIOCloseErr struct{}

func (fakeDirectIOCloseErr) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) {
	// Ensure parent exists for convenience
	if dir := filepath.Dir(filename); dir != "." {
		_ = os.MkdirAll(dir, perm)
	}
	return os.OpenFile(filename, flag, perm)
}
func (fakeDirectIOCloseErr) WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.WriteAt(block, offset)
}
func (fakeDirectIOCloseErr) ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.ReadAt(block, offset)
}
func (fakeDirectIOCloseErr) Close(file *os.File) error {
	// Close the file, then return a forced error regardless of underlying result.
	_ = file.Close()
	return fmt.Errorf("forced close error")
}

// Ensures hashmap.close surfaces the (last) error from closing file handles.
func Test_hashmap_close_ReturnsError(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	// Prepare a fileDirectIO with an open file and a DirectIO whose Close returns error.
	// We don't rely on open(); we construct the struct manually to isolate the Close path.
	f := filepath.Join(base, "tmp.dat")
	fh, err := os.Create(f)
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}

	dio := &fileDirectIO{file: fh, filename: f, directIO: fakeDirectIOCloseErr{}}
	hm := &hashmap{fileHandles: map[string]*fileDirectIO{f: dio}}

	if e := hm.close(); e == nil {
		t.Fatalf("expected close to surface error")
	}
	// Subsequent close should be a no-op and not panic.
	if e := hm.close(); e != nil {
		t.Fatalf("second close should be nil, got %v", e)
	}
	_ = rt
}

// Verifies findOneFileRegion(forWriting=true) initializes a new segment file with expected size.
func Test_hashmap_setupNewFile_TruncatesToSegmentSize(t *testing.T) {
	// Not parallel: modifies DirectIOSim
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	hm := newHashmap(true, 16, rt, mocks.NewMockClient())

	table := "tblNew"
	id := sop.NewUUID()
	if _, err := hm.findOneFileRegion(ctx, true, table, id); err != nil {
		t.Fatalf("findOneFileRegion(new): %v", err)
	}
	// Validate that segment file exists with the expected preallocated size.
	seg := filepath.Join(rt.getActiveBaseFolder(), table, fmt.Sprintf("%s-1%s", table, registryFileExtension))
	st, err := os.Stat(seg)
	if err != nil {
		t.Fatalf("stat seg: %v", err)
	}
	want := hm.getSegmentFileSize()
	if st.Size() != want {
		t.Fatalf("segment size mismatch: got %d want %d", st.Size(), want)
	}
}

// Covers findOneFileRegion reading across multiple segment files: segment 1 has no match, segment 2 contains the handle.
func TestHashmap_findOneFileRegion_MultiSegmentScan_Read(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	cache := mocks.NewMockClient()
	hm := newHashmap(true, 16, rt, cache)

	table := "mscan"
	id := sop.NewUUID()
	blockOffset, slotOffset := hm.getBlockOffsetAndHandleInBlockOffset(id)

	// Prepare segment 1 and 2 files with full segment size; write the handle into segment 2 only.
	segDir := filepath.Join(base, table)
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seg1 := filepath.Join(segDir, table+"-1"+registryFileExtension)
	seg2 := filepath.Join(segDir, table+"-2"+registryFileExtension)
	// Ensure files exist before truncating to full segment size.
	if f, err := os.OpenFile(seg1, os.O_CREATE|os.O_RDWR, 0o644); err != nil {
		t.Fatalf("create seg1: %v", err)
	} else {
		_ = f.Close()
	}
	if f, err := os.OpenFile(seg2, os.O_CREATE|os.O_RDWR, 0o644); err != nil {
		t.Fatalf("create seg2: %v", err)
	} else {
		_ = f.Close()
	}
	if err := os.Truncate(seg1, hm.getSegmentFileSize()); err != nil {
		t.Fatalf("truncate1: %v", err)
	}
	if err := os.Truncate(seg2, hm.getSegmentFileSize()); err != nil {
		t.Fatalf("truncate2: %v", err)
	}
	// Write the handle into segment 2 at the ideal slot within the target block.
	f2, err := os.OpenFile(seg2, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open seg2: %v", err)
	}
	defer f2.Close()
	buf := make([]byte, blockSize)
	h := sop.NewHandle(id)
	mb := encoding.NewHandleMarshaler()
	hb, _ := mb.Marshal(h, make([]byte, 0, sop.HandleSizeInBytes))
	copy(buf[slotOffset:slotOffset+int64(len(hb))], hb)
	if _, err := f2.WriteAt(buf, blockOffset); err != nil {
		t.Fatalf("seed seg2 block: %v", err)
	}

	// Now locate via read; it should scan segment 1 then find in segment 2.
	frd, err := hm.findOneFileRegion(ctx, false, table, id)
	if err != nil {
		t.Fatalf("find(read): %v", err)
	}
	if frd.handle.LogicalID != id {
		t.Fatalf("unexpected handle id: got %s want %s", frd.handle.LogicalID.String(), id.String())
	}
	if frd.dio == nil || frd.dio.filename != table+"-2"+registryFileExtension {
		t.Fatalf("expected segment 2; got %+v", frd.dio)
	}
}

// Ensures forWriting=true returns the first free slot via scan when the ideal slot is occupied by another ID.
func TestHashmap_findOneFileRegion_ScanFindsFreeSlot_OnWrite(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	hm := newHashmap(true, 32, rt, mocks.NewMockClient())

	table := "scanfree"
	idTarget := sop.NewUUID()
	blockOffset, idealOffset := hm.getBlockOffsetAndHandleInBlockOffset(idTarget)

	// Prepare a full-size segment-1 file and write a block where the ideal slot contains a different handle.
	segDir := filepath.Join(base, table)
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seg1 := filepath.Join(segDir, table+"-1"+registryFileExtension)
	if f, err := os.OpenFile(seg1, os.O_CREATE|os.O_RDWR, 0o644); err != nil {
		t.Fatalf("create seg1: %v", err)
	} else {
		_ = f.Close()
	}
	if err := os.Truncate(seg1, hm.getSegmentFileSize()); err != nil {
		t.Fatalf("truncate seg1: %v", err)
	}

	// Seed the block: put another handle at the target's ideal slot; leave other slots zero.
	other := sop.NewHandle(sop.NewUUID())
	mb := encoding.NewHandleMarshaler()
	hb, _ := mb.Marshal(other, make([]byte, 0, sop.HandleSizeInBytes))
	blk := make([]byte, blockSize)
	copy(blk[idealOffset:idealOffset+int64(len(hb))], hb)
	f1, err := os.OpenFile(seg1, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open seg1: %v", err)
	}
	defer f1.Close()
	if _, err := f1.WriteAt(blk, blockOffset); err != nil {
		t.Fatalf("seed block: %v", err)
	}

	// Now request a write location for idTarget. Ideal slot is occupied; function should scan and return a different offset.
	frd, err := hm.findOneFileRegion(ctx, true, table, idTarget)
	if err != nil {
		t.Fatalf("find(write): %v", err)
	}
	if frd.dio == nil {
		t.Fatalf("expected non-nil dio")
	}
	if frd.handleInBlockOffset == idealOffset {
		t.Fatalf("expected scan to choose a free slot different from ideal; got same offset %d", frd.handleInBlockOffset)
	}
}

// Ensures read path finds a handle placed at a non-ideal slot via block scan.
func TestHashmap_findOneFileRegion_ScanFindsNonIdeal_OnRead(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	hm := newHashmap(true, 32, rt, mocks.NewMockClient())

	table := "scanread"
	id := sop.NewUUID()
	blockOffset, idealOffset := hm.getBlockOffsetAndHandleInBlockOffset(id)

	// Prepare full-size segment and seed the block with the handle at a non-ideal slot.
	dir := filepath.Join(base, table)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seg := filepath.Join(dir, table+"-1"+registryFileExtension)
	if f, err := os.OpenFile(seg, os.O_CREATE|os.O_RDWR, 0o644); err != nil {
		t.Fatalf("create seg: %v", err)
	} else {
		_ = f.Close()
	}
	if err := os.Truncate(seg, hm.getSegmentFileSize()); err != nil {
		t.Fatalf("truncate seg: %v", err)
	}

	// Choose a non-ideal offset inside the block.
	alt := idealOffset + int64(sop.HandleSizeInBytes)
	if alt+int64(sop.HandleSizeInBytes) > blockSize {
		alt = 0
		if idealOffset == 0 {
			alt = int64(sop.HandleSizeInBytes)
		}
	}

	blk := make([]byte, blockSize)
	h := sop.NewHandle(id)
	m := encoding.NewHandleMarshaler()
	hb, _ := m.Marshal(h, make([]byte, 0, sop.HandleSizeInBytes))
	copy(blk[alt:alt+int64(len(hb))], hb)

	f, err := os.OpenFile(seg, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open seg: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteAt(blk, blockOffset); err != nil {
		t.Fatalf("seed block: %v", err)
	}

	frd, err := hm.findOneFileRegion(ctx, false, table, id)
	if err != nil {
		t.Fatalf("find(read): %v", err)
	}
	if frd.handle.LogicalID != id || frd.handleInBlockOffset != alt {
		t.Fatalf("unexpected result: handle %s offset %d (want offset %d)", frd.handle.LogicalID.String(), frd.handleInBlockOffset, alt)
	}
}
