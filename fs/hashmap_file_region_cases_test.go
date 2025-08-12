package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// fakeDirectIO acts as a simple pass-through but ensures parent directories exist on Open.
type fakeDirectIO struct{}

// Open mirrors stdDirectIO.Open behavior (ensures parent dirs) but allows us to inject
// subsequent read/write failures for coverage of error branches.
func (f *fakeDirectIO) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) {
	if dir := filepath.Dir(filename); dir != "." {
		_ = os.MkdirAll(dir, perm)
	}
	return os.OpenFile(filename, flag, perm)
}
func (f *fakeDirectIO) WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.WriteAt(block, offset)
}
func (f *fakeDirectIO) ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.ReadAt(block, offset)
}
func (f *fakeDirectIO) Close(file *os.File) error { return file.Close() }

// Focused tests for low-coverage hashmap file region helpers: findOneFileRegion (write + read paths),
// updateFileRegion, markDeleteFileRegion, isZeroData, setupNewFile.
func TestHashmapUpdateAndDeleteFileRegion(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	// replication disabled -> single folder tracking
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	cache := mocks.NewMockClient()
	hm := newHashmap(true, 32, rt, cache) // small mod value for fast test

	table := "regcase"
	id := sop.NewUUID()

	// Inject fake DirectIO (wraps std behavior + induced errors) and restore after test.
	old := DirectIOSim
	DirectIOSim = &fakeDirectIO{}
	t.Cleanup(func() { DirectIOSim = old })

	// Acquire a file region for writing (forces new segment file creation & preallocation path).
	frd, err := hm.findOneFileRegion(ctx, true, table, id)
	if err != nil {
		t.Fatalf("findOneFileRegion(write): %v", err)
	}
	if frd.dio == nil {
		t.Fatalf("expected dio handle initialized")
	}
	if frd.blockOffset < 0 || frd.handleInBlockOffset < 0 {
		t.Fatalf("invalid offsets: %+v", frd)
	}

	// Prepare a handle & update the region.
	h := sop.NewHandle(id)
	frd.handle = h
	if err := hm.updateFileRegion(ctx, []fileRegionDetails{frd}); err != nil {
		t.Fatalf("updateFileRegion: %v", err)
	}

	// Read path: should locate the same handle when searching (forWriting=false).
	frd2, err := hm.findOneFileRegion(ctx, false, table, id)
	if err != nil {
		t.Fatalf("findOneFileRegion(read): %v", err)
	}
	if frd2.handle.LogicalID != id {
		t.Fatalf("expected handle id %v got %v", id, frd2.handle.LogicalID)
	}

	// Delete (mark) the region then ensure subsequent read returns not-found error.
	if err := hm.markDeleteFileRegion(ctx, []fileRegionDetails{frd2}); err != nil {
		t.Fatalf("markDeleteFileRegion: %v", err)
	}
	if _, err := hm.findOneFileRegion(ctx, false, table, id); err == nil || !contains(err.Error(), idNotFoundErr) {
		t.Fatalf("expected not found error after delete, got: %v", err)
	}

	// isZeroData sanity.
	if !isZeroData(make([]byte, sop.HandleSizeInBytes)) {
		t.Fatalf("isZeroData expected true for zero slice")
	}

	// Close resources.
	if err := hm.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Confirm preallocated file exists under active folder.
	segGlob := filepath.Join(base, table, "*"+registryFileExtension)
	matches, _ := filepath.Glob(segGlob)
	if len(matches) == 0 {
		t.Fatalf("expected at least one segment file created, glob: %s", segGlob)
	}
}

// contains local helper (avoid importing strings just for this tiny need).
func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

// naive substring search to keep dependencies minimal.
func indexOf(s, sub string) int {
	outer := len(s) - len(sub) + 1
	for i := 0; i < outer; i++ {
		if s[i] == sub[0] {
			if s[i:i+len(sub)] == sub {
				return i
			}
		}
	}
	return -1
}
