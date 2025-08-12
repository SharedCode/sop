package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// TestHashmap_FindOneFileRegion_ScanBranch crafts an on-disk state where the target handle
// resides in a non-ideal slot with the ideal slot empty, forcing the scan loop path.
// This scenario is artificially constructed (production logic normally places a handle in its ideal slot
// if empty) but is useful to exercise the scanning branch for coverage.
func TestHashmap_FindOneFileRegion_ScanBranch(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
	hm := newHashmap(true, 64, rt, l2) // modest mod value

	id := sop.NewUUID()
	high, low := id.Split()
	blockOffset, idealOffset := hm.getBlockOffsetAndHandleInBlockOffset(id)
	_ = high
	idealSlot := (low % uint64(handlesPerBlock))
	// Choose a different slot (wrap if at end).
	var altSlot uint64
	if idealSlot == uint64(handlesPerBlock-1) {
		altSlot = 0
	} else {
		altSlot = idealSlot + 1
	}
	altOffset := int64(altSlot * sop.HandleSizeInBytes)

	// Prepare aligned block with only alt slot populated.
	blk := make([]byte, blockSize)
	if altOffset == idealOffset {
		t.Fatalf("altOffset unexpectedly equals idealOffset (logic error)")
	}

	// Marshal handle bytes.
	m := encoding.NewHandleMarshaler()
	h := sop.NewHandle(id)
	ba, _ := m.Marshal(h, make([]byte, 0, sop.HandleSizeInBytes))
	copy(blk[altOffset:altOffset+int64(len(ba))], ba)

	// Create segment file path: <base>/<name>/<name>-1.reg
	name := "scanmm"
	segDir := filepath.Join(base, name)
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	segFile := filepath.Join(segDir, name+"-1"+registryFileExtension)
	f, err := os.OpenFile(segFile, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open seg: %v", err)
	}
	defer f.Close()
	// Ensure file sized to full expected segment size so code treats it as existing & valid.
	if err := f.Truncate(hm.getSegmentFileSize()); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := f.WriteAt(blk, blockOffset); err != nil {
		t.Fatalf("write block: %v", err)
	}

	// Invoke fetch which uses findOneFileRegion(forWriting=false) internally via fetch.
	res, err := hm.fetch(ctx, name, []sop.UUID{id})
	if err != nil {
		t.Fatalf("fetch err: %v", err)
	}
	if len(res) != 1 || res[0].LogicalID != id {
		t.Fatalf("expected 1 handle via scan path, got %+v", res)
	}
}
