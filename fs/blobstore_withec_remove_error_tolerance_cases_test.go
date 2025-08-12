package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
)

// failingRemoveFileIO wraps FileIO to force a remove error on first attempt for each shard.
type failingRemoveFileIO struct {
	FileIO
	fail bool
}

func (f *failingRemoveFileIO) Remove(ctx context.Context, name string) error {
	if f.fail {
		f.fail = false
		return errors.New("remove shard fail")
	}
	return f.FileIO.Remove(ctx, name)
}

// TestBlobStoreWithEC_RemoveErrorTolerance ensures Remove tolerates shard delete errors (logged, not returned).
func TestBlobStoreWithEC_RemoveErrorTolerance(t *testing.T) {
	ctx := context.Background()
	base1 := filepath.Join(t.TempDir(), "d1")
	base2 := filepath.Join(t.TempDir(), "d2")
	cfg := map[string]ErasureCodingConfig{"tbl": {DataShardsCount: 1, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{base1, base2}}}

	fio := &failingRemoveFileIO{FileIO: NewFileIO(), fail: true}
	bsIntf, err := NewBlobStoreWithEC(nil, fio, cfg)
	if err != nil {
		t.Fatalf("new bs: %v", err)
	}
	bs := bsIntf.(*blobStoreWithEC)

	// Seed one blob so shard files exist.
	id := sop.NewUUID()
	if err := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "tbl", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("abc")}}}}); err != nil {
		t.Fatalf("add: %v", err)
	}
	// Ensure at least one shard file present.
	found := 0
	for _, d := range []string{base1, base2} {
		fp := DefaultToFilePath(filepath.Join(d, "tbl"), id)
		entries, _ := os.ReadDir(fp)
		if len(entries) > 0 {
			found++
		}
	}
	if found == 0 {
		t.Fatalf("expected shard files present")
	}

	// Remove with forced first error tolerated.
	if err := bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: "tbl", Blobs: []sop.UUID{id}}}); err != nil {
		t.Fatalf("remove should tolerate shard error: %v", err)
	}
}
