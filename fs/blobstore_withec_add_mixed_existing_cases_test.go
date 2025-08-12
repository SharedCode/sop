package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
)

// TestBlobStoreWithEC_AddMixedExisting ensures Add handles mixture of pre-existing and new shard directories without error.
func TestBlobStoreWithEC_AddMixedExisting(t *testing.T) {
	ctx := context.Background()
	base1 := filepath.Join(t.TempDir(), "d1")
	base2 := filepath.Join(t.TempDir(), "d2")
	cfg := map[string]ErasureCodingConfig{"tbl2": {DataShardsCount: 1, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{base1, base2}}}

	bsIntf, err := NewBlobStoreWithEC(nil, nil, cfg)
	if err != nil {
		t.Fatalf("new bs: %v", err)
	}
	bs := bsIntf.(*blobStoreWithEC)

	id1 := sop.NewUUID()
	// Pre-create one shard directory (drive 1) to simulate existing path.
	prefp := DefaultToFilePath(filepath.Join(base1, "tbl2"), id1)
	if err := os.MkdirAll(prefp, 0o755); err != nil {
		t.Fatalf("mkdir pre: %v", err)
	}

	if err := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "tbl2", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id1, Value: []byte("xyz")}}}}); err != nil {
		t.Fatalf("add mixed existing: %v", err)
	}

	// Validate both shard files written.
	for i, d := range []string{base1, base2} {
		fp := DefaultToFilePath(filepath.Join(d, "tbl2"), id1)
		fn := filepath.Join(fp, id1.String()+"_"+string('0'+byte(i)))
		if _, err := os.Stat(fn); err != nil {
			t.Fatalf("expected shard file %s: %v", fn, err)
		}
	}
}
