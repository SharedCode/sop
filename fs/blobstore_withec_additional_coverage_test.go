package fs

import (
	"context"
	"github.com/sharedcode/sop"
	"testing"
)

// Additional micro tests to exercise early error branches in blobstore.withec.go
// when a per-table erasure coding configuration cannot be found (and no global
// default exists). Legacy granular tests covered these implicitly; after
// consolidation they were uncovered, so we isolate them here.

func TestBlobStoreWithEC_GetOneMissingConfig(t *testing.T) {
	ctx := context.Background()
	cfg := map[string]ErasureCodingConfig{"present": {DataShardsCount: 1, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{"d1", "d2"}}}
	bs, err := NewBlobStoreWithEC(nil, nil, cfg)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, e := bs.(*blobStoreWithEC).GetOne(ctx, "absent", sop.NewUUID()); e == nil {
		t.Fatalf("expected missing config error for GetOne")
	}
}

func TestBlobStoreWithEC_AddMissingConfig(t *testing.T) {
	ctx := context.Background()
	cfg := map[string]ErasureCodingConfig{"only": {DataShardsCount: 1, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{"d1", "d2"}}}
	bs, err := NewBlobStoreWithEC(nil, nil, cfg)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	id := sop.NewUUID()
	if e := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "abs", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("x")}}}}); e == nil {
		t.Fatalf("expected missing config error for Add")
	}
}

func TestBlobStoreWithEC_RemoveMissingConfig(t *testing.T) {
	ctx := context.Background()
	cfg := map[string]ErasureCodingConfig{"oth": {DataShardsCount: 1, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{"d1", "d2"}}}
	bs, err := NewBlobStoreWithEC(nil, nil, cfg)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if e := bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: "zzz", Blobs: []sop.UUID{sop.NewUUID()}}}); e == nil {
		t.Fatalf("expected missing config error for Remove")
	}
}
