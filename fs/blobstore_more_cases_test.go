package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
)

// Covers blobStore basic Add/GetOne/Update/Remove flows.
func TestBlobStoreBasicLifecycle(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	bs := NewBlobStore(nil, nil)
	// Override DefaultToFilePath by storing under temp base.
	// Use a custom toFilePath to pin root for deterministic layout.
	bs = NewBlobStore(func(root string, id sop.UUID) string { return filepath.Join(base, root) }, nil)

	id1 := sop.NewUUID()
	id2 := sop.NewUUID()
	payloads := []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		{BlobTable: "tbl", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{Key: id1, Value: []byte("hello")},
			{Key: id2, Value: []byte("world")},
		}},
	}
	if err := bs.Add(ctx, payloads); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// GetOne
	got, err := bs.GetOne(ctx, "tbl", id1)
	if err != nil || string(got) != "hello" {
		t.Fatalf("GetOne mismatch: %v %s", err, string(got))
	}
	// Update (reuse Add path) - modify id2
	payloads[0].Blobs[1].Value = []byte("planet")
	if err := bs.Update(ctx, payloads); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got2, _ := bs.GetOne(ctx, "tbl", id2)
	if string(got2) != "planet" {
		t.Fatalf("Update not reflected: %s", string(got2))
	}
	// Remove id1
	if err := bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: "tbl", Blobs: []sop.UUID{id1}}}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := bs.GetOne(ctx, "tbl", id1); err == nil {
		t.Fatalf("expected error reading removed blob")
	}
}

// Test blobStoreWithEC minimal Add/GetOne and Remove toleration of missing files.
func TestBlobStoreWithECBasic(t *testing.T) {
	ctx := context.Background()
	base1 := filepath.Join(t.TempDir(), "d1")
	base2 := filepath.Join(t.TempDir(), "d2")
	config := map[string]ErasureCodingConfig{
		"tbl": {DataShardsCount: 1, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{base1, base2}},
	}
	bs, err := NewBlobStoreWithEC(func(root string, id sop.UUID) string { return filepath.Join(root, id.String()[:2]) }, nil, config)
	if err != nil {
		t.Fatalf("NewBlobStoreWithEC: %v", err)
	}
	id := sop.NewUUID()
	payload := []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "tbl", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("ecdata")}}}}
	if err := bs.Add(ctx, payload); err != nil {
		t.Fatalf("Add ec: %v", err)
	}
	// Remove one shard manually to exercise decode with parity.
	shardDir := filepath.Join(base1, "tbl", id.String()[:2])
	// remove any shard file present
	entries, _ := os.ReadDir(shardDir)
	for _, e := range entries {
		os.Remove(filepath.Join(shardDir, e.Name()))
	}
	got, err := bs.GetOne(ctx, "tbl", id)
	if err != nil || string(got) != "ecdata" {
		t.Fatalf("GetOne ec mismatch: %v %s", err, string(got))
	}
	// Remove should tolerate already deleted shards
	if err := bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: "tbl", Blobs: []sop.UUID{id}}}); err != nil {
		t.Fatalf("Remove ec: %v", err)
	}
	// Remove again to hit non-existent branch
	_ = bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: "tbl", Blobs: []sop.UUID{id}}})
	_ = fmt.Sprintf("") // silence unused fmt if build tags differ
}
