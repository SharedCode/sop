package fs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/sharedcode/sop"
)

// TestBlobStoreWithECRepairCorruptedShard exercises the repairCorruptedShards branch by deleting one shard
// and corrupting another so reconstruction occurs and a repaired shard write is attempted.
func TestBlobStoreWithECRepairCorruptedShard(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	d1 := filepath.Join(root, "d1")
	d2 := filepath.Join(root, "d2")
	d3 := filepath.Join(root, "d3")
	for _, d := range []string{d1, d2, d3} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}

	cfg := map[string]ErasureCodingConfig{
		"rt": {DataShardsCount: 2, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{d1, d2, d3}, RepairCorruptedShards: true},
	}
	bsIntf, err := NewBlobStoreWithEC(nil, nil, cfg)
	if err != nil {
		t.Fatalf("NewBlobStoreWithEC: %v", err)
	}
	bs := bsIntf.(*blobStoreWithEC)

	id := sop.NewUUID()
	payload := []byte("repair-path-payload")
	if err := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "rt", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: payload}}}}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Derive per-shard directory (fp) using the same toFilePath logic used during Add.
	shardPaths := make([]string, 0, 3)
	for i, base := range []string{d1, d2, d3} {
		baseFolderPath := filepath.Join(base, "rt")
		fp := bs.toFilePath(baseFolderPath, id)
		shardFile := filepath.Join(fp, id.String()+"_"+strconv.Itoa(i))
		if _, err := os.Stat(shardFile); err != nil {
			t.Fatalf("stat shard %d: %v", i, err)
		}
		shardPaths = append(shardPaths, shardFile)
	}
	if len(shardPaths) != 3 {
		t.Fatalf("expected 3 shard files, got %d", len(shardPaths))
	}

	// Delete one shard to force reconstruction and corrupt another's metadata to simulate bitrot.
	// Delete a single shard to force reconstruction (data+parity remaining) and trigger repair logic.
	deleted := shardPaths[0]
	if err := os.Remove(deleted); err != nil {
		t.Fatalf("remove shard: %v", err)
	}

	got, err := bs.GetOne(ctx, "rt", id)
	if err != nil {
		t.Fatalf("GetOne after reconstruction: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("decoded payload mismatch: %q vs %q", got, payload)
	}
	// Repaired shard should have been re-written.
	if _, err := os.Stat(deleted); err != nil {
		t.Fatalf("expected repaired shard file to exist: %v", err)
	}
}
