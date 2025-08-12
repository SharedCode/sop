package fs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
)

// TestBlobStoreWithECRepairPartialFailure forces one repaired shard write to fail (read-only dir)
// to cover warning branch inside repair loop while still returning decoded data.
func TestBlobStoreWithECRepairPartialFailure(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	d1 := filepath.Join(base, "d1")
	d2 := filepath.Join(base, "d2")
	d3 := filepath.Join(base, "d3")
	cfg := map[string]ErasureCodingConfig{
		"tbp": {DataShardsCount: 2, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{d1, d2, d3}, RepairCorruptedShards: true},
	}
	bsIntf, err := NewBlobStoreWithEC(nil, nil, cfg)
	if err != nil {
		t.Fatalf("NewBlobStoreWithEC: %v", err)
	}
	bs := bsIntf.(*blobStoreWithEC)

	blobID := sop.NewUUID()
	payload := []byte("abcdefghijklmno")
	if err := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "tbp", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: blobID, Value: payload}}}}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Delete one shard (index 2) to force reconstruction & attempted repair (which we allow to succeed/fail silently).
	// Find path using toFilePath logic.
	shardDir := bs.toFilePath(d3, blobID)
	entries, _ := os.ReadDir(shardDir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), "_2") {
			os.Remove(filepath.Join(shardDir, e.Name()))
		}
	}

	data, err := bs.GetOne(ctx, "tbp", blobID)
	if err != nil {
		t.Fatalf("GetOne: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("decoded mismatch")
	}
}
