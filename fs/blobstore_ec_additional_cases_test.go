package fs

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
)

// TestGlobalErasureConfigSetGet ensures global erasure coding config set/reset logic covered.
func TestGlobalErasureConfigSetGet(t *testing.T) {
	SetGlobalErasureConfig(nil)
	if GetGlobalErasureConfig() != nil {
		t.Fatalf("expected nil global config after reset")
	}
	cfg := map[string]ErasureCodingConfig{
		"": {DataShardsCount: 2, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{"x", "y", "z"}},
	}
	SetGlobalErasureConfig(cfg)
	if len(GetGlobalErasureConfig()) != 1 {
		t.Fatalf("expected 1 entry global config")
	}
}

// Negative config validation for EC: mismatch of shards vs folders.
func TestNewBlobStoreWithECConfigValidation(t *testing.T) {
	badCfg := map[string]ErasureCodingConfig{
		"t": {DataShardsCount: 2, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{"only-two"}}, // need 3 total
	}
	if _, err := NewBlobStoreWithEC(nil, nil, badCfg); err == nil {
		t.Fatalf("expected error for mismatched shard count vs folders")
	}
}

// Exercise blobStoreWithEC Add minimal happy path (single blob) and Remove tolerance when shard files absent.
func TestBlobStoreWithECAddRemoveAndGetOneErrors(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	cfg := map[string]ErasureCodingConfig{
		"table": {DataShardsCount: 2, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{
			filepath.Join(base, "d1"), filepath.Join(base, "d2"), filepath.Join(base, "d3")},
		},
	}
	bs, err := NewBlobStoreWithEC(nil, nil, cfg)
	if err != nil {
		t.Fatalf("NewBlobStoreWithEC: %v", err)
	}
	id := sop.NewUUID()
	addPayload := []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{
		BlobTable: "table",
		Blobs:     []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("hello world")}},
	}}
	if err := bs.Add(ctx, addPayload); err != nil {
		t.Fatalf("Add EC: %v", err)
	}

	// Force GetOne error by temporarily clearing global config mapping inside store (simulate missing config).
	w := bs.(*blobStoreWithEC)
	saved := w.baseFolderPathsAcrossDrives
	w.baseFolderPathsAcrossDrives = map[string][]string{} // empty -> lookup fails -> error
	if _, err := w.GetOne(ctx, "table", id); err == nil {
		t.Fatalf("expected error when config missing")
	}
	// restore then GetOne works
	w.baseFolderPathsAcrossDrives = saved
	if _, err := w.GetOne(ctx, "table", id); err != nil {
		t.Fatalf("GetOne after restore: %v", err)
	}

	// Remove existing then call Remove again with IDs (tolerates missing).
	rem := []sop.BlobsPayload[sop.UUID]{{BlobTable: "table", Blobs: []sop.UUID{id}}}
	if err := w.Remove(ctx, rem); err != nil {
		t.Fatalf("Remove shards: %v", err)
	}
	if err := w.Remove(ctx, rem); err != nil {
		t.Fatalf("Remove shards second time tolerated: %v", err)
	}
}

// Test isShardsEmpty helper explicitly.
func TestIsShardsEmptyHelper(t *testing.T) {
	if !isShardsEmpty(make([][]byte, 3)) {
		t.Fatalf("expected empty true")
	}
	shards := make([][]byte, 2)
	shards[1] = []byte("x")
	if isShardsEmpty(shards) {
		t.Fatalf("expected false when one shard present")
	}
}

// Simulate GetOne path when all shards failed: inject reader that returns error and verify propagated.
func TestBlobStoreWithECGetOneAllShardFailures(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	cfg := map[string]ErasureCodingConfig{
		"table": {DataShardsCount: 2, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{
			filepath.Join(base, "d1"), filepath.Join(base, "d2"), filepath.Join(base, "d3")},
		},
	}
	bs, err := NewBlobStoreWithEC(nil, errFileIOAlwaysErr{FileIO: NewFileIO()}, cfg)
	if err != nil {
		t.Fatalf("NewBlobStoreWithEC: %v", err)
	}
	id := sop.NewUUID()
	if _, err := bs.(*blobStoreWithEC).GetOne(ctx, "table", id); err == nil {
		t.Fatalf("expected shard read error when all fail")
	}
}

// errFileIOAlwaysErr implements FileIO.ReadFile to always fail (used by GetOne all shard failures test).
type errFileIOAlwaysErr struct{ FileIO }

func (e errFileIOAlwaysErr) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return nil, errors.New("boom")
}
