package fs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sharedcode/sop"
)

// failingMultiShardFileIO lets us induce write errors for multiple shard indices so we can
// exercise the Add() branch that exceeds parity tolerance (c > ParityShardsCount).
type failingMultiShardFileIO struct {
	// failWrites holds shard indices that should fail Writes (e.g. 0,1).
	failWrites map[int]struct{}
	mu         sync.Mutex
	// store keeps data only for shards that succeed so later GetOne works (not required for
	// the parity exceed test, but useful if extended).
	store map[string][]byte
}

func (f *failingMultiShardFileIO) WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error {
	// shard index is the numeric suffix after last underscore.
	parts := strings.Split(name, "_")
	if len(parts) > 0 {
		if idxStr := parts[len(parts)-1]; idxStr != "" {
			var idx int
			fmt.Sscanf(idxStr, "%d", &idx)
			if _, ok := f.failWrites[idx]; ok {
				return fmt.Errorf("induced write failure on shard %d", idx)
			}
		}
	}
	f.mu.Lock()
	f.store[name] = data
	f.mu.Unlock()
	return nil
}
func (f *failingMultiShardFileIO) ReadFile(ctx context.Context, name string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ba, ok := f.store[name]; ok {
		return ba, nil
	}
	return nil, fmt.Errorf("file %s not found", name)
}
func (f *failingMultiShardFileIO) Remove(ctx context.Context, name string) error {
	f.mu.Lock()
	delete(f.store, name)
	f.mu.Unlock()
	return nil
}
func (f *failingMultiShardFileIO) Exists(ctx context.Context, path string) bool     { return true }
func (f *failingMultiShardFileIO) RemoveAll(ctx context.Context, path string) error { return nil }
func (f *failingMultiShardFileIO) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	return nil
}
func (f *failingMultiShardFileIO) ReadDir(ctx context.Context, sourceDir string) ([]os.DirEntry, error) {
	return nil, nil
}

func newFailingMultiShardFileIO(fails ...int) *failingMultiShardFileIO {
	m := make(map[int]struct{}, len(fails))
	for _, i := range fails {
		m[i] = struct{}{}
	}
	return &failingMultiShardFileIO{failWrites: m, store: make(map[string][]byte)}
}

// Test that when more than parity shard writes fail, Add returns an error (rollback trigger).
func TestBlobStoreWithEC_AddExceedsParityTolerance(t *testing.T) {
	ctx := context.Background()
	// Parity=1 but we fail two shard writes (indices 0 and 1) to exceed tolerance.
	ec := map[string]ErasureCodingConfig{
		"bX": {DataShardsCount: 2, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{"d1", "d2", "d3"}},
	}
	fio := newFailingMultiShardFileIO(0, 1)
	bs, err := NewBlobStoreWithEC(DefaultToFilePath, fio, ec)
	if err != nil {
		t.Fatalf("NewBlobStoreWithEC: %v", err)
	}

	id := sop.NewUUID()
	gotErr := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "bX", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("abc")}}}})
	if gotErr == nil {
		t.Fatalf("expected error exceeding parity tolerance; got nil")
	}
	if !strings.Contains(gotErr.Error(), "induced") {
		t.Fatalf("unexpected error: %v", gotErr)
	}
}

// Test Add no-op when empty input slice supplied.
func TestBlobStoreWithEC_AddEmptyInput(t *testing.T) {
	bs, err := NewBlobStoreWithEC(DefaultToFilePath, newFileIOSim(), nil)
	if err != nil {
		t.Fatalf("NewBlobStoreWithEC: %v", err)
	}
	if err := bs.Add(context.Background(), nil); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// Test all shards fail to read so GetOne returns captured shard read error (isShardsEmpty path).
func TestBlobStoreWithEC_GetOneAllShardsMissing(t *testing.T) {
	ctx := context.Background()
	fileIO := newFileIOSim()
	bs, _ := NewBlobStoreWithEC(DefaultToFilePath, fileIO, nil)
	id := sop.NewUUID()
	data := []byte("payload")
	if err := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "b1", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: data}}}}); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Purge all 3 shard entries from the simulator and set two error flags so every ReadFile errors.
	// File names in simulator are of the form /<uuid>_<i>
	uid := id.String()
	sim := fileIO // same concrete type
	sim.locker.Lock()
	for i := 0; i < 3; i++ {
		delete(sim.lookup, filepath.Join("/", fmt.Sprintf("%s_%d", uid, i)))
	}
	sim.locker.Unlock()
	sim.setErrorOnSuffixNumber(0)
	sim.setErrorOnSuffixNumber2(1)

	if _, err := bs.GetOne(ctx, "b1", id); err == nil {
		t.Fatalf("expected error when all shards missing")
	}
}

// Test successful repair path: read fails for one shard (errorOnSuffixNumber2) but write repair succeeds.
func TestBlobStoreWithEC_RepairSuccessful(t *testing.T) {
	ctx := context.Background()
	fileIO := newFileIOSim()
	bs, _ := NewBlobStoreWithEC(DefaultToFilePath, fileIO, nil)
	id := sop.NewUUID()
	data := []byte{9, 9, 9, 9}
	if err := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "b1", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: data}}}}); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Trigger read failure for shard 1 only (using errorOnSuffixNumber2) so repair attempt uses WriteFile successfully.
	fileIO.setErrorOnSuffixNumber2(1)
	got, err := bs.GetOne(ctx, "b1", id)
	if err != nil {
		t.Fatalf("GetOne: %v", err)
	}
	if !errors.Is(err, nil) && len(got) == 0 {
		t.Fatalf("expected data reconstructed")
	}
	if string(got) != string(data) {
		t.Fatalf("data mismatch: %v vs %v", got, data)
	}
}
