package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sharedcode/sop"
)

// fileIOWithShardFail injects deterministic shard write errors based on shard index for a specific blob ID.
type fileIOWithShardFail struct {
	FileIO
	blobID       sop.UUID
	errorIndices map[int]struct{}
	mu           sync.Mutex // guards errorIndices
}

func (f *fileIOWithShardFail) WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error {
	// Only intercept writes for the target blob; filenames end with _<shardIndex>
	if strings.Contains(name, f.blobID.String()+"_") {
		underscore := strings.LastIndex(name, "_")
		if underscore > -1 {
			// Parse shard index suffix
			var idx int
			// We expect the format <uuid>_<index>; ignore parse errors (delegate to real write)
			if _, err := fmt.Sscanf(name[underscore+1:], "%d", &idx); err == nil {
				f.mu.Lock()
				_, shouldFail := f.errorIndices[idx]
				if shouldFail {
					delete(f.errorIndices, idx) // fail only once per shard index
					f.mu.Unlock()
					return fmt.Errorf("injected shard write failure index %d", idx)
				}
				f.mu.Unlock()
			}
		}
	}
	return f.FileIO.WriteFile(ctx, name, data, perm)
}

// Test parity exceed path in blobStoreWithEC.Add where shard write failures exceed tolerated parity count
// causing Add to return the last encountered write error (rollback signal to caller).
func TestBlobStoreWithECAddParityExceedRollback(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()

	table := []struct {
		name         string
		errorIndices []int // shard indices to force write errors
		expectErr    bool
	}{
		{name: "within_parity_tolerance", errorIndices: []int{1}, expectErr: false},        // parity=1 so 1 failure tolerated
		{name: "exceed_parity_triggers_error", errorIndices: []int{0, 2}, expectErr: true}, // 2 failures > parity triggers error
	}

	for _, tc := range table {
		// Each scenario isolated using subtests.
		t.Run(tc.name, func(t *testing.T) {
			// Config: 2 data shards + 1 parity shard => total 3 shards, parity tolerance 1.
			cfg := map[string]ErasureCodingConfig{
				"tbl": {DataShardsCount: 2, ParityShardsCount: 1, BaseFolderPathsAcrossDrives: []string{
					filepath.Join(base, tc.name, "d1"), filepath.Join(base, tc.name, "d2"), filepath.Join(base, tc.name, "d3"),
				}},
			}
			id := sop.NewUUID()

			// Compose failing FileIO: base on real default to honor directory ops.
			failMap := make(map[int]struct{}, len(tc.errorIndices))
			for _, i := range tc.errorIndices {
				failMap[i] = struct{}{}
			}
			fio := &fileIOWithShardFail{FileIO: NewFileIO(), blobID: id, errorIndices: failMap}

			bs, err := NewBlobStoreWithEC(nil, fio, cfg)
			if err != nil {
				t.Fatalf("NewBlobStoreWithEC: %v", err)
			}

			payload := []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{
				BlobTable: "tbl",
				Blobs:     []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("parity exceed test payload")}},
			}}

			err = bs.Add(ctx, payload)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error when shard write failures exceed parity tolerance")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error within parity tolerance: %v", err)
				}
				// Verify we can read the blob back when write tolerated.
				if got, rerr := bs.GetOne(ctx, "tbl", id); rerr != nil || len(got) == 0 {
					if rerr != nil {
						t.Fatalf("GetOne after tolerated write failures: %v", rerr)
					}
					if len(got) == 0 {
						t.Fatalf("GetOne returned empty payload")
					}
				}
			}
		})
	}
}
