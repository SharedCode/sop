package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// errMarshaler forces Marshal to fail; Unmarshal is a no-op to satisfy the interface.
type errMarshaler struct{}

func (e errMarshaler) Marshal(v any) ([]byte, error)      { return nil, errors.New("forced marshal error") }
func (e errMarshaler) Unmarshal(data []byte, v any) error { return nil }

func TestStoreRepository_Update_MarshalError_Undo(t *testing.T) {
	// This test exercises the Update path when encoding.Marshal fails, ensuring
	// the function aborts and previously persisted data remains unchanged.
	// Not parallel: modifies global encoding.BlobMarshaler.

	ctx := context.Background()
	base := t.TempDir()

	// Build a replication tracker with no replication.
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}

	cache := mocks.NewMockClient()
	sr, err := NewStoreRepository(ctx, rt, nil, cache, 0)
	if err != nil {
		t.Fatalf("NewStoreRepository: %v", err)
	}

	// Seed one store and persist.
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "u1", SlotLength: 8})
	if err := sr.Add(ctx, *si); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Override the marshaler to force an encode error during Update.
	prev := encoding.BlobMarshaler
	encoding.BlobMarshaler = errMarshaler{}
	t.Cleanup(func() { encoding.BlobMarshaler = prev })

	// Attempt to update Count via CountDelta; this should fail on Marshal and not modify file.
	_, uerr := sr.Update(ctx, []sop.StoreInfo{{Name: "u1", CountDelta: 1, CacheConfig: si.CacheConfig}})
	if uerr == nil || uerr.Error() != "forced marshal error" {
		t.Fatalf("expected forced marshal error, got %v", uerr)
	}

	// Verify storeinfo on disk did not change Count (remains 0) due to undo/abort.
	fn := filepath.Join(rt.getActiveBaseFolder(), "u1", storeInfoFilename)
	ba, rerr := NewFileIO().ReadFile(ctx, fn)
	if rerr != nil {
		t.Fatalf("ReadFile: %v", rerr)
	}
	var got sop.StoreInfo
	if err := encoding.Unmarshal(ba, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Count != 0 {
		t.Fatalf("expected Count to remain 0, got %d", got.Count)
	}

	// Small sanity: file exists.
	if _, statErr := os.Stat(fn); statErr != nil {
		t.Fatalf("Stat storeinfo: %v", statErr)
	}
}

func Test_copyFilesByExtension_ErrorPaths_Table(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	cases := []struct {
		name          string
		sourceDir     string
		targetDir     string
		wantErrSubstr string
		prep          func()
	}{
		{
			name:          "source read error (missing dir)",
			sourceDir:     filepath.Join(tmp, "missing-src"),
			targetDir:     filepath.Join(tmp, "dst1"),
			wantErrSubstr: "error reading source directory",
		},
		{
			name:          "target mkdir error (target is file)",
			sourceDir:     filepath.Join(tmp, "src2"),
			targetDir:     filepath.Join(tmp, "dst-file"),
			wantErrSubstr: "error creating target directory",
			prep: func() {
				_ = os.MkdirAll(filepath.Join(tmp, "src2"), 0o755)
				// Create a file at the targetDir path to force MkdirAll error
				_ = os.WriteFile(filepath.Join(tmp, "dst-file"), []byte("x"), 0o644)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.prep != nil {
				tc.prep()
			}
			err := copyFilesByExtension(ctx, tc.sourceDir, tc.targetDir, ".reg")
			if err == nil || (tc.wantErrSubstr != "" && !contains(err.Error(), tc.wantErrSubstr)) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrSubstr, err)
			}
		})
	}
}
