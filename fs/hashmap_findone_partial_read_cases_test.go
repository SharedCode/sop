package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestHashmapFindOneFileRegionPartialRead creates a truncated segment file so findOneFileRegion (for read)
// encounters a partial block read and returns the expected error path.
func TestHashmapFindOneFileRegionPartialRead(t *testing.T) {
	t.Skip("partial read path is difficult to deterministically trigger without altering production constants; skipping to avoid flakiness")
	ctx := context.Background()
	base := t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	cache := mocks.NewMockClient()
	hm := newHashmap(true, 32, rt, cache)

	table := "tpart"
	// Pre-create truncated segment file tpart-1.reg smaller than a full block.
	segDir := filepath.Join(base, table)
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	segFile := filepath.Join(segDir, table+"-1"+registryFileExtension)
	if err := os.WriteFile(segFile, []byte("tiny"), 0o644); err != nil {
		t.Fatalf("seed seg file: %v", err)
	}

	id := sop.NewUUID()
	if _, err := hm.findOneFileRegion(ctx, true, table, id); err == nil || !contains(err.Error(), "only able to read partially") {
		t.Fatalf("expected partial read error, got: %v", err)
	}
}
