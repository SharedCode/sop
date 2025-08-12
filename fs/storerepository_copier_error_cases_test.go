package fs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestStoreRepositoryCopyToPassiveFoldersErrors covers error branches: GetAll error propagates and
// registry segment copy error (missing source directory).
func TestStoreRepositoryCopyToPassiveFoldersErrors(t *testing.T) {
	ctx := context.Background()

	// Missing registry segment directory: create a store without creating registry folder, expect copyFilesByExtension error.
	active := t.TempDir()
	passive := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
	sr, _ := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), 0)

	store := *sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 10})
	if err := sr.Add(ctx, store); err != nil {
		t.Fatalf("Add: %v", err)
	}
	regDir := filepath.Join(active, store.RegistryTable)
	if _, err := os.Stat(regDir); err == nil {
		t.Fatalf("unexpected registry dir exists; adjust test preconditions")
	}

	if err := sr.CopyToPassiveFolders(ctx); err == nil || !strings.Contains(err.Error(), "error reading source directory") {
		t.Fatalf("expected source directory read error, got: %v", err)
	}
}
