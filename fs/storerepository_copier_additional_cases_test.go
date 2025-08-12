package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// Covers StoreRepository.CopyToPassiveFolders when there are zero stores (GetAll returns nil) and when store info file copy occurs.
func TestStoreRepositoryCopyToPassiveFolders_NoStores(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()
	// replication tracker
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	// Store repo with no stores yet
	sr, err := NewStoreRepository(ctx, rt, nil, cache, 32)
	if err != nil {
		t.Fatalf("sr: %v", err)
	}
	// Should no-op successfully
	if err := sr.CopyToPassiveFolders(ctx); err != nil {
		t.Fatalf("CopyToPassiveFolders(empty): %v", err)
	}
}

func TestStoreRepositoryCopyToPassiveFolders_WithStore(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	sr, _ := NewStoreRepository(ctx, rt, nil, cache, 32)
	// Add a store so there is metadata to copy.
	store := sop.StoreInfo{Name: "s1", RegistryTable: "c1_r"}
	if err := sr.Add(ctx, store); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Simulate a registry segment file in active.
	segDir := filepath.Join(active, store.RegistryTable)
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	segFile := filepath.Join(segDir, store.RegistryTable+"-1"+registryFileExtension)
	if err := os.WriteFile(segFile, []byte("segment"), 0o644); err != nil {
		t.Fatalf("write seg: %v", err)
	}
	if err := sr.CopyToPassiveFolders(ctx); err != nil {
		t.Fatalf("CopyToPassiveFolders: %v", err)
	}
	// Verify file copied to passive
	copied := filepath.Join(passive, store.RegistryTable, store.RegistryTable+"-1"+registryFileExtension)
	if _, err := os.Stat(copied); err != nil {
		t.Fatalf("expected copied segment file: %v", err)
	}
}
