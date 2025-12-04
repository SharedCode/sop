package vector_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/vector"
	core_database "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/inredfs"
)

func TestOptimize_GracePeriod(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-grace-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storeName := "test_grace"

	// Initialize Database
	db := database.NewDatabase(core_database.Standalone, tmpDir)
	tx, err := db.BeginTransaction(context.Background(), sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	idx, err := db.OpenVectorStore(context.Background(), storeName, tx, vector.Config{
		UsageMode: ai.Dynamic,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Add some data to have a valid state
	if err := idx.Upsert(context.Background(), ai.Item[map[string]any]{
		ID:     "item1",
		Vector: []float32{1, 2, 3},
	}); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Simulate a failed optimization by creating the "next version" stores manually.
	// Current version is 0, so next version is 1.
	// The stores created during optimization are:
	// - storeName + "_lku_1"
	// - storeName + "_centroids_1"
	// - storeName + "_vecs_1"

	tx2, err := db.BeginTransaction(context.Background(), sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction 2 failed: %v", err)
	}

	lookupName := fmt.Sprintf("%s_lku_1", storeName)
	// We need to create this store so it exists in the registry and on disk.
	// We use the internal btree creation via the transaction if possible,
	// but here we can just use the database to open/create a generic btree.
	// Since database.OpenVectorStore abstracts this, we might need to access the underlying store creation.
	// However, the database interface doesn't expose generic B-Tree creation easily if it's not part of the public API.
	// But `sop` package has `NewStore`.

	// We can use the fact that `db` wraps `sop.StoreRepository` or similar.
	// Actually, `db` is `database.Database`.
	// Let's try to use `btree.New` with the transaction.

	// We need to match the store configuration used in Optimize.
	// It uses `sop.ConfigureStore(..., true, 1000, ...)`

	// Create the "failed" lookup store
	_, err = inredfs.NewBtree[int, string](context.Background(), sop.ConfigureStore(lookupName, true, 1000, "lookup", sop.SmallData, ""), tx2, func(a, b int) int { return a - b })
	if err != nil {
		t.Fatalf("Failed to create simulation store: %v", err)
	}

	if err := tx2.Commit(context.Background()); err != nil {
		t.Fatalf("Commit 2 failed: %v", err)
	}

	// Now we need to find the physical file and touch it.
	// In Standalone mode (fs), the store is a directory in tmpDir.
	// The directory name might be the store name or something derived.
	// Usually it is `tmpDir/lookupName`.
	// GetStoreFileStat checks for "storeinfo.txt" inside the store folder.
	storePath := filepath.Join(tmpDir, lookupName, "storeinfo.txt")

	// Verify it exists
	_, err = os.Stat(storePath)
	if err != nil {
		t.Logf("Store info file %s not found, listing tmpDir:", storePath)
		entries, _ := os.ReadDir(tmpDir)
		for _, e := range entries {
			t.Logf("- %s", e.Name())
			if e.IsDir() && e.Name() == lookupName {
				subEntries, _ := os.ReadDir(filepath.Join(tmpDir, e.Name()))
				for _, se := range subEntries {
					t.Logf("  - %s", se.Name())
				}
			}
		}
		t.Fatalf("Failed to stat store info file: %v", err)
	}

	// Case 1: Recent Modification (Grace Period Active)
	// It was just created, so it is recent. But let's be explicit.
	now := time.Now()
	if err := os.Chtimes(storePath, now, now); err != nil {
		t.Fatalf("Failed to chtimes: %v", err)
	}

	// Try to Optimize
	// We need a new transaction/context for the index.
	// Note: Optimize commits the transaction passed to it (or used to open it).
	// But `idx` was opened with `tx` which is already committed.
	// We need to re-open the index or start a new transaction.
	// `domainIndex` keeps `trans` internally.

	tx3, err := db.BeginTransaction(context.Background(), sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction 3 failed: %v", err)
	}

	idx3, err := db.OpenVectorStore(context.Background(), storeName, tx3, vector.Config{
		UsageMode: ai.Dynamic,
	})
	if err != nil {
		t.Fatalf("Open 3 failed: %v", err)
	}

	err = idx3.Optimize(context.Background())
	if err == nil {
		t.Fatal("Expected Optimize to fail due to grace period, but it succeeded")
	}
	expectedError := "grace period active"
	if err.Error() != "optimization aborted: grace period active for existing store "+lookupName {
		t.Errorf("Expected error containing %q, got %v", expectedError, err)
	}

	// Case 2: Old Modification (Grace Period Expired)
	// Set time to 2 hours ago
	oldTime := now.Add(-2 * time.Hour)
	if err := os.Chtimes(storePath, oldTime, oldTime); err != nil {
		t.Fatalf("Failed to chtimes old: %v", err)
	}

	// Try to Optimize again
	tx4, err := db.BeginTransaction(context.Background(), sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction 4 failed: %v", err)
	}

	idx4, err := db.OpenVectorStore(context.Background(), storeName, tx4, vector.Config{
		UsageMode: ai.Dynamic,
	})
	if err != nil {
		t.Fatalf("Open 4 failed: %v", err)
	}

	if err := idx4.Optimize(context.Background()); err != nil {
		t.Fatalf("Optimize failed after grace period: %v", err)
	}
}
