package vector_test

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/vector"
	core_database "github.com/sharedcode/sop/database"
)

func TestConfigurationMethods(t *testing.T) {
	// Test Config struct
	cfg := vector.Config{
		UsageMode: ai.Dynamic,
	}
	if cfg.UsageMode != ai.Dynamic {
		t.Errorf("Expected UsageMode to be Dynamic, got %v", cfg.UsageMode)
	}
}

/*
func TestUpsertBatchWithLookupAndGetLookup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-lookup-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db := NewDatabase[map[string]any](Standalone)
	db.SetStoragePath(tmpDir)
	idx := db.Open("test_lookup")

	// Create items
	items := []ai.Item{
		{ID: "item1", Vector: []float32{1, 1}, Meta: map[string]any{"label": "A"}},
		{ID: "item2", Vector: []float32{2, 2}, Meta: map[string]any{"label": "B"}},
	}

	// Test UpsertContent + IndexAll (New Flow)
	if err := idx.(*domainIndex).UpsertContent(items); err != nil {
		t.Fatalf("UpsertContent failed: %v", err)
	}
	if err := idx.(*domainIndex).IndexAll(); err != nil {
		t.Fatalf("IndexAll failed: %v", err)
	}

	// Test GetBySequenceID (was GetLookup)
	// IndexAll starts sequence at 0
	item, err := idx.(*domainIndex).GetBySequenceID(0)
	if err != nil {
		t.Fatalf("GetBySequenceID(0) failed: %v", err)
	}
	if item.ID != "item1" {
		t.Errorf("Expected item1, got %s", item.ID)
	}

	item, err = idx.(*domainIndex).GetBySequenceID(1)
	if err != nil {
		t.Fatalf("GetBySequenceID(1) failed: %v", err)
	}
	if item.ID != "item2" {
		t.Errorf("Expected item2, got %s", item.ID)
	}

	// Test GetBySequenceID failure
	_, err = idx.(*domainIndex).GetBySequenceID(999)
	if err == nil {
		t.Error("Expected error for non-existent lookup ID, got nil")
	}
}
*/

/*
func TestSeedCentroidsAndIterateAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-seed-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db := NewDatabase[map[string]any](Standalone)
	db.SetStoragePath(tmpDir)
	idx := db.Open("test_seed")

	// Test SeedCentroids
	centroids := map[int][]float32{
		1: {1.0, 1.0},
		2: {10.0, 10.0},
	}
	if err := idx.(*domainIndex).SeedCentroids(centroids); err != nil {
		t.Fatalf("SeedCentroids failed: %v", err)
	}

	// Add some items that will use these centroids
	items := []ai.Item{
		{ID: "item1", Vector: []float32{1.1, 1.1}, Meta: map[string]any{"val": 1}},
		{ID: "item2", Vector: []float32{10.1, 10.1}, Meta: map[string]any{"val": 2}},
	}
	if err := idx.UpsertBatch(items); err != nil {
		t.Fatalf("UpsertBatch failed: %v", err)
	}

	// Test Count
	count, err := idx.(*domainIndex).Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	// Test IterateAll
	itemCount := 0
	err = idx.(*domainIndex).IterateAll(func(item ai.Item) error {
		itemCount++
		if item.ID == "item1" {
			if item.Meta["val"].(float64) != 1 {
				return fmt.Errorf("item1 meta mismatch")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("IterateAll failed: %v", err)
	}
	if itemCount != 2 {
		t.Errorf("Expected to iterate 2 items, got %d", itemCount)
	}
}
*/

func TestArchitectureDirectMethods(t *testing.T) {
	// These methods (Add, Search) in Architecture struct seem to be helpers or demo code
	// but since they are exported, we should test them or at least cover them.

	tmpDir, err := os.MkdirTemp("", "sop-ai-test-arch-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// We need to manually setup the environment to call Architecture methods directly
	// This is a bit involved because Architecture expects a transaction.
	// We can reuse the Database helper to get a transaction.

	db := database.NewDatabase(core_database.Standalone, tmpDir)

	ctx := context.Background()
	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	defer trans.Rollback(ctx)

	arch, err := vector.OpenDomainStore(ctx, trans, "test_arch", 1, sop.MediumData, false)
	if err != nil {
		t.Fatalf("OpenDomainStore failed: %v", err)
	}

	// Test Add
	if err := arch.Add(ctx, "arch-item-1", []float32{1, 2}, `{"foo":"bar"}`); err != nil {
		t.Fatalf("Architecture.Add failed: %v", err)
	}

	// Test Search
	// Search expects centroids to be populated?
	// The Add method hardcodes centroidID := 1.
	// The Search method hardcodes targetCentroid := 1.
	// So it should work if we just Add then Search.

	results, err := arch.Search(ctx, []float32{1, 2}, 1)
	if err != nil {
		t.Fatalf("Architecture.Search failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if results[0] != `{"foo":"bar"}` {
		t.Errorf("Expected data, got %s", results[0])
	}
}

/*
func TestUpdateExistingItem(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-update-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db := NewDatabase[map[string]any](Standalone)
	db.SetStoragePath(tmpDir)
	idx := db.Open("test_update")

	// 1. Insert Item
	item := ai.Item{ID: "item1", Vector: []float32{1, 1}, Meta: map[string]any{"ver": 1}}
	if err := idx.UpsertBatch([]ai.Item{item}); err != nil {
		t.Fatalf("UpsertBatch failed: %v", err)
	}

	// 2. Update Item (New Vector, New Meta)
	// New vector {10,10} should move it to a different centroid if we had multiple,
	// but here we might only have 1 centroid.
	// Let's force a centroid seed to ensure we have 2 centroids to test moving.
	centroids := map[int][]float32{
		1: {1.0, 1.0},
		2: {10.0, 10.0},
	}
	if err := idx.(*domainIndex).SeedCentroids(centroids); err != nil {
		t.Fatalf("SeedCentroids failed: %v", err)
	}

	// Update item to be closer to centroid 2
	itemV2 := ai.Item{ID: "item1", Vector: []float32{10, 10}, Meta: map[string]any{"ver": 2}}
	if err := idx.UpsertBatch([]ai.Item{itemV2}); err != nil {
		t.Fatalf("UpsertBatch update failed: %v", err)
	}

	// 3. Verify
	fetched, err := idx.Get("item1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if fetched.Meta["ver"].(float64) != 2 {
		t.Errorf("Expected ver 2, got %v", fetched.Meta["ver"])
	}
	// Check internal centroid ID if possible, or just trust the system.
	// We can check if the old vector is gone?
	// Since we don't have direct access to Vectors B-Tree here easily without opening store,
	// we rely on Get returning the correct new data.
}
*/
