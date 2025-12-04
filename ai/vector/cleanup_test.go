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

func TestOptimizeCleansUpSoftDeletedItems(t *testing.T) {
	// 1. Setup
	ctx := context.Background()
	path, _ := os.MkdirTemp("", "sop-ai-test-cleanup")
	defer os.RemoveAll(path)

	db := database.NewDatabase(core_database.Standalone, path)

	t1, _ := db.BeginTransaction(ctx, sop.ForWriting)

	cfg := vector.Config{
		UsageMode: ai.DynamicWithVectorCountTracking,
	}

	store, err := db.OpenVectorStore(ctx, "cleanup_test", t1, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// 2. Add Item
	id := "item1"
	if err := store.Upsert(ctx, ai.Item[map[string]any]{
		ID:      id,
		Vector:  []float32{1, 2, 3},
		Payload: map[string]any{"data": "payload"},
	}); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	t1.Commit(ctx)

	// 3. Delete Item (Soft Delete)
	t2, _ := db.BeginTransaction(ctx, sop.ForWriting)
	store, _ = db.OpenVectorStore(ctx, "cleanup_test", t2, cfg)

	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	t2.Commit(ctx)

	// Verify Soft Delete
	t3, _ := db.BeginTransaction(ctx, sop.ForWriting)
	store, _ = db.OpenVectorStore(ctx, "cleanup_test", t3, cfg)

	// Should not be found via Get
	if _, err := store.Get(ctx, id); err == nil {
		t.Errorf("Item should be not found after delete")
	}

	// But should exist in Content with Deleted=true
	contentStore, _ := store.Content(ctx)
	if found, _ := contentStore.Find(ctx, ai.ContentKey{ItemID: id}, false); !found {
		t.Errorf("Item should still exist in Content (soft deleted)")
	} else {
		k := contentStore.GetCurrentKey()
		if !k.Key.Deleted {
			t.Errorf("Item should be marked Deleted")
		}
	}
	t3.Commit(ctx)

	// 4. Run Optimize
	t4, _ := db.BeginTransaction(ctx, sop.ForWriting)
	store, _ = db.OpenVectorStore(ctx, "cleanup_test", t4, cfg)

	if err := store.Optimize(ctx); err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	// 5. Verify Physical Deletion
	t5, _ := db.BeginTransaction(ctx, sop.ForWriting)
	store, _ = db.OpenVectorStore(ctx, "cleanup_test", t5, cfg)
	contentStore, _ = store.Content(ctx)

	if found, _ := contentStore.Find(ctx, ai.ContentKey{ItemID: id}, false); found {
		t.Errorf("Item should have been physically deleted from Content after Optimize")
	}

	t5.Commit(ctx)
}
