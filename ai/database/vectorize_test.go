package database_test

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
	core "github.com/sharedcode/sop/database"
)

func TestVectorize(t *testing.T) {
	storagePath := t.TempDir()

	db := database.NewDatabase(core.DatabaseOptions{
		StoresFolders: []string{storagePath},
	})
	ctx := context.Background()
	llm := &MockGen{}
	emb := &MockEmbeddings{}

	// Setup: create knowledge base, category, and items
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}

	kb, err := db.OpenKnowledgeBase(ctx, "test_kb", tx, llm, emb)
	if err != nil {
		t.Fatalf("OpenKnowledgeBase: %v", err)
	}

	cats, err := kb.Store.Categories(ctx)
	if err != nil {
		t.Fatalf("Categories: %v", err)
	}
	items, err := kb.Store.Items(ctx)
	if err != nil {
		t.Fatalf("Items: %v", err)
	}

	catID := sop.NewUUID()
	cat := memory.Category{
		ID:          catID,
		Name:        "Test Category",
		Description: "A category for test",
	}
	cats.Add(ctx, catID, &cat)

	itemID1 := sop.NewUUID()
	itemKey1 := memory.ItemKey{CategoryID: catID, ItemID: itemID1}
	item1 := memory.Item[map[string]any]{
		ID:         itemID1,
		CategoryID: catID,
		Data:       map[string]any{"content": "test item 1"},
	}
	items.Add(ctx, itemKey1, item1)

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Setup Commit: %v", err)
	}

	// 1) Test Vectorize (scan entire category + specific items dynamically if we had to, but it handles all)
	err = db.Vectorize(ctx, "test_kb", llm, emb, 10)
	if err != nil {
		t.Fatalf("Vectorize failed: %v", err)
	}

	// Read check
	tx2, _ := db.BeginTransaction(ctx, sop.ForReading)
	kb2, _ := db.OpenKnowledgeBase(ctx, "test_kb", tx2, llm, emb)
	cats2, _ := kb2.Store.Categories(ctx)
	found, _ := cats2.Find(ctx, catID, false)
	if !found {
		t.Errorf("Category not found")
	}
	catVal, _ := cats2.GetCurrentValue(ctx)
	if len(catVal.CenterVector) == 0 {
		t.Errorf("Category was not vectorized")
	}
	if catVal.ItemCount != 1 {
		t.Errorf("Expected ItemCount to be 1 after full Vectorize, got %d", catVal.ItemCount)
	}

	items2, _ := kb2.Store.Items(ctx)
	foundItem, _ := items2.Find(ctx, itemKey1, false)
	if !foundItem {
		t.Errorf("Item not found")
	}
	itemVal, _ := items2.GetCurrentValue(ctx)
	if len(itemVal.Positions) == 0 {
		t.Errorf("Item was not vectorized")
	}
	tx2.Rollback(ctx)
}
