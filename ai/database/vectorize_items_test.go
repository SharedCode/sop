package database_test

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
	core "github.com/sharedcode/sop/database"
)

type MockEmbeddings struct{}

func (m *MockEmbeddings) Name() string { return "MockEmbeddings" }
func (m *MockEmbeddings) Dim() int     { return 3 }
func (m *MockEmbeddings) EmbedTexts(ctx context.Context, texts []string) ([][]float32, error) {
	var res [][]float32
	for range texts {
		res = append(res, []float32{1.1, 2.2, 3.3})
	}
	return res, nil
}
func (m *MockEmbeddings) Info() string { return "MockEmbeddings" }

type MockGen struct{}

func (m *MockGen) Name() string                                 { return "MockGen" }
func (m *MockGen) EstimateCost(inTokens, outTokens int) float64 { return 0.0 }
func (m *MockGen) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	return ai.GenOutput{Text: "mocked output"}, nil
}
func (m *MockGen) ModelInfo() string { return "MockGen" }

func TestVectorizeItems(t *testing.T) {
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
	kb.Store.AddCategory(ctx, &cat)

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

	// 1) Test VectorizeItems with itemIDs=nil (scan by category)
	err = db.VectorizeItems(ctx, "test_kb", llm, emb, 10, catID, nil)
	if err != nil {
		t.Fatalf("VectorizeItems (category scan) failed: %v", err)
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
		t.Errorf("Expected ItemCount to be 1, got %d", catVal.ItemCount)
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

	// 2) Add a second item and test VectorizeItems with explicit itemIDs
	tx3, _ := db.BeginTransaction(ctx, sop.ForWriting)
	kb3, _ := db.OpenKnowledgeBase(ctx, "test_kb", tx3, llm, emb)
	items3, _ := kb3.Store.Items(ctx)
	itemID2 := sop.NewUUID()
	itemKey2 := memory.ItemKey{CategoryID: catID, ItemID: itemID2}
	item2 := memory.Item[map[string]any]{
		ID:         itemID2,
		CategoryID: catID,
		Data:       map[string]any{"content": "explicit item 2"},
	}
	items3.Add(ctx, itemKey2, item2)
	tx3.Commit(ctx)

	err = db.VectorizeItems(ctx, "test_kb", llm, emb, 10, catID, []sop.UUID{itemID2})
	if err != nil {
		t.Fatalf("VectorizeItems (explicit items) failed: %v", err)
	}

	tx4, _ := db.BeginTransaction(ctx, sop.ForReading)
	kb4, _ := db.OpenKnowledgeBase(ctx, "test_kb", tx4, llm, emb)
	items4, _ := kb4.Store.Items(ctx)
	items4.Find(ctx, itemKey2, false)
	item2Val, _ := items4.GetCurrentValue(ctx)
	if len(item2Val.Positions) == 0 {
		t.Errorf("Explicit Item 2 was not vectorized")
	}
	tx4.Rollback(ctx)
}
