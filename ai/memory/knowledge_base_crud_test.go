package memory

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/inmemory"
)

func prepareTestStore() *store[string] {
	cats := inmemory.NewBtree[sop.UUID, *Category](true)
	vecs := inmemory.NewBtree[VectorKey, Vector](true)
	items := inmemory.NewBtree[ItemKey, Item[string]](true)
	pathTree := inmemory.NewBtree[string, sop.UUID](false)
	distTree := inmemory.NewBtree[DistanceKey, byte](false)
	docsTree := inmemory.NewBtree[sop.UUID, Document](false)

	s := NewStore[string]("test_kb", nil, cats.Btree, pathTree.Btree, distTree.Btree, vecs.Btree, items.Btree, docsTree.Btree).(*store[string])
	s.SetTextIndex(&MockTextIndex{})
	s.SetLLM(&MockPlaybookLLM{})
	return s
}

func TestBatchCategories(t *testing.T) {
	ctx := context.Background()
	s := prepareTestStore()
	kb := &KnowledgeBase[string]{Store: s}

	rootID := sop.NewUUID()
	childID := sop.NewUUID()

	// 1. Upsert Categories
	err := kb.UpsertCategories(ctx, []UpsertCategoryParam{
		{
			Category: &Category{ID: rootID, Name: "Root"},
		},
		{
			ParentIDs: []sop.UUID{rootID},
			Category:  &Category{ID: childID, Name: "Child"},
		},
	})
	if err != nil {
		t.Fatalf("UpsertCategories failed: %v", err)
	}

	// 2. List Categories

	allCats, allCount, err := kb.ListCategories(ctx, ListCategoriesParam{Limit: 10})
	if err != nil {
		t.Fatalf("ListCategories failed: %v", err)
	}
	if allCount != 2 {
		t.Errorf("Expected 2 categories, got %d", allCount)
	}
	if len(allCats) != 2 {
		t.Errorf("Expected 2 categories in slice, got %d", len(allCats))
	}

	rootCats, rootCount, err := kb.ListCategories(ctx, ListCategoriesParam{Limit: 10, ParentPath: "/"})
	if err != nil {
		t.Fatalf("ListCategories failed: %v", err)
	}
	if rootCount != 1 {
		t.Errorf("Expected 1 root category, got %d", rootCount)
	}
	if len(rootCats) != 1 {
		t.Errorf("Expected 1 root category in slice, got %d", len(rootCats))
	}

	// 3. Delete Categories
	err = kb.DeleteCategories(ctx, []sop.UUID{childID})
	if err != nil {
		t.Fatalf("DeleteCategories failed: %v", err)
	}

	// 4. Verify Deletion
	_, countC, err := kb.ListCategories(ctx, ListCategoriesParam{Limit: 10})
	if err != nil {
		t.Fatalf("ListCategories failed: %v", err)
	}
	if countC != 1 {
		t.Errorf("Expected 1 category after deletion, got %d", countC)
	}
}

func TestBatchItemsAndSearch(t *testing.T) {
	ctx := context.Background()
	s := prepareTestStore()
	kb := &KnowledgeBase[string]{Store: s}

	catID := sop.NewUUID()
	err := kb.UpsertCategories(ctx, []UpsertCategoryParam{
		{Category: &Category{ID: catID, Name: "DataCategory"}},
	})
	if err != nil {
		t.Fatalf("UpsertCategories failed: %v", err)
	}

	// Also map the path explicitly since SearchByPath relies on CategoriesByPath
	catPathTree, _ := s.CategoriesByPath(ctx)
	catPathTree.Add(ctx, "DataCategory", catID)

	item1ID := sop.NewUUID()
	item2ID := sop.NewUUID()

	err = kb.UpsertItems(ctx, []UpsertItemParam[string]{
		{
			CategoryID: catID,
			Item:       &Item[string]{ID: item1ID, CategoryID: catID, Data: "item data 1", Summaries: []string{"Searchable Item 1"}},
		},
		{
			CategoryID: catID,
			Item:       &Item[string]{ID: item2ID, CategoryID: catID, Data: "item data 2", Summaries: []string{"Hidden Item 2"}},
		},
	})
	if err != nil {
		t.Fatalf("UpsertItems failed: %v", err)
	}

	// Test ListItems
	_, count, err := kb.ListItems(ctx, ListItemsParam{CategoryPath: "DataCategory", Limit: 10})
	if err != nil {
		t.Fatalf("ListItems failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 items, got %d", count)
	}

	// Test SearchByPath
	res, err := kb.SearchByPath(ctx, []PathSearchParam{
		{CategoryPath: "DataCategory", SearchText: "Searchable"},
	})
	if err != nil {
		t.Fatalf("SearchByPath failed: %v", err)
	}
	if len(res) != 1 {
		t.Errorf("Expected 1 search result, got %d", len(res))
	} else if !strings.HasPrefix(res[0].Summaries[0], "Searchable") {
		t.Errorf("Expected item with 'Searchable', got '%s'", res[0].Summaries[0])
	}

	// Test DeleteItems
	err = kb.DeleteItems(ctx, []ItemKey{{CategoryID: catID, ItemID: item1ID}})
	if err != nil {
		t.Fatalf("DeleteItems failed: %v", err)
	}

	_, count, err = kb.ListItems(ctx, ListItemsParam{CategoryPath: "DataCategory", Limit: 10})
	if err != nil {
		t.Fatalf("ListItems failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 item after deletion, got %d", count)
	}
}

func TestKnowledgeBaseEdgeCases(t *testing.T) {
	ctx := context.Background()
	s := prepareTestStore()
	kb := &KnowledgeBase[string]{Store: s}

	// Create 5 categories
	upserts := make([]UpsertCategoryParam, 5)
	for i := 0; i < 5; i++ {
		upserts[i] = UpsertCategoryParam{
			Category: &Category{ID: sop.NewUUID(), Name: "Cat"},
		}
	}
	_ = kb.UpsertCategories(ctx, upserts)

	// Test category pagination
	cats, _, _ := kb.ListCategories(ctx, ListCategoriesParam{Limit: 2, Offset: 1})
	if len(cats) != 2 {
		t.Errorf("Expected 2 categories from pagination, got %d", len(cats))
	}

	// Create 5 items in the first category
	catID := upserts[0].Category.ID
	itemUpserts := make([]UpsertItemParam[string], 5)
	for i := 0; i < 5; i++ {
		itemUpserts[i] = UpsertItemParam[string]{
			CategoryID: catID,
			Item:       &Item[string]{ID: sop.NewUUID(), CategoryID: catID, Data: "item"},
		}
	}
	_ = kb.UpsertItems(ctx, itemUpserts)

	// Test Item Pagination directly with offset
	items, count, _ := kb.ListItems(ctx, ListItemsParam{Limit: 2, Offset: 2})
	if len(items) != 2 {
		t.Errorf("Expected 2 items from pagination, got %d", len(items))
	}
	if count != 5 {
		t.Errorf("Expected total count 5, got %d", count)
	}

	// Edge Case: Empty ID or missing path Delete
	_ = kb.DeleteCategories(ctx, []sop.UUID{sop.NilUUID})
	_ = kb.DeleteItems(ctx, []ItemKey{{}})

	// Edge Case: SearchByPath with unmapped or empty category
	res, err := kb.SearchByPath(ctx, []PathSearchParam{
		{CategoryPath: "NonExistentPath", SearchText: "Hello"},
	})
	if err != nil {
		t.Errorf("Expected nil error for non existent path, got %v", err)
	}
	if len(res) != 0 {
		t.Errorf("Expected 0 results, got %d", len(res))
	}
}
