package main

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
)

func main() {
	ctx := context.Background()
	kb, err := database.NewKnowledgeBase(ctx, "demo-cli-kb", sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{"./data/demo-cli-kb"},
	}, nil, nil, false)
	if err != nil {
		panic(err)
	}
	defer func() {
		if cerr := kb.Close(ctx); cerr != nil {
			panic(cerr)
		}
	}()

	fmt.Println("1) Create a nested category tree")
	if err := kb.UpsertCategories(ctx, []memory.UpsertCategoryParam{{
		Category: &memory.Category{ID: sop.NewUUID(), Name: "Root", Path: "Root", Description: "Top-level space"},
	}}); err != nil {
		panic(err)
	}
	if err := kb.UpsertCategories(ctx, []memory.UpsertCategoryParam{{
		Category:    &memory.Category{ID: sop.NewUUID(), Name: "Engineering", Path: "Root/Engineering", Description: "Engineering topics"},
		ParentPaths: []string{"Root"},
	}}); err != nil {
		panic(err)
	}
	if err := kb.UpsertCategories(ctx, []memory.UpsertCategoryParam{{
		Category:    &memory.Category{ID: sop.NewUUID(), Name: "Memory", Path: "Root/Engineering/Memory", Description: "Memory subsystem"},
		ParentPaths: []string{"Root/Engineering"},
	}}); err != nil {
		panic(err)
	}

	fmt.Println("2) Add items to the leaf category")
	items := []memory.UpsertItemParam[map[string]any]{{
		CategoryPath: "Root/Engineering/Memory",
		Item: &memory.Item[map[string]any]{
			Data:      map[string]any{"text": "Use KnowledgeBase for semantic memory"},
			Summaries: []string{"semantic memory"},
		},
	}, {
		CategoryPath: "Root/Engineering/Memory",
		Item: &memory.Item[map[string]any]{
			Data:      map[string]any{"text": "Store nested categories as a path-based taxonomy"},
			Summaries: []string{"nested categories"},
		},
	}}
	if err := kb.UpsertItems(ctx, items); err != nil {
		panic(err)
	}

	fmt.Println("3) List categories under the nested parent")
	categories, _, err := kb.ListCategories(ctx, memory.ListCategoriesParam{ParentPath: "Root/Engineering", Limit: 20, Offset: 0})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Matched categories (%d):\n", len(categories))
	for _, cat := range categories {
		fmt.Printf("- %s (%s)\n", cat.Name, cat.Path)
	}

	fmt.Println("4) Search items by optional query")
	results, err := kb.SearchByPath(ctx, []memory.PathSearchParam{{CategoryPath: "Root/Engineering/Memory", SearchText: "semantic"}})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Search result sets: %d\n", len(results))
	for i, hit := range results {
		fmt.Printf("Result %d:\n", i)
		fmt.Printf("- Category path: Root/Engineering/Memory\n")
		fmt.Printf("- Matched item: %v\n", hit.Data)
	}
}
