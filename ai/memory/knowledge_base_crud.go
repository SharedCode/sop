package memory

import (
	"context"
	"strings"

	"github.com/sharedcode/sop"
)

// ============================================================================
// V1 Space CRUD & Omni Batched API Parameters
// ============================================================================

type UpsertCategoryParam struct {
	ParentPaths []string   `json:"parent_paths"` // Declarative DAG routing (e.g. ["Root/Engineering", "Root/DevOps"])
	ParentIDs   []sop.UUID `json:"parent_ids"`   // Explicit DAG routing fallback
	Category    *Category  `json:"category"`     // Pointer to the Category to upsert
}

type UpsertItemParam[T any] struct {
	CategoryPath string      `json:"category_path"` // e.g. "Root/Engineering/Architecture"
	CategoryID   sop.UUID    `json:"category_id"`   // Direct ID fallback if path is empty
	Item         *Item[T]    `json:"item"`          // Pointer to avoid heavy allocation during batch
	Vectors      [][]float32 `json:"vectors"`       // Optional explicit embeddings
}

type PathSearchParam struct {
	CategoryPath string `json:"category_path"` // e.g. "Root/Engineering/Architecture"
	SearchText   string `json:"search_text"`   // Text to prefix search on item content/title
}

type ListCategoriesParam struct {
	ParentPath string   `json:"parent_path"` // Optional: restrict list to a specific parent node
	ParentID   sop.UUID `json:"parent_id"`   // Optional: explicit restriction by ID
	Limit      int      `json:"limit"`
	Offset     int      `json:"offset"`
}

type ListItemsParam struct {
	CategoryPath string   `json:"category_path"` // Optional: restrict list to a specific category
	CategoryID   sop.UUID `json:"category_id"`   // Optional: restrict list by explicit ID
	Limit        int      `json:"limit"`
	Offset       int      `json:"offset"`
}

// ============================================================================
// V1 Space CRUD & Omni Batched API (Implementations)
// ============================================================================

func (kb *KnowledgeBase[T]) UpsertCategories(ctx context.Context, params []UpsertCategoryParam) error {
	catBtree, err := kb.Store.Categories(ctx)
	if err != nil {
		return err
	}
	catsByPath, err := kb.Store.CategoriesByPath(ctx)
	if err != nil {
		return err
	}

	for _, p := range params {
		if p.Category == nil {
			continue
		}

		for _, path := range p.ParentPaths {
			found, _ := catsByPath.Find(ctx, path, false)
			if found {
				id, _ := catsByPath.GetCurrentValue(ctx)
				p.ParentIDs = append(p.ParentIDs, id)
			}
		}

		parentMap := make(map[sop.UUID]bool)
		var finalParents []CategoryParent
		for _, pid := range p.ParentIDs {
			if !parentMap[pid] {
				parentMap[pid] = true
				finalParents = append(finalParents, CategoryParent{ParentID: pid})
			}
		}
		p.Category.ParentIDs = finalParents

		if p.Category.ID.IsNil() {
			p.Category.ID = sop.NewUUID()
		}

		found, _ := catBtree.Find(ctx, p.Category.ID, false)
		if found {
			_, _ = catBtree.Update(ctx, p.Category.ID, p.Category)
		} else {
			_, _ = catBtree.Add(ctx, p.Category.ID, p.Category)
		}

		path := p.Category.Path
		if path == "" {
			path = p.Category.Name
		}
		if path != "" {
			foundPath, _ := catsByPath.Find(ctx, path, false)
			if foundPath {
				_, _ = catsByPath.Update(ctx, path, p.Category.ID)
			} else {
				_, _ = catsByPath.Add(ctx, path, p.Category.ID)
			}
		}
	}
	return nil
}

func (kb *KnowledgeBase[T]) DeleteCategories(ctx context.Context, categoryIDs []sop.UUID) error {
	tree, err := kb.Store.Categories(ctx)
	if err != nil {
		return err
	}
	for _, id := range categoryIDs {
		_, _ = tree.Remove(ctx, id)
	}
	return nil
}

func (kb *KnowledgeBase[T]) ListCategories(ctx context.Context, param ListCategoriesParam) ([]Category, int, error) {
	catsTree, err := kb.Store.Categories(ctx)
	if err != nil {
		return nil, 0, err
	}

	var parentFilter sop.UUID
	if !param.ParentID.IsNil() {
		parentFilter = param.ParentID
	} else if param.ParentPath != "" && param.ParentPath != "/" {
		catsByPath, err := kb.Store.CategoriesByPath(ctx)
		if err == nil {
			if found, _ := catsByPath.Find(ctx, param.ParentPath, false); found {
				parentFilter, _ = catsByPath.GetCurrentValue(ctx)
			}
		}
	}

	var categories []Category
	matchCount := 0
	ok, _ := catsTree.First(ctx)

	for ok {
		cat, _ := catsTree.GetCurrentValue(ctx)
		if cat != nil {
			isMatch := false
			if param.ParentPath == "/" {
				if len(cat.ParentIDs) == 0 {
					isMatch = true
				}
			} else if !parentFilter.IsNil() {
				for _, p := range cat.ParentIDs {
					if p.ParentID == parentFilter {
						isMatch = true
						break
					}
				}
			} else if param.ParentPath == "" && param.ParentID.IsNil() {
				isMatch = true
			}
			if isMatch {
				if matchCount >= param.Offset && matchCount < param.Offset+param.Limit {
					categories = append(categories, *cat)
				}
				matchCount++
			}
		}
		ok, _ = catsTree.Next(ctx)
	}
	if categories == nil {
		categories = make([]Category, 0)
	}
	return categories, matchCount, nil
}

func (kb *KnowledgeBase[T]) UpsertItems(ctx context.Context, params []UpsertItemParam[T]) error {
	itemsTree, err := kb.Store.Items(ctx)
	if err != nil {
		return err
	}
	catsByPath, err := kb.Store.CategoriesByPath(ctx)
	if err != nil {
		return err
	}

	for _, p := range params {
		if p.Item == nil {
			continue
		}

		catID := p.CategoryID
		if p.CategoryPath != "" {
			if found, _ := catsByPath.Find(ctx, p.CategoryPath, false); found {
				catID, _ = catsByPath.GetCurrentValue(ctx)
			}
		}

		if catID.IsNil() {
			continue
		}
		if p.Item.ID.IsNil() {
			p.Item.ID = sop.NewUUID()
		}

		key := ItemKey{CategoryID: catID, ItemID: p.Item.ID}
		found, _ := itemsTree.Find(ctx, key, false)
		if found {
			_, _ = itemsTree.Update(ctx, key, *p.Item)
		} else {
			_, _ = itemsTree.Add(ctx, key, *p.Item)
		}
	}
	return nil
}

func (kb *KnowledgeBase[T]) DeleteItems(ctx context.Context, itemKeys []ItemKey) error {
	tree, err := kb.Store.Items(ctx)
	if err != nil {
		return err
	}
	for _, key := range itemKeys {
		_, _ = tree.Remove(ctx, key)
	}
	return nil
}

func (kb *KnowledgeBase[T]) ListItems(ctx context.Context, param ListItemsParam) ([]Item[T], int, error) {
	itemsTree, err := kb.Store.Items(ctx)
	if err != nil {
		return nil, 0, err
	}

	var categoryFilter sop.UUID
	if !param.CategoryID.IsNil() {
		categoryFilter = param.CategoryID
	} else if param.CategoryPath != "" {
		catsByPath, err := kb.Store.CategoriesByPath(ctx)
		if err == nil {
			if found, _ := catsByPath.Find(ctx, param.CategoryPath, false); found {
				categoryFilter, _ = catsByPath.GetCurrentValue(ctx)
			}
		}
	}

	var items []Item[T]
	matchCount := 0
	totalCount := 0

	c, _ := kb.Store.Count(ctx)
	totalCount = int(c)

	ok, _ := itemsTree.First(ctx)

	if !categoryFilter.IsNil() {
		ok, _ = itemsTree.Find(ctx, ItemKey{CategoryID: categoryFilter, ItemID: sop.NilUUID}, true)
		if !ok {
			currKey := itemsTree.GetCurrentKey()
			if !currKey.ID.IsNil() {
				if currKey.Key.CategoryID == categoryFilter {
					ok = true
				} else if currKey.Key.CategoryID.Compare(categoryFilter) < 0 {
					ok, _ = itemsTree.Next(ctx)
				}
			}
		}
	}

	for ok {
		itemReq := itemsTree.GetCurrentKey()
		if itemReq.ID.IsNil() {
			break
		}
		if !categoryFilter.IsNil() && itemReq.Key.CategoryID != categoryFilter {
			break
		}
		if categoryFilter.IsNil() || itemReq.Key.CategoryID == categoryFilter {
			if matchCount >= param.Offset && matchCount < param.Offset+param.Limit {
				item, _ := itemsTree.GetCurrentValue(ctx)
				items = append(items, item)
			}
			matchCount++
			if categoryFilter.IsNil() && matchCount >= param.Offset+param.Limit {
				break
			}
		}
		ok, _ = itemsTree.Next(ctx)
	}

	if items == nil {
		items = make([]Item[T], 0)
	}
	if !categoryFilter.IsNil() {
		totalCount = matchCount
	}
	return items, totalCount, nil
}

func (kb *KnowledgeBase[T]) SearchByPath(ctx context.Context, params []PathSearchParam) ([]Item[T], error) {
	itemsTree, err := kb.Store.Items(ctx)
	if err != nil {
		return nil, err
	}
	catsByPathTree, err := kb.Store.CategoriesByPath(ctx)
	if err != nil {
		return nil, err
	}

	var results []Item[T]

	for _, param := range params {
		found, err := catsByPathTree.Find(ctx, param.CategoryPath, false)
		if err != nil {
			return nil, err
		}
		if !found {
			continue // Skip if category path is not found
		}

		catID, err := catsByPathTree.GetCurrentValue(ctx)
		if err != nil {
			return nil, err
		}

		ok, _ := itemsTree.Find(ctx, ItemKey{CategoryID: catID, ItemID: sop.NilUUID}, true)
		if !ok {
			currKey := itemsTree.GetCurrentKey()
			if !currKey.Key.ItemID.IsNil() {
				if currKey.Key.CategoryID == catID {
					ok = true
				} else if currKey.Key.CategoryID.Compare(catID) < 0 {
					ok, _ = itemsTree.Next(ctx)
				}
			}
		}

		for ok {
			itemReq := itemsTree.GetCurrentKey()
			if itemReq.Key.ItemID.IsNil() {
				break
			}
			if itemReq.Key.CategoryID != catID {
				break
			}

			item, err := itemsTree.GetCurrentValue(ctx)
			if err == nil && len(item.Summaries) > 0 && strings.HasPrefix(item.Summaries[0], param.SearchText) {
				results = append(results, item)
			}

			ok, _ = itemsTree.Next(ctx)
		}
	}

	if results == nil {
		results = make([]Item[T], 0)
	}

	return results, nil
}
