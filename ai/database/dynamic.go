package database

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/dynamic"
	"github.com/sharedcode/sop/btree"
	core "github.com/sharedcode/sop/database"
)

func uuidComparer(a, b sop.UUID) int {
	return a.Compare(b)
}

func vectorKeyComparer(a, b dynamic.VectorKey) int {
	if cmp := a.CategoryID.Compare(b.CategoryID); cmp != 0 {
		return cmp
	}
	if a.DistanceToCategory < b.DistanceToCategory {
		return -1
	}
	if a.DistanceToCategory > b.DistanceToCategory {
		return 1
	}
	return a.VectorID.Compare(b.VectorID)
}

// OpenDynamicStore intelligently provisions or opens the physical tables
// and returns the clean KnowledgeBase facade.
func (db *Database) OpenDynamicStore(
	ctx context.Context,
	name string,
	t sop.Transaction,
	llm ai.Generator,
	embedder ai.Embeddings,
) (*dynamic.KnowledgeBase[map[string]any], error) {

	// 1. Open Categories Store
	catsStore := sop.ConfigureStore(fmt.Sprintf("%s_categories", name), true, btree.DefaultSlotLength, "dynamic categories store", sop.SmallData, "")
	catsTree, err := core.NewBtree[sop.UUID, *dynamic.Category](ctx, db.config, catsStore.Name, t, uuidComparer, catsStore)
	if err != nil {
		if err.Error() == fmt.Sprintf("b-tree '%s' is already in the transaction's b-tree instances list", catsStore.Name) {
			catsTree, err = core.OpenBtree[sop.UUID, *dynamic.Category](ctx, db.config, catsStore.Name, t, uuidComparer)
		}
		if err != nil {
			return nil, err
		}
	}

	// 2. Open Vectors Store
	vecsStore := sop.ConfigureStore(fmt.Sprintf("%s_vectors", name), true, 10000, "dynamic vectors store", sop.SmallData, "")
	vecsTree, err := core.NewBtree[dynamic.VectorKey, dynamic.Vector](ctx, db.config, vecsStore.Name, t, vectorKeyComparer, vecsStore)
	if err != nil {
		if err.Error() == fmt.Sprintf("b-tree '%s' is already in the transaction's b-tree instances list", vecsStore.Name) {
			vecsTree, err = core.OpenBtree[dynamic.VectorKey, dynamic.Vector](ctx, db.config, vecsStore.Name, t, vectorKeyComparer)
		}
		if err != nil {
			return nil, err
		}
	}

	// 3. Open Items Store
	itemsStore := sop.ConfigureStore(fmt.Sprintf("%s_items", name), true, btree.DefaultSlotLength, "dynamic items store", sop.SmallData, "")
	itemsTree, err := core.NewBtree[sop.UUID, dynamic.Item[map[string]any]](ctx, db.config, itemsStore.Name, t, uuidComparer, itemsStore)
	if err != nil {
		if err.Error() == fmt.Sprintf("b-tree '%s' is already in the transaction's b-tree instances list", itemsStore.Name) {
			itemsTree, err = core.OpenBtree[sop.UUID, dynamic.Item[map[string]any]](ctx, db.config, itemsStore.Name, t, uuidComparer)
		}
		if err != nil {
			return nil, err
		}
	}

	// Assemble the storage engine
	store := dynamic.NewStore[map[string]any](catsTree, vecsTree, itemsTree)

	textIndex, err := db.OpenSearch(ctx, fmt.Sprintf("%s_text", name), t)
	if err == nil && textIndex != nil {
		if st, ok := store.(interface{ SetTextIndex(idx ai.TextIndex) }); ok {
			st.SetTextIndex(textIndex)
		}
	}

	manager := dynamic.NewMemoryManager[map[string]any](store, llm, embedder)

	return &dynamic.KnowledgeBase[map[string]any]{
		BaseKnowledgeBase: dynamic.BaseKnowledgeBase[map[string]any]{Store: store},
		Manager:           manager,
	}, nil
}
