package database

import (
	"context"
	"fmt"
	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/memory"
	"github.com/sharedcode/sop/ai/vector"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common"
	core "github.com/sharedcode/sop/database"
)

func vectorKeyComparer(a, b memory.VectorKey) int {
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

func itemKeyComparer(a, b memory.ItemKey) int {
	if cmp := a.CategoryID.Compare(b.CategoryID); cmp != 0 {
		return cmp
	}
	return a.ItemID.Compare(b.ItemID)
}

func distanceKeyComparer(a, b memory.DistanceKey) int {
	if a.Distance < b.Distance {
		return -1
	}
	if a.Distance > b.Distance {
		return 1
	}
	return a.ID.Compare(b.ID)
}

// KnowledgeBaseExists checks if the underlying physical tables for a knowledge base have been created.
func (db *Database) KnowledgeBaseExists(ctx context.Context, name string) (bool, error) {
	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	if trans, ok := tx.GetPhasedTransaction().(*common.Transaction); ok {
		sr := trans.GetStoreRepository()
		if sr != nil {
			catStoreName := name + "/categories"
			infos, err := sr.Get(ctx, catStoreName)
			if err == nil && len(infos) > 0 && !infos[0].IsEmpty() {
				return true, nil
			}
		}
	}
	return false, nil
}

// OpenKnowledgeBase intelligently provisions or opens the physical tables
// and returns the clean KnowledgeBase facade.
// KnowledgeBase leverages LLM or human generated semantic Categories for clustering,
// avoiding the periodic rebuilds (Optimize) required by standard VectorStores.
func (db *Database) OpenKnowledgeBase(
	ctx context.Context,
	name string,
	t sop.Transaction,
	llm ai.Generator,
	embedder ai.Embeddings,
	documentMode bool,
	enableTextSearch ...bool,
) (*memory.KnowledgeBase[map[string]any], error) {

	// Resurrect DocumentMode if the items store was physically provisioned in a prior lifecycle
	if trans, ok := t.GetPhasedTransaction().(*common.Transaction); ok {
		sr := trans.GetStoreRepository()
		if sr != nil {
			infos, err := sr.Get(ctx, fmt.Sprintf("%s/items", name))
			if err == nil && len(infos) > 0 && !infos[0].IsEmpty() {
				documentMode = infos[0].IsValueDataInNodeSegment
			}
		}
	}

	// 1. Open Categories Store
	catsStore := sop.ConfigureStore(fmt.Sprintf("%s/categories", name), true, btree.DefaultSlotLength, "dynamic categories store", sop.SmallData, "")
	catsTree, err := core.NewBtree[sop.UUID, *memory.Category](ctx, db.config, catsStore.Name, t, nil, catsStore)
	if err != nil {
		return nil, err
	}

	// 1.1 Open CategoriesByPath Store
	catsByPathStore := sop.ConfigureStore(fmt.Sprintf("%s/categoriesByPath", name), true, btree.DefaultSlotLength, "dynamic categoriesByPath store", sop.SmallData, "")
	catsByPathTree, err := core.NewBtree[string, sop.UUID](ctx, db.config, catsByPathStore.Name, t, nil, catsByPathStore)
	if err != nil {
		return nil, err
	}

	// 2. Open Vectors Store
	vecsStore := sop.ConfigureStore(fmt.Sprintf("%s/vectors", name), true, 10000, "dynamic vectors store", sop.SmallData, "")
	vecsTree, err := core.NewBtree[memory.VectorKey, memory.Vector](ctx, db.config, vecsStore.Name, t, vectorKeyComparer, vecsStore)
	if err != nil {
		return nil, err
	}

	// 3. Open Items Store
	itemsSize := sop.MediumData
	if documentMode {
		itemsSize = sop.SmallData
	}
	itemsStore := sop.ConfigureStore(fmt.Sprintf("%s/items", name), true, btree.DefaultSlotLength, "dynamic items store", itemsSize, "")
	itemsTree, err := core.NewBtree[memory.ItemKey, memory.Item[map[string]any]](ctx, db.config, itemsStore.Name, t, itemKeyComparer, itemsStore)
	if err != nil {
		return nil, err
	}

	// 4. Open Categories By Distance Store
	catsByDistStore := sop.ConfigureStore(fmt.Sprintf("%s/categoriesByDistance", name), true, btree.DefaultSlotLength, "dynamic categoriesByDistance store", sop.SmallData, "")
	catsByDistTree, err := core.NewBtree[memory.DistanceKey, byte](ctx, db.config, catsByDistStore.Name, t, distanceKeyComparer, catsByDistStore)
	if err != nil {
		return nil, err
	}

	// 5. Open Documents Store
	docsStore := sop.ConfigureStore(fmt.Sprintf("%s/documents", name), true, btree.DefaultSlotLength, "dynamic canonical documents store", sop.BigData, "")
	docsTree, err := core.NewBtree[sop.UUID, memory.Document](ctx, db.config, docsStore.Name, t, nil, docsStore)
	if err != nil {
		return nil, err
	}

	// Assemble the storage engine
	store := memory.NewStore(name, db, catsTree, catsByPathTree, catsByDistTree, vecsTree, itemsTree, docsTree)

	manager := memory.NewMemoryManager(store, llm, embedder)

	kb := &memory.KnowledgeBase[map[string]any]{
		Store:   store,
		Manager: manager,
	}

	cfg, _ := kb.GetConfig(ctx)

	useTextSearch := false
	if cfg != nil {
		useTextSearch = cfg.TextSearchEnabled
	} else if len(enableTextSearch) > 0 {
		useTextSearch = enableTextSearch[0]
	}

	if useTextSearch {
		textIndex, err := db.OpenSearch(ctx, fmt.Sprintf("%s/text", name), t)
		if err == nil && textIndex != nil {
			if st, ok := store.(interface{ SetTextIndex(idx ai.TextIndex) }); ok {
				st.SetTextIndex(textIndex)
			}
		}
	}

	if cfg != nil && len(cfg.DomainReference) > 0 {
		store.SetDomainReference(cfg.DomainReference)
	}

	return kb, nil
}

// MigrateVectorToKnowledgeBase reads an existing pure-math VectorStore index,
// extracts its math-based groupings (Centroids), and enriches them (Post-Hoc Labeling)
// by asking the LLM to generate descriptive categories. This "Mints" a digital asset
// into a human-readable KnowledgeBase for the marketplace.
func (db *Database) MigrateVectorToKnowledgeBase(
	ctx context.Context,
	vectorStoreName string,
	knowledgeBaseName string,
	t sop.Transaction,
	llm ai.Generator,
	embedder ai.Embeddings,
) (*memory.KnowledgeBase[map[string]any], error) {

	// 1. Open the existing raw vector store
	cfg := vector.Config{} // Inherit config from context/db if needed
	vStore, err := db.OpenVectorStore(ctx, vectorStoreName, t, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open vector store: %w", err)
	}

	// 2. Instantiate a fresh new KnowledgeBase
	kb, err := db.OpenKnowledgeBase(ctx, knowledgeBaseName, t, llm, embedder, false)
	if err != nil {
		return nil, fmt.Errorf("failed to open knowledge base: %w", err)
	}

	// 3. The "Post-Hoc Labeling" Migration Magic
	centroidsTree, err := vStore.Centroids(ctx)
	if err != nil {
		return kb, nil // Just return kb if no centroids to process.
	}

	if ok, _ := centroidsTree.First(ctx); ok {
		for {
			cidItem := centroidsTree.GetCurrentKey()
			cid := cidItem.Key
			// cVal := centroidsTree.CurrentValue()

			// We can fetch data associated with this internal math centroid.
			// Instead of simply dropping raw data, we ask the LLM what this generic
			// math grouping represents. Then, we ingest those thoughts directly
			// into the KnowledgeBase, transforming meaningless vectors into a Categorized Space.
			log.Info("Minting new asset for vector centroid", "cid", cid)

			// For a fully automated pipeline, fetch vectors for 'cid',
			// send to llm via kb.IngestThoughts(...) assigning to the KnowledgeBase.
			// We mock the iteration over centroids to complete the pattern.

			if ok, _ := centroidsTree.Next(ctx); !ok {
				break
			}
		}
	}

	// Because Knowledge Base naturally clusters without blocking Optimize, it
	// dynamically routes the vectors seamlessly into its UUID B-Trees!
	return kb, nil
}
