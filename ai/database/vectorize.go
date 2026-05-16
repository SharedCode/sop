package database

import (
	"context"
	"encoding/json"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/memory"
)

// Vectorize processes a KnowledgeBase in batches, managing its own transactions and cursor state.
func Vectorize(
	ctx context.Context,
	db *Database,
	name string,
	llm ai.Generator,
	embedder ai.Embeddings,
	batchSize int,
) error {
	var cursorPos sop.UUID = sop.NilUUID
	var isFirstBatch = true

	for {
		tx, err := db.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return err
		}

		kb, err := db.OpenKnowledgeBase(ctx, name, tx, llm, embedder)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}

		// On the very first transaction batch, vectorize Categories since they are small in number.
		if isFirstBatch {
			if err := vectorizeAllCategories(ctx, kb, embedder); err != nil {
				tx.Rollback(ctx)
				return err
			}
			isFirstBatch = false
		}

		hasMore, newCursor, err := processVectorizeBatch(ctx, kb, embedder, batchSize, cursorPos)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}

		cursorPos = newCursor

		if err := tx.Commit(ctx); err != nil {
			return err
		}

		if !hasMore {
			break
		}
	}

	return nil
}

func vectorizeAllCategories(ctx context.Context, kb *memory.KnowledgeBase[map[string]any], embedder ai.Embeddings) error {
	catBtree, err := kb.Store.Categories(ctx)
	if err != nil {
		return err
	}

	ok, _ := catBtree.First(ctx)
	var allCats []*memory.Category
	for ok {
		cat, err := catBtree.GetCurrentValue(ctx)
		if err == nil {
			// explicitly copy and reference
			catCopy := cat
			allCats = append(allCats, catCopy)
		}
		ok, _ = catBtree.Next(ctx)
	}

	var categoriesToUpdate []*memory.Category
	var catNames []string
	embedderDim := embedder.Dim()

	for _, c := range allCats {
		embedText := c.Description
		if embedText == "" {
			embedText = c.Name
		}
		expectedHash := memory.ComputeVectorHash(embedderDim, embedText)
		if c.VectorHash != expectedHash || len(c.CenterVector) == 0 {
			c.VectorHash = expectedHash
			categoriesToUpdate = append(categoriesToUpdate, c)
			catNames = append(catNames, embedText)
		}
	}

	if len(catNames) > 0 {
		catVecs, err := embedder.EmbedTexts(ctx, catNames)
		if err == nil && len(catVecs) == len(catNames) {
			for i, c := range categoriesToUpdate {
				c.CenterVector = catVecs[i]
				catBtree.Update(ctx, c.ID, c)
			}
		} else if err != nil {
			return err
		}
	}

	return nil
}

func processVectorizeBatch(
	ctx context.Context,
	kb *memory.KnowledgeBase[map[string]any],
	embedder ai.Embeddings,
	batchSize int,
	startCursor sop.UUID,
) (bool, sop.UUID, error) {
	itemsBtree, err := kb.Store.Items(ctx)
	if err != nil {
		return false, startCursor, err
	}
	
	catBtree, err := kb.Store.Categories(ctx)
	if err != nil {
		return false, startCursor, err
	}

	hasMore := false
	processedCount := 0

	ok := false
	if startCursor != sop.NilUUID {
		ok, _ = itemsBtree.Find(ctx, startCursor, false)
	} else {
		ok, _ = itemsBtree.First(ctx)
	}

	currentCursor := startCursor
	var batch []*memory.Item[map[string]any]

	// 1. Gather a batch of items
	for ok {
		if processedCount >= batchSize {
			hasMore = true
			break
		}
		item, err := itemsBtree.GetCurrentValue(ctx)
		if err == nil {
			currentCursor = item.ID
			itemCopy := item
			batch = append(batch, &itemCopy)
			processedCount++
		}
		ok, _ = itemsBtree.Next(ctx)
	}

	if len(batch) == 0 {
		return false, currentCursor, nil
	}

	// 2. Format items that need embeddings
	var itemsNeedingEmbeddings []*memory.Item[map[string]any]
	var batchSummaries []string
	var itemSummaryCounts []int
	embedderDim := embedder.Dim()

	for _, item := range batch {
		dataStr := ""
		if len(item.Summaries) == 0 {
			if str, isStr := any(item.Data).(string); isStr {
				dataStr = str
			} else {
				b, _ := json.Marshal(item.Data)
				dataStr = string(b)
			}
		}

		hashTexts := item.Summaries
		if len(hashTexts) == 0 {
			hashTexts = []string{dataStr}
		}

		expectedHash := memory.ComputeVectorHash(embedderDim, hashTexts...)
		
		// If the vector hash is mismatched or it lacks a vector position, vectorize it.
		// Note: The LLM step might be skipped entirely if we just need to re-embed.
		if item.VectorHash != expectedHash || len(item.Positions) == 0 {
			item.VectorHash = expectedHash
			
			if len(item.Summaries) == 0 {
				item.Summaries = []string{dataStr}
			}
			
			itemsNeedingEmbeddings = append(itemsNeedingEmbeddings, item)
			batchSummaries = append(batchSummaries, item.Summaries...)
			itemSummaryCounts = append(itemSummaryCounts, len(item.Summaries))
		}
	}

	// 3. Generate embeddings
	var allVecs [][]float32
	if len(batchSummaries) > 0 {
		allVecs, err = embedder.EmbedTexts(ctx, batchSummaries)
		if err != nil {
			return false, currentCursor, err
		}
	}

	// 4. Map vectors back and Upsert
	vecIdx := 0
	for j, item := range itemsNeedingEmbeddings {
		count := itemSummaryCounts[j]
		if vecIdx+count > len(allVecs) {
			break
		}
		itemVecs := allVecs[vecIdx : vecIdx+count]
		vecIdx += count

		// Retrieve category name
		catName := "default"
		if found, _ := catBtree.Find(ctx, item.CategoryID, false); found {
			if c, err := catBtree.GetCurrentValue(ctx); err == nil {
				catName = c.Name
			}
		}

		// Re-save logic
		err = kb.Store.UpsertByCategory(ctx, catName, *item, itemVecs)
		if err != nil {
			return false, currentCursor, err
		}
	}

	return hasMore, currentCursor, nil
}
