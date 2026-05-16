package database

import (
	"context"
	"encoding/json"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/memory"
)

// VectorizeItems processes specific items or an entire category within a single isolated transaction.
// It is intended for real-time web-handlers and granular hooks, avoiding the full Space scan.
func VectorizeItems(
	ctx context.Context,
	db *Database,
	name string,
	llm ai.Generator,
	embedder ai.Embeddings,
	batchSize int,
	categoryID sop.UUID,
	itemIDs []sop.UUID,
) error {
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	kb, err := db.OpenKnowledgeBase(ctx, name, tx, llm, embedder)
	if err != nil {
		return err
	}

	itemsBtree, err := kb.Store.Items(ctx)
	if err != nil {
		return err
	}

	catBtree, err := kb.Store.Categories(ctx)
	if err != nil {
		return err
	}

	var category *memory.Category
	if ok, _ := catBtree.Find(ctx, categoryID, false); ok {
		cat, _ := catBtree.GetCurrentValue(ctx)
		category = cat
	}
	if category == nil || category.Name == "" {
		return nil
	}

	embedderDim := embedder.Dim()
	catUpdated := false

	embedText := category.Description
	if embedText == "" {
		embedText = category.Name
	}

	expectedCatHash := memory.ComputeVectorHash(embedderDim, embedText)
	if category.VectorHash != expectedCatHash || len(category.CenterVector) == 0 {
		catVecs, _ := embedder.EmbedTexts(ctx, []string{embedText})
		if len(catVecs) > 0 {
			category.CenterVector = catVecs[0]
			category.VectorHash = expectedCatHash
			catBtree.Update(ctx, category.ID, category)
			catUpdated = true
		}
		itemIDs = nil
	}

	var itemsToUpdate []*memory.Item[map[string]any]
	var catItemsNoLLM []*memory.Item[map[string]any]

	if len(itemIDs) == 0 {
		ok, _ := itemsBtree.First(ctx)
		for ok {
			item, err := itemsBtree.GetCurrentValue(ctx)
			if err == nil && item.CategoryID == categoryID {
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

				itemCopy := item
				if item.VectorHash != expectedHash || len(item.Positions) == 0 {
					itemCopy.VectorHash = expectedHash
					itemsToUpdate = append(itemsToUpdate, &itemCopy)
				} else if catUpdated {
					catItemsNoLLM = append(catItemsNoLLM, &itemCopy)
				}
			}
			ok, _ = itemsBtree.Next(ctx)
		}
	} else {
		for _, id := range itemIDs {
			if ok, _ := itemsBtree.Find(ctx, id, false); ok {
				item, err := itemsBtree.GetCurrentValue(ctx)
				if err == nil && item.CategoryID == categoryID {
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

					itemCopy := item
					if item.VectorHash != expectedHash || len(item.Positions) == 0 {
						itemCopy.VectorHash = expectedHash
						itemsToUpdate = append(itemsToUpdate, &itemCopy)
					} else if catUpdated {
						catItemsNoLLM = append(catItemsNoLLM, &itemCopy)
					}
				}
			}
		}
	}

	for i := 0; i < len(itemsToUpdate); i += batchSize {
		end := i + batchSize
		if end > len(itemsToUpdate) {
			end = len(itemsToUpdate)
		}

		batch := itemsToUpdate[i:end]
		var batchSummaries []string
		var itemSummaryCounts []int

		for _, item := range batch {
			if len(item.Summaries) == 0 {
				dataStr := ""
				if str, isStr := any(item.Data).(string); isStr {
					dataStr = str
				} else {
					b, _ := json.Marshal(item.Data)
					dataStr = string(b)
				}
				item.Summaries = []string{dataStr}
			}
			batchSummaries = append(batchSummaries, item.Summaries...)
			itemSummaryCounts = append(itemSummaryCounts, len(item.Summaries))
		}

		if len(batchSummaries) == 0 {
			continue
		}

		allVecs, err := embedder.EmbedTexts(ctx, batchSummaries)
		if err != nil {
			return err
		}

		vecIdx := 0
		for j, item := range batch {
			count := itemSummaryCounts[j]
			itemVecs := allVecs[vecIdx : vecIdx+count]
			vecIdx += count

			err = kb.Store.UpsertByCategory(ctx, category.Name, *item, itemVecs)
			if err != nil {
				return err
			}
		}
	}

	vectorsBtree, _ := kb.Store.Vectors(ctx)
	if vectorsBtree != nil {
		for _, item := range catItemsNoLLM {
			var itemVecs [][]float32
			for _, pos := range item.Positions {
				if found, _ := vectorsBtree.Find(ctx, pos, false); found {
					if v, err := vectorsBtree.GetCurrentValue(ctx); err == nil {
						itemVecs = append(itemVecs, v.Data)
					}
				}
			}
			if len(itemVecs) > 0 {
				err = kb.Store.UpsertByCategory(ctx, category.Name, *item, itemVecs)
				if err != nil {
					return err
				}
			}
		}
	}

	return tx.Commit(ctx)
}
