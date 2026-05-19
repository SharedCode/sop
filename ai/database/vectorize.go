package database

import (
	"context"
	"encoding/json"
	"log/slog"
	log "log/slog"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/memory"
)

// Vectorize processes a KnowledgeBase in batches, managing its own transactions and cursor state.
func (db *Database) Vectorize(
	ctx context.Context,
	name string,
	llm ai.Generator,
	embedder ai.Embeddings,
	batchSize int,
) error {
	slog.Info("Vectorize started", "kb_name", name, "batchSize", batchSize)

	embedderDim := embedder.Dim()

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return err
	}

	kb, err := db.OpenKnowledgeBase(ctx, name, tx, llm, embedder, false)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	catBtree, err := kb.Store.Categories(ctx)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	itemsBtree, err := kb.Store.Items(ctx)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	ok, err := catBtree.First(ctx)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	type batchTask struct {
		item       *memory.Item[map[string]any]
		catID      sop.UUID
		centerVec  []float32
		summaryCnt int
	}

	batchTasks := make([]batchTask, 0, batchSize)
	var batchSummaries []string

	for ok {
		cat, err := catBtree.GetCurrentValue(ctx)
		if err != nil || cat == nil {
			tx.Rollback(ctx)
			break
		}

		catID := cat.ID
		catCenterVector := cat.CenterVector

		// Vectorize the category itself if outdated
		embedText := cat.Description
		if embedText == "" {
			embedText = cat.Name
		}
		expectedCatHash := memory.ComputeVectorHash(embedderDim, embedText)
		if cat.VectorHash != expectedCatHash || len(cat.CenterVector) == 0 {
			catVecs, err := embedder.EmbedTexts(ctx, []string{embedText})
			if err == nil && len(catVecs) > 0 {
				cat.CenterVector = catVecs[0]
				catCenterVector = cat.CenterVector
				cat.VectorHash = expectedCatHash
				catBtree.UpdateCurrentItem(ctx, cat.ID, cat)
			} else if err != nil {
				tx.Rollback(ctx)
				return err
			}
		}

		// Now process Items for this Category
		totalItemCount := 0
		iok, err := itemsBtree.Find(ctx, memory.ItemKey{CategoryID: catID, ItemID: sop.NilUUID}, false)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
		if !iok {
			// SOP Btree positions to closest item automatically on false. Evaluate it.
			iok = true
		}

		var lastItemInCat sop.UUID = sop.NilUUID

		for iok {
			item, err := itemsBtree.GetCurrentValue(ctx)

			// Stop if cursor hits the end of BTree or leaves the category bounds
			if err != nil || item.CategoryID.Compare(catID) != 0 {
				break
			}

			if item.IsConfig() {
				iok, _ = itemsBtree.Next(ctx)
				continue
			}

			totalItemCount++
			lastItemInCat = item.ID

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

			if item.VectorHash != expectedHash || len(item.Positions) == 0 {
				item.VectorHash = expectedHash
				if len(item.Summaries) == 0 {
					item.Summaries = []string{dataStr}
				}

				itemCopy := item
				batchTasks = append(batchTasks, batchTask{
					item:       &itemCopy,
					catID:      catID,
					centerVec:  catCenterVector,
					summaryCnt: len(itemCopy.Summaries),
				})
				batchSummaries = append(batchSummaries, itemCopy.Summaries...)
			}

			// If batch is full, flush and commit
			if len(batchTasks) >= batchSize {
				if len(batchSummaries) > 0 {
					slog.Info("Vectorize embedding items batch", "kb_name", name, "count", len(batchSummaries))
					allVecs, err := embedder.EmbedTexts(ctx, batchSummaries)
					if err != nil {
						tx.Rollback(ctx)
						return err
					}

					log.Debug("before batch processing via kb.Store,UpsertByCategoryID")
					vecIdx := 0
					for _, task := range batchTasks {
						count := task.summaryCnt
						itemVecs := allVecs[vecIdx : vecIdx+count]
						vecIdx += count

						err = kb.Store.UpsertByCategoryID(ctx, task.catID, task.centerVec, *task.item, itemVecs)
						if err != nil {
							tx.Rollback(ctx)
							return err
						}
					}
					log.Debug("after batch processing via kb.Store,UpsertByCategoryID")
				}

				// Reset batch
				batchTasks = batchTasks[:0]
				batchSummaries = batchSummaries[:0]

				if err := tx.Commit(ctx); err != nil {
					return err
				}

				// Start new transaction
				tx, err = db.BeginTransaction(ctx, sop.ForWriting)
				if err != nil {
					return err
				}
				kb, err = db.OpenKnowledgeBase(ctx, name, tx, llm, embedder, false)
				if err != nil {
					tx.Rollback(ctx)
					return err
				}
				catBtree, err = kb.Store.Categories(ctx)
				if err != nil {
					tx.Rollback(ctx)
					return err
				}
				itemsBtree, err = kb.Store.Items(ctx)
				if err != nil {
					tx.Rollback(ctx)
					return err
				}

				// Restore category cursor
				_, err = catBtree.Find(ctx, catID, false)
				if err != nil {
					tx.Rollback(ctx)
					return err
				}
				// Restore item cursor
				_, err = itemsBtree.Find(ctx, memory.ItemKey{CategoryID: catID, ItemID: lastItemInCat}, false)
				if err != nil {
					tx.Rollback(ctx)
					return err
				}
			}

			iok, _ = itemsBtree.Next(ctx)
		}

		// Category finished processing. Update ItemCount if necessary
		if cat.ItemCount != totalItemCount {
			cat.ItemCount = totalItemCount
			catBtree.UpdateCurrentItem(ctx, cat.ID, cat)
		}

		ok, _ = catBtree.Next(ctx)
	}

	// Flush any remaining batch
	if len(batchTasks) > 0 {
		if len(batchSummaries) > 0 {
			slog.Info("Vectorize embedding items batch (final)", "kb_name", name, "count", len(batchSummaries))
			allVecs, err := embedder.EmbedTexts(ctx, batchSummaries)
			if err != nil {
				tx.Rollback(ctx)
				return err
			}

			vecIdx := 0
			for _, task := range batchTasks {
				count := task.summaryCnt
				itemVecs := allVecs[vecIdx : vecIdx+count]
				vecIdx += count

				err = kb.Store.UpsertByCategoryID(ctx, task.catID, task.centerVec, *task.item, itemVecs)
				if err != nil {
					tx.Rollback(ctx)
					return err
				}
			}
		}
	}

	cfg, _ := kb.GetConfig(ctx)
	if cfg == nil {
		cfg = &memory.KnowledgeBaseConfig{}
	}
	cfg.LastVectorized = time.Now().Unix()
	kb.SetConfig(ctx, cfg)
	return tx.Commit(ctx)
}
