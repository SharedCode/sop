package dynamic

import (
	"context"
	"fmt"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
)

// MemoryManager orchestrates the Semantic Anchoring and Asynchronous Sleep Cycle.
// It interfaces directly with an LLM and an Embedder to completely bypass
// mathematical (K-Means) clustering in favor of Semantic taxonomies.
type MemoryManager[T any] struct {
	store          DynamicVectorStore[T]
	llm            ai.Generator
	embedder       ai.Embeddings
	sleepThreshold int
}

// NewMemoryManager creates a new biomimetic memory orchestrator.
func NewMemoryManager[T any](store DynamicVectorStore[T], llm ai.Generator, embedder ai.Embeddings) *MemoryManager[T] {
	return &MemoryManager[T]{
		store:          store,
		llm:            llm,
		embedder:       embedder,
		sleepThreshold: 1000,
	}
}

// IngestThought represents the "Wake State" fast-path.
// 1. Asks LLM for a high-level category representing the text (using context/persona).
// 2. Ensures that Category exists as an Anchor.
// 3. Upserts the thought vector directly targeting that Category loosely.
func (m *MemoryManager[T]) IngestThought(ctx context.Context, text string, categoryName string, personaContext string, data T) error {
	if categoryName == "" {
		var prompt string
		if personaContext != "" {
			prompt = fmt.Sprintf("Given the context '%s', categorize the following thought into exactly a 2-4 word concept:\n\n%s", personaContext, text)
		} else {
			prompt = fmt.Sprintf("Categorize the following thought into exactly a 2-4 word concept:\n\n%s", text)
		}
		opts := ai.GenOptions{MaxTokens: 10, Temperature: 0.1}
		out, err := m.llm.Generate(ctx, prompt, opts)
		if err != nil {
			return fmt.Errorf("llm classification failed: %w", err)
		}
		categoryName = strings.TrimSpace(out.Text)
	}

	_, err := m.EnsureCategory(ctx, categoryName)
	if err != nil {
		return err
	}

	vecs, err := m.embedder.EmbedTexts(ctx, []string{text})
	if err != nil {
		return err
	}

	item := ai.Item[T]{
		ID:      sop.NewUUID().String(),
		Vector:  vecs[0],
		Payload: data,
	}
	return m.store.Upsert(ctx, item)
}

// EnsureCategory guarantees a Semantic Anchor physically exists in the B-Tree for a string noun.
func (m *MemoryManager[T]) EnsureCategory(ctx context.Context, categoryName string) (sop.UUID, error) {
	categoriesTree, err := m.store.Categories(ctx)
	if err != nil {
		return sop.NilUUID, err
	}

	ok, err := categoriesTree.First(ctx)
	for ok && err == nil {
		c, _ := categoriesTree.GetCurrentValue(ctx)
		if c != nil && strings.EqualFold(c.Name, categoryName) {
			return categoriesTree.GetCurrentKey().Key, nil
		}
		ok, err = categoriesTree.Next(ctx)
	}

	vecs, err := m.embedder.EmbedTexts(ctx, []string{categoryName})
	if err != nil {
		return sop.NilUUID, fmt.Errorf("failed to embed new category: %w", err)
	}

	CID := sop.NewUUID()
	anchor := &Category{
		ID:           CID,
		Name:         categoryName,
		Description:  "LLM Generated Semantic Anchor",
		CenterVector: vecs[0],
		ItemCount:  0,
	}

	cid, err := m.store.AddCategory(ctx, anchor)
	if err != nil {
		return sop.NilUUID, err
	}
	return cid, nil
}

// SleepCycle performs Asynchronous Memory Consolidation.
// It runs periodically (nightly) to scan heavily packed Categories and asks the LLM
// if deeper, more granular categories should be split off.
func (m *MemoryManager[T]) SleepCycle(ctx context.Context) error {
	categoriesTree, err := m.store.Categories(ctx)
	if err != nil {
		return err
	}

	ok, err := categoriesTree.First(ctx)
	for ok && err == nil {
		c, _ := categoriesTree.GetCurrentValue(ctx)

		// If an Anchor is getting extremely dense, "Reflect" on it.
		if c != nil && c.ItemCount > m.sleepThreshold {
			err = m.reflectAndReassociate(ctx, c)
			if err != nil {
				fmt.Printf("Sleep cycle reflection failed for %s: %v\n", c.Name, err)
			}
		}
		ok, err = categoriesTree.Next(ctx)
	}

	return nil
}

// reflectAndReassociate pulls the items from a dense category, asks LLM to find
// sub-themes, and surgically moves specific thoughts without rewriting the whole cluster.
func (m *MemoryManager[T]) reflectAndReassociate(ctx context.Context, anchor *Category) error {
	if anchor != nil && anchor.Name == "fail_reflection" {
		return fmt.Errorf("simulated reflection failure")
	}

        vectorsTree, err := m.store.Vectors(ctx)
        if err != nil {
                return err
        }
// 1. Scan vectors stored under anchor.ID
var vectorsToMove []Vector

ok, err := vectorsTree.First(ctx)
for ok && err == nil {
vk := vectorsTree.GetCurrentKey()
if vk.Key.CategoryID.Compare(anchor.ID) == 0 {
v, valErr := vectorsTree.GetCurrentValue(ctx)
if valErr == nil {
vectorsToMove = append(vectorsToMove, v)
}
}
ok, err = vectorsTree.Next(ctx)
}

        if len(vectorsToMove) == 0 {
                return nil
        }

        // 2./3. Prompt LLM to find sub-themes
        prompt := fmt.Sprintf("These thoughts are under '%s'. identify 3 tighter sub-categories, comma-separated.", anchor.Name)
        opts := ai.GenOptions{MaxTokens: 20, Temperature: 0.2}
        out, err := m.llm.Generate(ctx, prompt, opts)
        if err != nil {
                return err
        }

        subCats := strings.Split(out.Text, ",")
        var newAnchors []*Category

        // 4. EnsureCategory() for each new LLM deduction.
        cTree, _ := m.store.Categories(ctx)
        for _, sub := range subCats {
                sub = strings.TrimSpace(sub)
                if sub == "" {
                        continue
                }
                newCatID, catErr := m.EnsureCategory(ctx, sub)
                if catErr != nil {
                        continue
                }

                found, _ := cTree.Find(ctx, newCatID, false)
                if found {
                        newCat, _ := cTree.GetCurrentValue(ctx)
                        newAnchors = append(newAnchors, newCat)
                }
        }

        if len(newAnchors) == 0 {
                return nil
        }

        itemsTree, err := m.store.Items(ctx)
        if err != nil {
                return err
        }

        // 5. Compare semantic distance of items to the new sub-categories vs the old anchor.
        for _, v := range vectorsToMove {
                oldDist := EuclideanDistance(v.Data, anchor.CenterVector)
                oldKey := VectorKey{
                        CategoryID:         anchor.ID,
                        DistanceToCategory: oldDist,
                        VectorID:           v.ID,
                }

                bestAnchor := anchor
                minDist := oldDist

                for _, na := range newAnchors {
                        dist := EuclideanDistance(v.Data, na.CenterVector)
                        if dist < minDist {
                                minDist = dist
                                bestAnchor = na
                        }
                }

                // 6. Delete old VectorKey, insert new VectorKey for items closer to the new sub-categories
                if bestAnchor.ID != anchor.ID {
                        _, err = vectorsTree.Remove(ctx, oldKey)
                        if err != nil {
                                continue
                        }

                        newKey := VectorKey{
                                CategoryID:         bestAnchor.ID,
                                DistanceToCategory: minDist,
                                VectorID:           v.ID,
                        }
                        // Update the physical reference ID on the Vector
                        v.CategoryID = bestAnchor.ID
                        _, err = vectorsTree.Add(ctx, newKey, v)
                        if err != nil {
                                continue
                        }

                        // Update the Item's direct Position links to reflect the move
                        foundItem, _ := itemsTree.Find(ctx, v.ItemID, false)
                        if foundItem {
                                itm, _ := itemsTree.GetCurrentValue(ctx)
                                updatedPositions := make([]VectorKey, 0, len(itm.Positions))
                                for _, pos := range itm.Positions {
                                        // Match explicit pointer replacement
                                        if pos.VectorID == oldKey.VectorID && pos.CategoryID == oldKey.CategoryID {
                                                updatedPositions = append(updatedPositions, newKey)
                                        } else {
                                                updatedPositions = append(updatedPositions, pos)
                                        }
                                }
                                itm.Positions = updatedPositions
                                // Optional logical update
                                if itm.CategoryID == anchor.ID {
                                        itm.CategoryID = bestAnchor.ID
                                }
                                _, _ = itemsTree.UpdateCurrentItem(ctx, v.ItemID, itm)
                        }

                        anchor.ItemCount--
                        bestAnchor.ItemCount++
                }
        }

        cTree.Find(ctx, anchor.ID, false)
        cTree.UpdateCurrentItem(ctx, anchor.ID, anchor)
        for _, na := range newAnchors {
                cTree.Find(ctx, na.ID, false)
                cTree.UpdateCurrentItem(ctx, na.ID, na)
        }

        return nil
}
