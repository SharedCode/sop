package memory

import (
	"context"
	"encoding/json"
	"errors"

	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
)

// KnowledgeBase provides a clean, unified API for developers.
// It orchestrates both the storage tables and the LLM memory management.
type KnowledgeBase[T any] struct {
	Store   MemoryStore[T]
	Manager *MemoryManager[T]
	// MaxMathCategoryDistance specifies the max Euclidean distance to cluster centroids
	// to avoid calling the LLM for category categorization. Set to 0.0 or less to disable
	// and always rely on "pristine" LLM categorization.
	MaxMathCategoryDistance float32
}

func (kb *KnowledgeBase[T]) SearchSemanticsBatch(ctx context.Context, queryVectors [][]float32, opts *SearchOptions[T]) ([][]ai.Hit[T], error) {
	return kb.Store.QueryBatch(ctx, queryVectors, opts)
}

// SearchKeywordsBatch executes textual BM25 text-matched sparse searches for multiple text payloads.
func (kb *KnowledgeBase[T]) SearchKeywordsBatch(ctx context.Context, textQueries []string, opts *SearchOptions[T]) ([][]ai.Hit[T], error) {
	return kb.Store.QueryTextBatch(ctx, textQueries, opts)
}

// SearchSemantics executes a spatial search (vector matching against mathematical bounds).
func (kb *KnowledgeBase[T]) SearchSemantics(ctx context.Context, queryVector []float32, opts *SearchOptions[T]) ([]ai.Hit[T], error) {
	return kb.Store.Query(ctx, queryVector, opts)
}

// SearchKeywords executes a traditional textual BM25 text-matched sparse search.
func (kb *KnowledgeBase[T]) SearchKeywords(ctx context.Context, textQuery string, opts *SearchOptions[T]) ([]ai.Hit[T], error) {
	return kb.Store.QueryText(ctx, textQuery, opts)
}

// Thought represents the individual entity of data in a batch categorization execution.
type Thought[T any] struct {
	Summaries []string
	Category  string
	Data      T
	Vectors   [][]float32
}

// IngestThoughts securely categorizes and stores an array of thoughts, optimizing latency
// by clustering queries and sending a batch request to the LLM generator.
func (kb *KnowledgeBase[T]) IngestThoughts(ctx context.Context, thoughts []Thought[T], persona string) error {
	if len(thoughts) == 0 {
		return nil
	}

	var textsToEmbed []string
	type embedJob struct {
		thoughtIdx int
		summaryIdx int
	}
	var jobs []embedJob

	// 0. Resolve missing summaries via LLM LLM Enrichment
	for i, thought := range thoughts {
		if len(thought.Summaries) > 0 {
			continue
		}

		dataStr := ""
		if str, ok := any(thought.Data).(string); ok {
			dataStr = str
		} else {
			b, _ := json.Marshal(thought.Data)
			dataStr = string(b)
		}

		gen, err := kb.Manager.GenerateSummaries(ctx, dataStr)
		if err == nil && len(gen) > 0 {
			thoughts[i].Summaries = gen
		} else {
			thoughts[i].Summaries = []string{dataStr}
		}
	}

	// 1. Resolve missing vectors
	for i, thought := range thoughts {
		if len(thought.Vectors) == len(thought.Summaries) {
			continue
		}
		// If length mismatch, re-embed all summaries for this thought
		thoughts[i].Vectors = make([][]float32, len(thought.Summaries))
		for j, summary := range thought.Summaries {
			textsToEmbed = append(textsToEmbed, summary)
			jobs = append(jobs, embedJob{thoughtIdx: i, summaryIdx: j})
		}
	}

	if len(textsToEmbed) > 0 {
		if kb.Manager.embedder == nil {
			return errors.New("embedder is nil: cannot vectorize thoughts")
		}
		vecs, err := kb.Manager.embedder.EmbedTexts(ctx, textsToEmbed)
		if err != nil {
			return err
		}
		for i, job := range jobs {
			thoughts[job.thoughtIdx].Vectors[job.summaryIdx] = vecs[i]
		}
	}

	var uncategorizedIdx []int
	var uncategorizedTexts []string

	// 2. Evaluate if we need to categorize
	for i, thought := range thoughts {
		if thought.Category != "" {
			continue // Already formally categorized
		}

		categorizedByMath := false
		if kb.MaxMathCategoryDistance > 0 && len(thought.Vectors) > 0 {
			closest, dist, err := kb.Manager.FindClosestCategory(ctx, thought.Vectors[0])
			if err == nil && closest != nil && dist <= kb.MaxMathCategoryDistance {
				thoughts[i].Category = closest.Name
				categorizedByMath = true
			}
		}

		if !categorizedByMath {
			uncategorizedIdx = append(uncategorizedIdx, i)
			// Aggregate summaries for LLM to deduce category
			agg := ""
			for _, s := range thought.Summaries {
				agg += s + " "
			}
			uncategorizedTexts = append(uncategorizedTexts, strings.TrimSpace(agg))
		}
	}

	// 3. Batch Fallback to LLM Pristine Cataloging
	if len(uncategorizedTexts) > 0 {
		generated, err := kb.Manager.GenerateCategories(ctx, uncategorizedTexts, persona)
		if err != nil {
			return err
		}

		// Re-align the generated outputs with the original array
		for idx, origIdx := range uncategorizedIdx {
			thoughts[origIdx].Category = generated[idx]
		}
	}

	// 4. Ensure Categories and Store
	for _, thought := range thoughts {
		_, err := kb.Manager.EnsureCategory(ctx, thought.Category)
		if err != nil {
			return err
		}

		item := Item[T]{
			ID:        sop.NewUUID(),
			Summaries: thought.Summaries,
			Data:      thought.Data,
		}

		err = kb.Store.UpsertByCategory(ctx, thought.Category, item, thought.Vectors)
		if err != nil {
			return err
		}
	}
	return nil
}

// IngestThought securely categorizes and stores a thought.
// If category is omitted (""), the LLM dynamically categorizes the text, unless it is close
// enough to an existing category centroid and MaxMathCategoryDistance > 0.
func (kb *KnowledgeBase[T]) IngestThought(
	ctx context.Context,
	text string,
	category string,
	persona string,
	vector []float32,
	data T,
) error {
	var vectors [][]float32
	if vector != nil {
		vectors = [][]float32{vector}
	}
	return kb.IngestThoughts(ctx, []Thought[T]{{Summaries: []string{text}, Category: category, Data: data, Vectors: vectors}}, persona)
}

// TriggerSleepCycle forces the LLM to scan, reflect, and re-organize dense categories.
func (kb *KnowledgeBase[T]) TriggerSleepCycle(ctx context.Context) error {
	return kb.Manager.SleepCycle(ctx)
}

// Vectorize iterates through all items in the KnowledgeBase, calculates or recalculates their embedding
// vectors using the configured embedder, and updates the store.
func (kb *KnowledgeBase[T]) Vectorize(ctx context.Context) error {
	if kb.Manager == nil || kb.Manager.embedder == nil {
		return errors.New("embedder is not configured: cannot vectorize space")
	}

	itemsBtree, err := kb.Store.Items(ctx)
	if err != nil {
		return err
	}

	catBtree, err := kb.Store.Categories(ctx)
	if err != nil {
		return err
	}

	catMap := make(map[sop.UUID]string)
	ok, _ := catBtree.First(ctx)
	for ok {
		cat, err := catBtree.GetCurrentValue(ctx)
		if err == nil && cat != nil {
			catMap[cat.ID] = cat.Name
		}
		ok, _ = catBtree.Next(ctx)
	}

	var itemsToUpdate []Item[T]

	ok, _ = itemsBtree.First(ctx)
	for ok {
		item, err := itemsBtree.GetCurrentValue(ctx)
		if err == nil {
			itemsToUpdate = append(itemsToUpdate, item)
		}
		ok, _ = itemsBtree.Next(ctx)
	}

	for _, item := range itemsToUpdate {
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

		vecs, err := kb.Manager.embedder.EmbedTexts(ctx, item.Summaries)
		if err != nil {
			return err
		}

		catName, found := catMap[item.CategoryID]
		if !found {
			continue
		}

		err = kb.Store.UpsertByCategory(ctx, catName, item, vecs)
		if err != nil {
			return err
		}
	}

	return nil
}

// VectorizeItems iterates through a specific set of items in the KnowledgeBase, calculates or recalculates their embedding
// vectors using the configured embedder, and updates the store.
func (kb *KnowledgeBase[T]) VectorizeItems(ctx context.Context, categoryID sop.UUID, itemIDs []sop.UUID) error {
	if kb.Manager == nil || kb.Manager.embedder == nil {
		return errors.New("embedder is not configured: cannot vectorize items")
	}

	itemsBtree, err := kb.Store.Items(ctx)
	if err != nil {
		return err
	}

	catBtree, err := kb.Store.Categories(ctx)
	if err != nil {
		return err
	}

	var category *Category
	if ok, _ := catBtree.Find(ctx, categoryID, false); ok {
		cat, _ := catBtree.GetCurrentValue(ctx)
		category = cat
	}
	if category == nil || category.Name == "" {
		return errors.New("category not found")
	}

	var itemsToUpdate []Item[T]

	if len(itemIDs) == 0 {
		ok, _ := itemsBtree.First(ctx)
		for ok {
			if item, err := itemsBtree.GetCurrentValue(ctx); err == nil && item.CategoryID == categoryID {
				itemsToUpdate = append(itemsToUpdate, item)
			}
			ok, _ = itemsBtree.Next(ctx)
		}
	} else {
		for _, id := range itemIDs {
			if ok, _ := itemsBtree.Find(ctx, id, false); ok {
				item, err := itemsBtree.GetCurrentValue(ctx)
				if err == nil && item.CategoryID == categoryID {
					itemsToUpdate = append(itemsToUpdate, item)
				}
			}
		}
	}

	batchSize := 50
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

		allVecs, err := kb.Manager.embedder.EmbedTexts(ctx, batchSummaries)
		if err != nil {
			return err
		}

		vecIdx := 0
		for j, item := range batch {
			count := itemSummaryCounts[j]
			itemVecs := allVecs[vecIdx : vecIdx+count]
			vecIdx += count

			err = kb.Store.UpsertByCategory(ctx, category.Name, item, itemVecs)
			if err != nil {
				return err
			}
		}
	}

	if len(category.CenterVector) == 0 {
		catVecs, _ := kb.Manager.embedder.EmbedTexts(ctx, []string{category.Name})
		if len(catVecs) > 0 {
			category.CenterVector = catVecs[0]
			catBtree.UpdateCurrentValue(ctx, category)
		}
	}

	return nil
}

// GetConfig retrieves the metadata configuration for this KnowledgeBase.
func (kb *KnowledgeBase[T]) GetConfig(ctx context.Context) (*KnowledgeBaseConfig, error) {
	itemsBtree, err := kb.Store.Items(ctx)
	if err != nil {
		return nil, err
	}

	found, err := itemsBtree.Find(ctx, sop.NilUUID, false)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil // No config found
	}

	item, err := itemsBtree.GetCurrentValue(ctx)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(item.Data)
	if err != nil {
		return nil, err
	}

	var cfg KnowledgeBaseConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}


// SetConfig saves the metadata configuration for this KnowledgeBase.
func (kb *KnowledgeBase[T]) SetConfig(ctx context.Context, config *KnowledgeBaseConfig) error {
	itemsBtree, err := kb.Store.Items(ctx)
	if err != nil {
		return err
	}

	found, err := itemsBtree.FindWithID(ctx, sop.NilUUID, sop.NilUUID)
	if err != nil && err.Error() == "not found" {
		found = false
	} else if err != nil {
		return err
	}

	var v T
	configBytes, err := json.Marshal(config)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(configBytes, &v); err != nil {
		return err
	}

	configItem := Item[T]{
		ID:         sop.NilUUID,
		CategoryID: sop.NilUUID,
		Data:       v,
	}

	if found {
		_, err = itemsBtree.UpdateCurrentItem(ctx, sop.NilUUID, configItem)
		return err
	}
	_, err = itemsBtree.Add(ctx, sop.NilUUID, configItem)
	return err
}
