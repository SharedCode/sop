package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	log "log/slog"

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

	configCache *KnowledgeBaseConfig
}

// Returns this KnowledgeBase's name.
func (kb *KnowledgeBase[T]) Name() string {
	return kb.Store.Name()
}

func (kb *KnowledgeBase[T]) SearchSemanticsBatch(ctx context.Context, queryVectors [][]float32, opts *SearchOptions[T]) ([][]ai.Hit[T], error) {
	config, err := kb.GetConfig(ctx)
	if err == nil && config != nil && config.LastVectorized == 0 {
		return nil, fmt.Errorf("Knowledge base is not vectorized")
	}
	return kb.Store.QueryBatch(ctx, queryVectors, opts)
}

// SearchKeywordsBatch executes textual BM25 text-matched sparse searches for multiple text payloads.
func (kb *KnowledgeBase[T]) SearchKeywordsBatch(ctx context.Context, textQueries []string, opts *SearchOptions[T]) ([][]ai.Hit[T], error) {
	config, err := kb.GetConfig(ctx)
	if err == nil && config != nil && config.LastVectorized == 0 {
		return nil, fmt.Errorf("Knowledge base is not vectorized")
	}
	return kb.Store.QueryTextBatch(ctx, textQueries, opts)
}

func searchOptionsSummary[T any](opts *SearchOptions[T]) []any {
	if opts == nil {
		return []any{"limit", 0, "category_path", "", "has_filter", false}
	}
	return []any{"limit", opts.Limit, "category_path", opts.CategoryPath, "has_filter", opts.Filter != nil}
}

// SearchSemantics executes a spatial search (vector matching against mathematical bounds).
func (kb *KnowledgeBase[T]) SearchSemantics(ctx context.Context, queryVector []float32, opts *SearchOptions[T]) ([]ai.Hit[T], error) {
	log.Info("SearchSemantics invoked",
		append([]any{"kb", kb.Name(), "query_vector_len", len(queryVector)}, searchOptionsSummary(opts)...)...)
	config, err := kb.GetConfig(ctx)
	if err == nil && config != nil && config.LastVectorized == 0 {
		log.Warn("SearchSemantics blocked: KB not vectorized", "kb", kb.Name(), "last_vectorized", config.LastVectorized)
		return nil, fmt.Errorf("Knowledge base is not vectorized")
	}
	hits, err := kb.Store.Query(ctx, queryVector, opts)
	if err != nil {
		log.Error("SearchSemantics failed", "kb", kb.Name(), "error", err)
	} else {
		log.Info("SearchSemantics completed", "kb", kb.Name(), "hit_count", len(hits))
	}
	return hits, err
}

// SearchKeywords executes a traditional textual BM25 text-matched sparse search.
func (kb *KnowledgeBase[T]) SearchKeywords(ctx context.Context, textQuery string, opts *SearchOptions[T]) ([]ai.Hit[T], error) {
	log.Info("SearchKeywords invoked",
		append([]any{"kb", kb.Name(), "query", textQuery}, searchOptionsSummary(opts)...)...)
	config, err := kb.GetConfig(ctx)
	if err == nil && config != nil && config.LastVectorized == 0 {
		log.Warn("SearchKeywords blocked: KB not vectorized", "kb", kb.Name(), "last_vectorized", config.LastVectorized)
		return nil, fmt.Errorf("Knowledge base is not vectorized")
	}
	hits, err := kb.Store.QueryText(ctx, textQuery, opts)
	if err != nil {
		log.Error("SearchKeywords failed", "kb", kb.Name(), "error", err)
	} else {
		log.Info("SearchKeywords completed", "kb", kb.Name(), "hit_count", len(hits))
	}
	return hits, err
}

// Thought represents the individual entity of data in a batch categorization execution.
type Thought[T any] struct {
	Summaries    []string
	CategoryPath string
	DocID        DocIDs
	Data         T
	Vectors      [][]float32
	Positions    []VectorKey
	VectorHash   string
}

// IngestThoughts securely categorizes and stores an array of thoughts, optimizing latency
// by clustering queries and sending a batch request to the LLM generator.
func (kb *KnowledgeBase[T]) IngestThoughts(ctx context.Context, thoughts []Thought[T], persona string) error {
	if len(thoughts) == 0 {
		return nil
	}

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

	var uncategorizedIdx []int
	var uncategorizedTexts []string

	// 2. Evaluate if we need to categorize
	for i, thought := range thoughts {
		if thought.CategoryPath != "" {
			continue // Already formally categorized
		}

		categorizedByMath := false
		if kb.MaxMathCategoryDistance > 0 && len(thought.Vectors) > 0 {
			closest, dist, err := kb.Manager.FindClosestCategory(ctx, thought.Vectors[0])
			if err == nil && closest != nil && dist <= kb.MaxMathCategoryDistance {
				thoughts[i].CategoryPath = closest.Name
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
			thoughts[origIdx].CategoryPath = generated[idx]
		}
	}

	// 4. Ensure Categories and Store
	catCache := make(map[string]sop.UUID)
	for _, thought := range thoughts {
		catID, exists := catCache[thought.CategoryPath]
		if !exists {
			var err error
			catID, err = kb.Manager.EnsureCategory(ctx, thought.CategoryPath)
			if err != nil {
				return err
			}
			catCache[thought.CategoryPath] = catID
		}

		item := Item[T]{
			ID:         sop.NewUUID(),
			DocID:      thought.DocID,
			Summaries:  thought.Summaries,
			Data:       thought.Data,
			Positions:  thought.Positions,
			VectorHash: thought.VectorHash,
		}

		err := kb.Store.UpsertByCategoryID(ctx, catID, nil, item, thought.Vectors)
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
	return kb.IngestThoughts(ctx, []Thought[T]{{Summaries: []string{text}, CategoryPath: category, Data: data, Vectors: vectors}}, persona)
}

// TriggerSleepCycle forces the LLM to scan, reflect, and re-organize dense categories.
func (kb *KnowledgeBase[T]) TriggerSleepCycle(ctx context.Context) error {
	if err := kb.Manager.SleepCycle(ctx); err != nil {
		return err
	}
	return nil
}

// GetConfig retrieves the metadata configuration for this KnowledgeBase.
func (kb *KnowledgeBase[T]) GetConfig(ctx context.Context) (*KnowledgeBaseConfig, error) {
	if kb.configCache != nil {
		return kb.configCache, nil
	}

	itemsBtree, err := kb.Store.Items(ctx)
	if err != nil {
		return nil, err
	}

	nilKey := ItemKey{CategoryID: sop.NilUUID, ItemID: sop.NilUUID}
	found, err := itemsBtree.Find(ctx, nilKey, false)
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

	kb.configCache = &cfg
	return &cfg, nil
}

// SetConfig saves the metadata configuration for this KnowledgeBase.
func (kb *KnowledgeBase[T]) SetConfig(ctx context.Context, config *KnowledgeBaseConfig) error {
	itemsBtree, err := kb.Store.Items(ctx)
	if err != nil {
		return err
	}

	nilKey := ItemKey{CategoryID: sop.NilUUID, ItemID: sop.NilUUID}

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

	_, err = itemsBtree.Upsert(ctx, nilKey, configItem)
	if err == nil {
		kb.configCache = config
	}
	return err
}

// ComputeVectorHash computes a predictable string hash representing the text content.
// We optionally include dimensions, but explicitly exclude embedderName so that compatible models (same dims) don't trigger re-vectorization unless content actually changes.
func ComputeVectorHash(embedderDim int, texts ...string) string {
	hasher := sha256.New()

	dimStr := "dim:" + string(rune(embedderDim)) + "::"
	hasher.Write([]byte(dimStr))

	for _, t := range texts {
		hasher.Write([]byte(t + "::"))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
