package memory

import (
	"context"
	"errors"

	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
)

// BaseKnowledgeBase encapsulates the shared search and store logic across all bases.
type BaseKnowledgeBase[T any] struct {
	Store MemoryStore[T]
}

func (kb *BaseKnowledgeBase[T]) SearchSemanticsBatch(ctx context.Context, queryVectors [][]float32, opts *SearchOptions[T]) ([][]ai.Hit[T], error) {
	return kb.Store.QueryBatch(ctx, queryVectors, opts)
}

// SearchKeywordsBatch executes textual BM25 text-matched sparse searches for multiple text payloads.
func (kb *BaseKnowledgeBase[T]) SearchKeywordsBatch(ctx context.Context, textQueries []string, opts *SearchOptions[T]) ([][]ai.Hit[T], error) {
	return kb.Store.QueryTextBatch(ctx, textQueries, opts)
}

// SearchSemantics executes a spatial search (vector matching against mathematical bounds).
func (kb *BaseKnowledgeBase[T]) SearchSemantics(ctx context.Context, queryVector []float32, opts *SearchOptions[T]) ([]ai.Hit[T], error) {
	return kb.Store.Query(ctx, queryVector, opts)
}

// SearchKeywords executes a traditional textual BM25 text-matched sparse search.
func (kb *BaseKnowledgeBase[T]) SearchKeywords(ctx context.Context, textQuery string, opts *SearchOptions[T]) ([]ai.Hit[T], error) {
	return kb.Store.QueryText(ctx, textQuery, opts)
}

// KnowledgeBase provides a clean, unified API for developers.
// It orchestrates both the storage tables and the LLM memory management.
type KnowledgeBase[T any] struct {
	BaseKnowledgeBase[T]
	Manager *MemoryManager[T]
	// MaxMathCategoryDistance specifies the max Euclidean distance to cluster centroids
	// to avoid calling the LLM for category categorization. Set to 0.0 or less to disable
	// and always rely on "pristine" LLM categorization.
	MaxMathCategoryDistance float32
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
