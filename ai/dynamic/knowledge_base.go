package dynamic

import (
	"context"

	"github.com/sharedcode/sop/ai"
)

// BaseKnowledgeBase encapsulates the shared search and store logic across all bases.
type BaseKnowledgeBase[T any] struct {
	Store DynamicVectorStore[T]
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
}

// IngestThought securely categorizes and stores a thought using the LLM & Embedder.
func (kb *KnowledgeBase[T]) IngestThought(ctx context.Context, text string, category string, persona string, data T) error {
	return kb.Manager.IngestThought(ctx, text, category, persona, data)
}

// TriggerSleepCycle forces the LLM to scan, reflect, and re-organize dense categories.
func (kb *KnowledgeBase[T]) TriggerSleepCycle(ctx context.Context) error {
	return kb.Manager.SleepCycle(ctx)
}

// StaticKnowledgeBase provides direct access to the store without LLM refactoring,
// designed for fixed static categories natively provided by standard data insertion.
type StaticKnowledgeBase[T any] struct {
	BaseKnowledgeBase[T]
}

// Insert bypasses the LLM and creates the embedded vectorized thought item identically
// assigned to the strictly specified static category without external AI reflection.
func (kb *StaticKnowledgeBase[T]) Insert(
	ctx context.Context,
	text string,
	category string,
	vector []float32,
	data T,
) error {
	// Let's create an ai.Item
	item := ai.Item[T]{
		ID:      text, // Often ID or Text text
		Payload: data,
		Vector:  vector,
	}
	return kb.Store.UpsertByCategory(ctx, category, item)
}
