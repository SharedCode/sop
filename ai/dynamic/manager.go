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
// 1. Asks LLM for a high-level category representing the text.
// 2. Ensures that Category exists as an Anchor.
// 3. Upserts the thought vector directly targeting that Category loosely.
func (m *MemoryManager[T]) IngestThought(ctx context.Context, text string, data T) error {
	prompt := fmt.Sprintf("Categorize the following thought into a 2-4 word concept:\n\n%s", text)
	opts := ai.GenOptions{MaxTokens: 10, Temperature: 0.1}
	out, err := m.llm.Generate(ctx, prompt, opts)
	if err != nil {
		return fmt.Errorf("llm classification failed: %w", err)
	}
	categoryName := strings.TrimSpace(out.Text)

	_, err = m.EnsureCategory(ctx, categoryName)
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
	centroidsTree, err := m.store.Centroids(ctx)
	if err != nil {
		return sop.NilUUID, err
	}

	ok, err := centroidsTree.First(ctx)
	for ok && err == nil {
		c, _ := centroidsTree.GetCurrentValue(ctx)
		if c != nil && strings.EqualFold(c.Name, categoryName) {
			return centroidsTree.GetCurrentKey().Key, nil
		}
		ok, err = centroidsTree.Next(ctx)
	}

	vecs, err := m.embedder.EmbedTexts(ctx, []string{categoryName})
	if err != nil {
		return sop.NilUUID, fmt.Errorf("failed to embed new category: %w", err)
	}

	CID := sop.NewUUID()
	anchor := &Centroid{
		ID:           CID,
		Name:         categoryName,
		Description:  "LLM Generated Semantic Anchor",
		CenterVector: vecs[0],
		VectorCount:  0,
	}

	cid, err := m.store.AddCentroid(ctx, anchor)
	if err != nil {
		return sop.NilUUID, err
	}
	return cid, nil
}

// SleepCycle performs Asynchronous Memory Consolidation.
// It runs periodically (nightly) to scan heavily packed Centroids and asks the LLM
// if deeper, more granular categories should be split off.
func (m *MemoryManager[T]) SleepCycle(ctx context.Context) error {
	centroidsTree, err := m.store.Centroids(ctx)
	if err != nil {
		return err
	}

	ok, err := centroidsTree.First(ctx)
	for ok && err == nil {
		c, _ := centroidsTree.GetCurrentValue(ctx)

		// If an Anchor is getting extremely dense, "Reflect" on it.
		if c != nil && c.VectorCount > m.sleepThreshold {
			err = m.reflectAndReassociate(ctx, c)
			if err != nil {
				fmt.Printf("Sleep cycle reflection failed for %s: %v\n", c.Name, err)
			}
		}
		ok, err = centroidsTree.Next(ctx)
	}

	return nil
}

// reflectAndReassociate pulls the payloads from a dense centroid, asks LLM to find
// sub-themes, and surgically moves specific thoughts without rewriting the whole cluster.
func (m *MemoryManager[T]) reflectAndReassociate(ctx context.Context, anchor *Centroid) error {
	// 1. Scan vectors stored under anchor.ID
	// 2. Sample N payloads.
	// 3. Prompt LLM: "These thoughts are under '" + anchor.Name + "'. Can you identify 3 tighter sub-categories?"
	// 4. EnsureCategory() for each new LLM deduction.
	// 5. Compare semantic distance of payloads to the new sub-categories vs the old anchor.
	// 6. Delete old VectorKey, insert new VectorKey for payloads closer to the new sub-categories, logically re-adjusting!
	return nil
}
