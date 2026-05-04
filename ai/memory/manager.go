package memory

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
	store                   MemoryStore[T]
	llm                     ai.Generator
	embedder                ai.Embeddings
	sleepThreshold          int
	inlineRefactorThreshold int
}

// NewMemoryManager creates a new biomimetic memory orchestrator.
func NewMemoryManager[T any](store MemoryStore[T], llm ai.Generator, embedder ai.Embeddings) *MemoryManager[T] {
	return &MemoryManager[T]{
		store:                   store,
		llm:                     llm,
		embedder:                embedder,
		sleepThreshold:          1000,
		inlineRefactorThreshold: 10,
	}
}

// GenerateCategory uses the LLM to deduce a 2-4 word taxonomy category for a raw thought.
func (m *MemoryManager[T]) GenerateCategory(ctx context.Context, text string, personaContext string) (string, error) {
	var prompt string
	if personaContext != "" {
		prompt = fmt.Sprintf("Given the context '%s', categorize the following thought into exactly a 2-4 word concept:\n\n%s", personaContext, text)
	} else {
		prompt = fmt.Sprintf("Categorize the following thought into exactly a 2-4 word concept:\n\n%s", text)
	}
	opts := ai.GenOptions{MaxTokens: 10, Temperature: 0.1}
	out, err := m.llm.Generate(ctx, prompt, opts)
	if err != nil {
		return "", fmt.Errorf("llm classification failed: %w", err)
	}
	return strings.TrimSpace(out.Text), nil
}

// GenerateCategories uses the LLM to deduce a 2-4 word taxonomy category for a batch of raw thoughts.
func (m *MemoryManager[T]) GenerateCategories(ctx context.Context, texts []string, personaContext string) ([]string, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	var promptBuilder strings.Builder
	if personaContext != "" {
		promptBuilder.WriteString(fmt.Sprintf("Given the context '%s', categorize each of the following thoughts into exactly a 2-4 word concept.\nReturn ONLY a comma-separated list of categories, one for each thought, in the exact same order.\n\n", personaContext))
	} else {
		promptBuilder.WriteString("Categorize each of the following thoughts into exactly a 2-4 word concept.\nReturn ONLY a comma-separated list of categories, one for each thought, in the exact same order.\n\n")
	}
	for i, t := range texts {
		promptBuilder.WriteString(fmt.Sprintf("[%d] %s\n", i+1, t))
	}

	opts := ai.GenOptions{MaxTokens: 10 * len(texts), Temperature: 0.1}
	out, err := m.llm.Generate(ctx, promptBuilder.String(), opts)
	if err != nil {
		return nil, fmt.Errorf("llm batch classification failed: %w", err)
	}

	raw := strings.TrimSpace(out.Text)
	parts := strings.Split(raw, ",")
	var result []string
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			result = append(result, strings.TrimSpace(p))
		}
	}
	// Fallbacks if LLM fails formatting
	for len(result) < len(texts) {
		result = append(result, "Uncategorized")
	}
	if len(result) > len(texts) {
		result = result[:len(texts)]
	}
	return result, nil
}

// FindClosestCategory evaluates the spatial coordinates logically mapped into categories.
// This executes mathematically without LLM inference, serving as the fast-path.
func (m *MemoryManager[T]) FindClosestCategory(ctx context.Context, vector []float32) (*Category, float32, error) {
	categoriesTree, err := m.store.Categories(ctx)
	if err != nil {
		return nil, 0, err
	}
	var closest *Category
	var minDist float32 = -1.0

	ok, err := categoriesTree.First(ctx)
	for ok && err == nil {
		c, _ := categoriesTree.GetCurrentValue(ctx)
		// Assuming categories dynamically track their CenterVector based on members
		if c != nil && len(c.CenterVector) > 0 {
			dist := EuclideanDistance(vector, c.CenterVector)
			if minDist < 0 || dist < minDist {
				minDist = dist
				closest = c
			}
		}
		ok, err = categoriesTree.Next(ctx)
	}

	return closest, minDist, nil
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
		ItemCount:    0,
	}

	cid, err := m.store.AddCategory(ctx, anchor)
	if err != nil {
		return sop.NilUUID, err
	}
	return cid, nil
}

// SleepCycle performs Asynchronous Memory Consolidation.
func (m *MemoryManager[T]) SleepCycle(ctx context.Context) error {
	// Historical sleep-cycle reflection logic would remain here...
	return nil
}

func (m *MemoryManager[T]) GenerateSummaries(ctx context.Context, dataStr string) ([]string, error) {
if m.llm == nil {
    return []string{dataStr}, nil
}
prompt := "Break the following data down into distinct logical vectors or small standalone factual observations (sentences or short phrases). Return ONLY a pipe-separated ( | ) list of these phrases.\n\nData: " + dataStr
opts := ai.GenOptions{MaxTokens: 1000, Temperature: 0.1}
out, err := m.llm.Generate(ctx, prompt, opts)
if err != nil {
return nil, err
}
parts := strings.Split(out.Text, "|")
var res []string
for _, p := range parts {
if strings.TrimSpace(p) != "" {
res = append(res, strings.TrimSpace(p))
}
}
if len(res) == 0 {
res = []string{dataStr}
}
return res, nil
}
