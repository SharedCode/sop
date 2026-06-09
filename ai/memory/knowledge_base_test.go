package memory

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/inmemory"
)

// Ensure mocks implement standard ai methods
func (m *MockPlaybookEmbedder) Dim() int                     { return 3 }
func (m *MockPlaybookEmbedder) Name() string                 { return "mock" }
func (m *MockPlaybookLLM) Name() string                      { return "mock" }
func (m *MockPlaybookLLM) EstimateCost(x int, y int) float64 { return 0 }
func (m *MockPlaybookLLM) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	return ai.GenOutput{}, nil
}
func (m *MockPlaybookLLM) PrewarmCache(ctx context.Context, opts ai.GenOptions) error {
	return nil
}

func TestKnowledgeBase_API(t *testing.T) {
	ctx := context.Background()

	cats := inmemory.NewBtree[sop.UUID, *Category](true)
	vecs := inmemory.NewBtree[VectorKey, Vector](true)
	items := inmemory.NewBtree[ItemKey, Item[string]](true)

	s := NewStore[string]("test_kb", nil, cats.Btree, inmemory.NewBtree[string, sop.UUID](false).Btree, inmemory.NewBtree[DistanceKey, byte](false).Btree, vecs.Btree, items.Btree, inmemory.NewBtree[sop.UUID, Document](false).Btree).(*store[string])
	s.SetTextIndex(&MockTextIndex{})

	embedder := &MockPlaybookEmbedder{Rules: []PlaybookRule{
		{Keywords: []string{"test"}, CategoryName: "test_cat", Vector: []float32{1.0, 1.0, 1.0}},
	}}
	llm := &MockPlaybookLLM{Rules: []PlaybookRule{
		{Keywords: []string{"test"}, CategoryName: "test_cat", Vector: []float32{1.0, 1.0, 1.0}},
	}, Embedder: embedder}
	s.SetLLM(llm)

	manager := NewMemoryManager[string](s, llm, embedder)

	kb := &KnowledgeBase[string]{
		Store:   s,
		Manager: manager,
	}

	// We inject vectors during IngestThoughts for this test to bypass vectorize
	err := kb.IngestThoughts(ctx, []Thought[string]{{Summaries: []string{"test_cat"}, CategoryPath: "test_id", Vectors: [][]float32{{1.0, 1.0, 1.0}}, Data: "payload"}}, "test")
	if err != nil {
		t.Fatalf("IngestThought failed: %v", err)
	}

	hits, err := kb.SearchSemantics(ctx, []float32{1.0, 1.0, 1.0}, &SearchOptions[string]{Limit: 10, CategoryPath: "test_id"})
	if err != nil {
		t.Fatalf("SearchSemantics failed: %v", err)
	}
	if len(hits) == 0 {
		t.Errorf("Expected hits from SearchSemantics, got 0")
	}

	khits, err := kb.SearchKeywords(ctx, "payload", &SearchOptions[string]{Limit: 10})
	if err != nil {
		t.Fatalf("SearchKeywords failed: %v", err)
	}
	if len(khits) == 0 {
		t.Errorf("Expected hits from SearchKeywords, got 0")
	}

	err = kb.TriggerSleepCycle(ctx)
	if err != nil {
		t.Fatalf("TriggerSleepCycle failed: %v", err)
	}
}

func TestStaticKnowledgeBase(t *testing.T) {
	ctx := context.Background()

	categories := inmemory.NewBtree[sop.UUID, *Category](true)
	vectors := inmemory.NewBtree[VectorKey, Vector](false)
	items := inmemory.NewBtree[ItemKey, Item[string]](false)

	memStore := NewStore[string]("test_kb", nil, categories.Btree, inmemory.NewBtree[string, sop.UUID](false).Btree, inmemory.NewBtree[DistanceKey, byte](false).Btree, vectors.Btree, items.Btree, inmemory.NewBtree[sop.UUID, Document](false).Btree)
	ds := memStore.(*store[string])
	ds.SetTextIndex(&MockTextIndex{})

	kb := KnowledgeBase[string]{
		Store:   ds,
		Manager: NewMemoryManager[string](ds, &MockLLM{}, &MockEmbedder{}),
	}

	err := kb.IngestThoughts(ctx, []Thought[string]{{Summaries: []string{"Apple is a fruit"}, CategoryPath: "Fruits", Vectors: [][]float32{{0.1, 0.2, 0.3}}, Data: "apple is a fruit"}}, "")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = kb.IngestThoughts(ctx, []Thought[string]{{Summaries: []string{"Car is a vehicle"}, CategoryPath: "Vehicles", Vectors: [][]float32{{0.9, 0.8, 0.7}}, Data: "car"}}, "")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	hits, err := kb.SearchSemantics(ctx, []float32{0.1, 0.2, 0.3}, &SearchOptions[string]{Limit: 10, CategoryPath: "Fruits"})
	if err != nil {
		t.Fatalf("SearchSemantics failed: %v", err)
	}
	if len(hits) == 0 {
		t.Errorf("Expected hits from SearchSemantics in Fruits category, got 0")
	} else if hits[0].Payload != "apple is a fruit" {
		t.Errorf("Expected apple is a fruit hit, got %v", hits[0].Payload)
	}

	hitsVehicles, err := kb.SearchSemantics(ctx, []float32{0.1, 0.2, 0.3}, &SearchOptions[string]{Limit: 10, CategoryPath: "Vehicles"})
	if err != nil {
		t.Fatalf("SearchSemantics failed: %v", err)
	}
	if len(hitsVehicles) != 1 || hitsVehicles[0].Payload != "car" {
		t.Errorf("Expected hits from SearchSemantics in Vehicles category")
	}

	khits, err := kb.SearchKeywords(ctx, "fruit", &SearchOptions[string]{CategoryPath: "Fruits", Limit: 10})
	if err != nil {
		t.Fatalf("SearchKeywords failed: %v", err)
	}
	if len(khits) == 0 {
		t.Errorf("Expected hits from SearchKeywords, got 0")
	}

	khitsEmpty, err := kb.SearchKeywords(ctx, "fruit", &SearchOptions[string]{CategoryPath: "NonExistent", Limit: 10})
	if err != nil {
		t.Fatalf("SearchKeywords failed: %v", err)
	}
	if len(khitsEmpty) != 0 {
		t.Errorf("Expected no hits for non-existent category, got %v", khitsEmpty)
	}
}

func TestSearchOptionsSummary_SafelyExcludesFilterFunction(t *testing.T) {
	opts := &SearchOptions[map[string]any]{
		Limit:          7,
		CategoryPath:   "docs",
		CategoryVector: []float32{1, 2},
		Filter:         func(map[string]any) bool { return true },
	}

	summary := searchOptionsSummary(opts)
	if len(summary) != 6 {
		t.Fatalf("expected 6 summary fields, got %d", len(summary))
	}
	if got := summary[0].(string); got != "limit" {
		t.Fatalf("expected limit key, got %v", got)
	}
	if got := summary[1].(int); got != 7 {
		t.Fatalf("expected limit value 7, got %v", got)
	}
	if got := summary[2].(string); got != "category_path" {
		t.Fatalf("expected category_path key, got %v", got)
	}
	if got := summary[3].(string); got != "docs" {
		t.Fatalf("expected docs category path, got %v", got)
	}
	if got := summary[4].(string); got != "has_filter" {
		t.Fatalf("expected has_filter key, got %v", got)
	}
	if got := summary[5].(bool); !got {
		t.Fatalf("expected has_filter to be true, got %v", got)
	}
}

func TestKnowledgeBase_SearchKeywords_NoTextSearchEnabledReturnsNoHits(t *testing.T) {
	ctx := context.Background()

	categories := inmemory.NewBtree[sop.UUID, *Category](true)
	vectors := inmemory.NewBtree[VectorKey, Vector](true)
	items := inmemory.NewBtree[ItemKey, Item[string]](true)

	store := NewStore[string]("test_kb", nil, categories.Btree, inmemory.NewBtree[string, sop.UUID](false).Btree, inmemory.NewBtree[DistanceKey, byte](false).Btree, vectors.Btree, items.Btree, inmemory.NewBtree[sop.UUID, Document](false).Btree)
	kb := KnowledgeBase[string]{
		Store:   store,
		Manager: NewMemoryManager[string](store, &MockLLM{}, &MockEmbedder{}),
	}

	hits, err := kb.SearchKeywords(ctx, "fruit", &SearchOptions[string]{Limit: 10})
	if err != nil {
		t.Fatalf("SearchKeywords should not fail when text search is disabled: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("expected no keyword hits when text search is disabled, got %v", hits)
	}
}
