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

func TestKnowledgeBase_API(t *testing.T) {
	ctx := context.Background()

	cats := inmemory.NewBtree[sop.UUID, *Category](true)
	vecs := inmemory.NewBtree[VectorKey, Vector](true)
	items := inmemory.NewBtree[sop.UUID, Item[string]](true)

	s := NewStore[string](cats.Btree, vecs.Btree, items.Btree).(*store[string])
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
		BaseKnowledgeBase: BaseKnowledgeBase[string]{Store: s},
		Manager:           manager,
	}

	err := kb.IngestThoughts(ctx, []Thought[string]{{Summaries: []string{"test_cat"}, Category: "test_id", Data: "payload"}}, "test")
	if err != nil {
		t.Fatalf("IngestThought failed: %v", err)
	}

	hits, err := kb.SearchSemantics(ctx, []float32{1.0, 1.0, 1.0}, &SearchOptions[string]{Limit: 10})
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
	items := inmemory.NewBtree[sop.UUID, Item[string]](false)

	memStore := NewStore[string](categories.Btree, vectors.Btree, items.Btree)
	ds := memStore.(*store[string])
	ds.SetTextIndex(&MockTextIndex{})

	kb := KnowledgeBase[string]{
		BaseKnowledgeBase: BaseKnowledgeBase[string]{Store: ds},
		Manager:           NewMemoryManager[string](ds, &MockLLM{}, &MockEmbedder{}),
	}

	err := kb.IngestThoughts(ctx, []Thought[string]{{Summaries: []string{"Apple is a fruit"}, Category: "Fruits", Vectors: [][]float32{{0.1, 0.2, 0.3}}, Data: "apple is a fruit"}}, "")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = kb.IngestThoughts(ctx, []Thought[string]{{Summaries: []string{"Car is a vehicle"}, Category: "Vehicles", Vectors: [][]float32{{0.9, 0.8, 0.7}}, Data: "car"}}, "")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	hits, err := kb.SearchSemantics(ctx, []float32{0.1, 0.2, 0.3}, &SearchOptions[string]{Limit: 10, Category: "Fruits"})
	if err != nil {
		t.Fatalf("SearchSemantics failed: %v", err)
	}
	if len(hits) == 0 {
		t.Errorf("Expected hits from SearchSemantics in Fruits category, got 0")
	} else if hits[0].Payload != "apple is a fruit" {
		t.Errorf("Expected apple is a fruit hit, got %v", hits[0].Payload)
	}

	hitsVehicles, err := kb.SearchSemantics(ctx, []float32{0.1, 0.2, 0.3}, &SearchOptions[string]{Limit: 10, Category: "Vehicles"})
	if err != nil {
		t.Fatalf("SearchSemantics failed: %v", err)
	}
	if len(hitsVehicles) != 1 || hitsVehicles[0].Payload != "car" {
		t.Errorf("Expected hits from SearchSemantics in Vehicles category")
	}

	khits, err := kb.SearchKeywords(ctx, "fruit", &SearchOptions[string]{Category: "Fruits", Limit: 10})
	if err != nil {
		t.Fatalf("SearchKeywords failed: %v", err)
	}
	if len(khits) == 0 {
		t.Errorf("Expected hits from SearchKeywords, got 0")
	}

	khitsEmpty, err := kb.SearchKeywords(ctx, "fruit", &SearchOptions[string]{Category: "NonExistent", Limit: 10})
	if err != nil {
		t.Fatalf("SearchKeywords failed: %v", err)
	}
	if len(khitsEmpty) != 0 {
		t.Errorf("Expected no hits for non-existent category, got %v", khitsEmpty)
	}
}
