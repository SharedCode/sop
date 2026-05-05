package memory

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/inmemory"
)

func TestKnowledgeBase_VectorizeItems(t *testing.T) {
	ctx := context.Background()
	cats := inmemory.NewBtree[sop.UUID, *Category](true)
	vecs := inmemory.NewBtree[VectorKey, Vector](true)
	items := inmemory.NewBtree[sop.UUID, Item[string]](true)
	s := NewStore[string](cats.Btree, vecs.Btree, items.Btree).(*store[string])
	s.SetTextIndex(&MockTextIndex{})

	embedder := &MockPlaybookEmbedder{Rules: []PlaybookRule{
		{Keywords: []string{"test_data1"}, CategoryName: "test_cat", Vector: []float32{1.0, 1.0, 1.0}},
		{Keywords: []string{"test_data2"}, CategoryName: "test_cat", Vector: []float32{2.0, 2.0, 2.0}},
	}}
	llm := &MockPlaybookLLM{Rules: []PlaybookRule{
		{Keywords: []string{"test"}, CategoryName: "test_cat", Vector: []float32{1.0, 1.0, 1.0}},
	}, Embedder: embedder}

	manager := NewMemoryManager[string](s, llm, embedder)

	kb := &KnowledgeBase[string]{
		BaseKnowledgeBase: BaseKnowledgeBase[string]{Store: s},
		Manager:           manager,
	}

	cat := &Category{
		ID:           sop.NewUUID(),
		Name:         "test_cat",
		CenterVector: []float32{0, 0, 0},
	}
	s.categories.Add(ctx, cat.ID, cat)

	id1 := sop.NewUUID()
	item1 := Item[string]{
		ID:         id1,
		CategoryID: cat.ID,
		Data:       "test_data1",
		Positions:  nil, // No vectors yet
	}
	items.Add(id1, item1)

	id2 := sop.NewUUID()
	item2 := Item[string]{
		ID:         id2,
		CategoryID: cat.ID,
		Data:       "other_data",
		Positions:  nil, // No vectors yet
	}
	items.Add(id2, item2)

	err := kb.VectorizeItems(ctx, cat.ID, []sop.UUID{id1})
	if err != nil {
		t.Fatalf("VectorizeItems failed: %v", err)
	}

	ok := items.Find(id1, false)
	it1 := items.GetCurrentValue()
	if !ok || len(it1.Positions) == 0 {
		t.Fatalf("Expected item1 positions to be populated")
	}

	ok = items.Find(id2, false)
	it2 := items.GetCurrentValue()
	if !ok || it2.Positions != nil {
		t.Fatalf("Expected item2 positions to be unmodified (nil)")
	}

	err = kb.VectorizeItems(ctx, cat.ID, nil) // Vectorize all items in the category
	if err != nil {
		t.Fatalf("VectorizeItems (all) failed: %v", err)
	}

	ok = items.Find(id2, false)
	it2 = items.GetCurrentValue()
	if !ok || len(it2.Positions) == 0 {
		t.Fatalf("Expected item2 positions to be populated after vectorizing all items")
	}
}
