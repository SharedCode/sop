package dynamic

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/inmemory"
	"github.com/sharedcode/sop/search"
)

// Compare implementation needed for ordered sorting
func (k VectorKey) Compare(other interface{}) int {
	o := other.(VectorKey)
	if k.CategoryID.Compare(o.CategoryID) != 0 {
		return k.CategoryID.Compare(o.CategoryID)
	}
	if k.DistanceToCategory < o.DistanceToCategory {
		return -1
	} else if k.DistanceToCategory > o.DistanceToCategory {
		return 1
	}
	return k.VectorID.Compare(o.VectorID)
}

func TestDynamicStore_Upsert(t *testing.T) {
	ctx := context.Background()
	categories := inmemory.NewBtree[sop.UUID, *Category](true)
	vectors := inmemory.NewBtree[VectorKey, Vector](false)
	items := inmemory.NewBtree[sop.UUID, Item[string]](false)

	s := NewStore[string](categories.Btree, vectors.Btree, items.Btree)

	err := s.Upsert(ctx, ai.Item[string]{
		ID:      sop.NewUUID().String(),
		Vector:  []float32{0.1, 0.2, 0.3},
		Payload: "LLM Thought One",
	})
	if err != nil {
		t.Fatalf("Failed to upsert item: %v", err)
	}

	count := categories.Count()
	if count != 1 {
		t.Fatalf("Expected 1 category, found %v", count)
	}

	vc := vectors.Count()
	if vc != 1 {
		t.Fatalf("Expected 1 vector, found %v", vc)
	}

	cc := items.Count()
	if cc != 1 {
		t.Fatalf("Expected 1 content, found %v", cc)
	}

	err = s.Upsert(ctx, ai.Item[string]{
		ID:      sop.NewUUID().String(),
		Vector:  []float32{0.15, 0.21, 0.33},
		Payload: "LLM Thought Two",
	})
	if err != nil {
		t.Fatalf("Failed to upsert second item: %v", err)
	}

	if vectors.Count() != 2 {
		t.Fatalf("Expected exactly 2 vectors, found %v", vectors.Count())
	}
}

type MockTextIndex struct {
	data map[string]string
}

func (m *MockTextIndex) Add(ctx context.Context, docID string, text string) error {
	if m.data == nil {
		m.data = make(map[string]string)
	}
	m.data[docID] = text
	return nil
}

func (m *MockTextIndex) Search(ctx context.Context, query string) ([]search.TextSearchResult, error) {
	var res []search.TextSearchResult
	for k, v := range m.data {
		if v == query {
			res = append(res, search.TextSearchResult{DocID: k, Score: 1.0})
		}
	}
	return res, nil
}

func (m *MockTextIndex) Delete(ctx context.Context, docID string) error {
	delete(m.data, docID)
	return nil
}

func TestDynamicStore_SimulateLLMSleepCycle(t *testing.T) {
	ctx := context.Background()

	categories := inmemory.NewBtree[sop.UUID, *Category](true)
	vectors := inmemory.NewBtree[VectorKey, Vector](true)
	items := inmemory.NewBtree[sop.UUID, Item[string]](true)

	ds := NewStore[string](categories.Btree, vectors.Btree, items.Btree)
	s := ds.(*store[string])

	s.SetTextIndex(&MockTextIndex{})

	item1 := ai.Item[string]{
		ID:      sop.NewUUID().String(),
		Vector:  []float32{0.1, 0.2, 0.3},
		Payload: "Apple is a fruit",
	}

	err := s.Upsert(ctx, item1)
	if err != nil {
		t.Fatalf("Failed to upsert item1: %v", err)
	}

	newRootCat := &Category{
		ID:           sop.NewUUID(),
		Name:         "Fruits",
		CenterVector: []float32{0.11, 0.21, 0.31},
	}

	_, err = s.categories.Add(ctx, newRootCat.ID, newRootCat)
	if err != nil {
		t.Fatalf("Failed to add new category: %v", err)
	}

	parsedID, _ := sop.ParseUUID(item1.ID)
	err = s.DeleteItem(ctx, parsedID)
	if err != nil {
		t.Fatalf("Failed to delete item: %v", err)
	}

	vecCount := vectors.Count()
	if vecCount != 0 {
		t.Errorf("Expected 0 vectors after delete, got %v", vecCount)
	}

	item1.Vector = []float32{0.11, 0.21, 0.31}
	err = s.Upsert(ctx, item1)
	if err != nil {
		t.Fatalf("Failed to re-upsert: %v", err)
	}

	hits, err := s.Query(ctx, []float32{0.11, 0.21, 0.31}, 5, nil)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	if len(hits) != 1 {
		t.Errorf("Expected 1 hit, got %d", len(hits))
	} else if hits[0].Payload != "Apple is a fruit" {
		t.Errorf("Expected hit payload 'Apple is a fruit', got %v", hits[0].Payload)
	}

	textHits, err := ds.QueryText(ctx, "Apple is a fruit", 5, nil)
	if err != nil {
		t.Fatalf("Failed to text search: %v", err)
	}

	if len(textHits) != 1 {
		t.Errorf("Expected 1 text hit, got %d", len(textHits))
	} else if textHits[0].Payload != "Apple is a fruit" {
		t.Errorf("Expected text hit payload 'Apple is a fruit', got %v", textHits[0].Payload)
	}
}

func TestDynamicStore_PublicAPIs(t *testing.T) {
	ctx := context.Background()

	categories := inmemory.NewBtree[sop.UUID, *Category](true)
	vectors := inmemory.NewBtree[VectorKey, Vector](true)
	items := inmemory.NewBtree[sop.UUID, Item[string]](true)
	s := NewStore[string](categories.Btree, vectors.Btree, items.Btree)

	err := s.UpsertBatch(ctx, []ai.Item[string]{
		{ID: sop.NewUUID().String(), Vector: []float32{0.1}, Payload: "1"},
		{ID: sop.NewUUID().String(), Vector: []float32{0.2}, Payload: "2"},
	})
	if err != nil {
		t.Fatalf("UpsertBatch failed: %v", err)
	}

	count, _ := s.Count(ctx)
	if count != 2 {
		t.Errorf("Expected Count=2, got %v", count)
	}

	itemTree, _ := s.Items(ctx)
	ok, err := itemTree.First(ctx)
	if !ok || err != nil {
		t.Fatalf("No items found")
	}
	firstItem, _ := itemTree.GetCurrentValue(ctx)
	_, err = s.Get(ctx, firstItem.ID)
	if err != nil {
		t.Errorf("Get API failed or returned wrong item")
	}

	err = s.Delete(ctx, firstItem.ID)
	if err != nil {
		t.Errorf("Delete API failed: %v", err)
	}

	_, err = s.Get(ctx, firstItem.ID)
	if err == nil {
		t.Errorf("Expected error fetching deleted item")
	}

	count, _ = s.Count(ctx)
	if count != 1 {
		t.Errorf("Expected Count=1 after delete, got %v", count)
	}

	s.Consolidate(ctx)
	s.SetDeduplication(false)
	s.UpdateEmbedderInfo(ctx, "mock", "1.0", 3)
	vecs, _ := s.Vectors(ctx)
	if vecs == nil {
		t.Errorf("Vectors() should return tree")
	}
	v, _ := s.Version(ctx)
	if v != 0 {
		t.Errorf("Version() should return 0")
	}
}
