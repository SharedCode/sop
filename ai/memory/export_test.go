package memory

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/inmemory"
)

func TestExportImportJSON(t *testing.T) {
	ctx := context.Background()
	llm := &MockLLM{}
	embedder := &MockEmbedder{}
	// Create a new Knowledge Base
	cats := inmemory.NewBtree[sop.UUID, *Category](true)
	vecs := inmemory.NewBtree[VectorKey, Vector](true)
	items := inmemory.NewBtree[sop.UUID, Item[string]](true)
	st := NewStore[string](cats.Btree, vecs.Btree, items.Btree).(*store[string])
	st.SetTextIndex(&MockTextIndex{})
	kb := &KnowledgeBase[string]{
		BaseKnowledgeBase: BaseKnowledgeBase[string]{Store: st},
		Manager:           NewMemoryManager[string](st, llm, embedder),
	}

	// Ingest some thoughts
	thoughts := []Thought[string]{
		{Summaries: []string{"Apple is a fruit"}, Category: "Food", Data: "apple_data", Vectors: [][]float32{{0.1, 0.2}}},
		{Summaries: []string{"Car is a vehicle"}, Category: "Vehicles", Data: "car_data", Vectors: [][]float32{{0.9, 0.8}}},
	}
	err := kb.IngestThoughts(ctx, thoughts, "test")
	if err != nil {
		t.Fatalf("IngestThoughts failed: %v", err)
	}

	// Export to JSON
	var buf bytes.Buffer
	err = kb.ExportJSON(ctx, &buf)
	if err != nil {
		t.Fatalf("ExportJSON failed: %v", err)
	}

	jsonStr := buf.String()
	if !strings.Contains(jsonStr, "Food") || !strings.Contains(jsonStr, "Vehicles") {
		t.Errorf("ExportJSON did not contain expected categories: %v", jsonStr)
	}

	if !strings.Contains(jsonStr, "apple_data") || !strings.Contains(jsonStr, "car_data") {
		t.Errorf("ExportJSON did not contain expected data: %v", jsonStr)
	}

	// Create a new empty KnowledgeBase and Import the JSON
	cats2 := inmemory.NewBtree[sop.UUID, *Category](true)
	vecs2 := inmemory.NewBtree[VectorKey, Vector](true)
	items2 := inmemory.NewBtree[sop.UUID, Item[string]](true)
	st2 := NewStore[string](cats2.Btree, vecs2.Btree, items2.Btree).(*store[string])
	st2.SetTextIndex(&MockTextIndex{})
	kb2 := &KnowledgeBase[string]{
		BaseKnowledgeBase: BaseKnowledgeBase[string]{Store: st2},
		Manager:           NewMemoryManager[string](st2, llm, embedder),
	}

	err = kb2.ImportJSON(ctx, &buf, "test")
	if err != nil {
		t.Fatalf("ImportJSON failed: %v", err)
	}

	// Verify imported data
	c2, err := st2.Count(ctx)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if c2 != 2 {
		t.Errorf("Expected 2 items in imported store, got %d", c2)
	}
}
