package dynamic

import (
	"context"
	"strings"
	"testing"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/inmemory"
)

type ReflectingLLM struct{}

func (m *ReflectingLLM) Name() string { return "ReflectingLLM" }
func (m *ReflectingLLM) EstimateCost(inTokens, outTokens int) float64 { return 0.0 }
func (m *ReflectingLLM) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	if strings.Contains(prompt, "fail_sub") {
		return ai.GenOutput{}, fmt.Errorf("llm fail sub")
	}
	if strings.Contains(prompt, "empty_sub") {
		return ai.GenOutput{Text: " "}, nil
	}
	return ai.GenOutput{Text: "SubCat1, SubCat2"}, nil
}

func TestReflectAndReassociate_Success(t *testing.T) {
	ctx := context.Background()

	catTree := inmemory.NewBtree[sop.UUID, *Category](true)
	vecTree := inmemory.NewBtree[VectorKey, Vector](true)
	itemTree := inmemory.NewBtree[sop.UUID, Item[string]](true)
	store := NewStore[string](catTree.Btree, vecTree.Btree, itemTree.Btree)

	mgr := NewMemoryManager[string](store, &ReflectingLLM{}, &MockEmbedder{})

	// Pre-create anchor
	anchorID, _ := store.AddCategory(ctx, &Category{
		ID:           sop.NewUUID(),
		Name:         "BroadCategory",
		CenterVector: []float32{0.5, 0.6, 0.7},
		ItemCount:    1,
	})

	// Pre-create item mapped to anchor
	itemID := sop.NewUUID()
	oldKey := VectorKey{
		CategoryID:         anchorID,
		DistanceToCategory: EuclideanDistance([]float32{0.1, 0.2, 0.3}, []float32{0.5, 0.6, 0.7}),
		VectorID:           sop.NewUUID(),
	}
	
	v := Vector{
		ID:         oldKey.VectorID,
		Data:       []float32{0.1, 0.2, 0.3}, // Matches MockEmbedder output perfectly
		ItemID:     itemID,
		CategoryID: anchorID,
	}
	
	itemsTree, _ := store.Items(ctx)
	itemsTree.Add(ctx, itemID, Item[string]{
		ID:         itemID,
		CategoryID: anchorID,
		Positions:  []VectorKey{oldKey},
	})
	
	vectorsTree, _ := store.Vectors(ctx)
	vectorsTree.Add(ctx, oldKey, v)
	
	catsTree, _ := store.Categories(ctx)
	catsTree.Find(ctx, anchorID, false)
	anchor, _ := catsTree.GetCurrentValue(ctx)

	err := mgr.reflectAndReassociate(ctx, anchor)
	if err != nil {
		t.Fatalf("Expected nil, got %v", err)
	}
	
	// Check if Vector moved 
	vectorsTree.First(ctx)
	vk := vectorsTree.GetCurrentKey()
	// Because it's perfectly matching MockEmbedder out on SubCat, Distance is 0 
	if vk.Key.CategoryID == anchorID {
		t.Errorf("Vector didn't move")
	}
	
	// Check item updated
	itemsTree.First(ctx)
	item, _ := itemsTree.GetCurrentValue(ctx)
	if item.CategoryID == anchorID {
		t.Errorf("Item category wasn't updated")
	}
	if len(item.Positions) > 0 && item.Positions[0].CategoryID == anchorID {
		t.Errorf("Item position wasn't updated")
	}
}

func TestReflectAndReassociate_NoVectors(t *testing.T) {
	ctx := context.Background()

	catTree := inmemory.NewBtree[sop.UUID, *Category](true)
	vecTree := inmemory.NewBtree[VectorKey, Vector](true)
	itemTree := inmemory.NewBtree[sop.UUID, Item[string]](true)
	store := NewStore[string](catTree.Btree, vecTree.Btree, itemTree.Btree)

	mgr := NewMemoryManager[string](store, &ReflectingLLM{}, &MockEmbedder{})
	anchor := &Category{ID: sop.NewUUID()}
	
	err := mgr.reflectAndReassociate(ctx, anchor)
	if err != nil {
		t.Fatalf("Expected nil for no vectors")
	}
}

func TestReflectAndReassociate_LLMFails(t *testing.T) {
	ctx := context.Background()

	catTree := inmemory.NewBtree[sop.UUID, *Category](true)
	vecTree := inmemory.NewBtree[VectorKey, Vector](true)
	itemTree := inmemory.NewBtree[sop.UUID, Item[string]](true)
	store := NewStore[string](catTree.Btree, vecTree.Btree, itemTree.Btree)

	mgr := NewMemoryManager[string](store, &ReflectingLLM{}, &MockEmbedder{})
	anchor := &Category{ID: sop.NewUUID(), Name: "fail_sub"}
	
	vtree, _ := store.Vectors(ctx)
	vtree.Add(ctx, VectorKey{CategoryID: anchor.ID}, Vector{})
	
	err := mgr.reflectAndReassociate(ctx, anchor)
	if err == nil {
		t.Fatalf("Expected LLM failure")
	}
}

func TestReflectAndReassociate_EmptyLLM(t *testing.T) {
	ctx := context.Background()

	catTree := inmemory.NewBtree[sop.UUID, *Category](true)
	vecTree := inmemory.NewBtree[VectorKey, Vector](true)
	itemTree := inmemory.NewBtree[sop.UUID, Item[string]](true)
	store := NewStore[string](catTree.Btree, vecTree.Btree, itemTree.Btree)

	mgr := NewMemoryManager[string](store, &ReflectingLLM{}, &MockEmbedder{})
	anchor := &Category{ID: sop.NewUUID(), Name: "empty_sub"}
	
	vtree, _ := store.Vectors(ctx)
	vtree.Add(ctx, VectorKey{CategoryID: anchor.ID}, Vector{})
	
	err := mgr.reflectAndReassociate(ctx, anchor)
	if err != nil {
		t.Fatalf("Expected nil when LLM returns nothing actionable")
	}
}
