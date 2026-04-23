package dynamic

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/inmemory"
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

	// Define B-Tree stores
	
	categories := inmemory.NewBtree[sop.UUID, *Category](true)
	vectors := inmemory.NewBtree[VectorKey, Vector](false)
	items := inmemory.NewBtree[sop.UUID, Item[string]](false)

	// Create DynamicStore
	s := NewStore[string](categories.Btree, vectors.Btree, items.Btree)
	// Single Item Upsert
	err := s.Upsert(ctx, ai.Item[string]{
		ID:      sop.NewUUID().String(),
		Vector:  []float32{0.1, 0.2, 0.3},
		Payload: "LLM Thought One",
	})
	if err != nil {
		t.Fatalf("Failed to upsert item: %v", err)
	}

	// Verify category creation
	count := categories.Count()
	if count != 1 {
		t.Fatalf("Expected 1 category, found %v", count)
	}

	// Double check vectors and items nodes
	vc := vectors.Count()
	if vc != 1 {
		t.Fatalf("Expected 1 vector, found %v", vc)
	}

	cc := items.Count()
	if cc != 1 {
		t.Fatalf("Expected 1 content, found %v", cc)
	}

	// Test a second item addition
	err = s.Upsert(ctx, ai.Item[string]{
		ID:      sop.NewUUID().String(),
		Vector:  []float32{0.15, 0.21, 0.33},
		Payload: "LLM Thought Two",
	})
	if err != nil {
		t.Fatalf("Failed to upsert second item: %v", err)
	}

	count = categories.Count()
	if count != 1 {
		t.Fatalf("Expected exactly 1 root category still, found %v", count)
	}

	vc = vectors.Count()
	if vc != 2 {
		t.Fatalf("Expected exactly 2 vectors, found %v", vc)
	}
}
