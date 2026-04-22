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
	if k.CentroidID.Compare(o.CentroidID) != 0 {
		return k.CentroidID.Compare(o.CentroidID)
	}
	if k.DistanceToCentroid < o.DistanceToCentroid {
		return -1
	} else if k.DistanceToCentroid > o.DistanceToCentroid {
		return 1
	}
	return k.VectorID.Compare(o.VectorID)
}

func (k ContentKey) Compare(other interface{}) int {
	o := other.(ContentKey)
	if k.VectorID.Compare(o.VectorID) != 0 {
		return k.VectorID.Compare(o.VectorID)
	}
	return k.PayloadID.Compare(o.PayloadID)
}

func TestDynamicStore_Upsert(t *testing.T) {
	ctx := context.Background()

	// Define B-Tree stores
	
	centroids := inmemory.NewBtree[sop.UUID, *Centroid](true)
	vectors := inmemory.NewBtree[VectorKey, Vector](false)
	content := inmemory.NewBtree[ContentKey, Payload[string]](false)

	// Create DynamicStore
	s := NewStore[string](centroids.Btree, vectors.Btree, content.Btree)
	// Single Payload Upsert
	err := s.Upsert(ctx, ai.Item[string]{
		ID:      sop.NewUUID().String(),
		Vector:  []float32{0.1, 0.2, 0.3},
		Payload: "LLM Thought One",
	})
	if err != nil {
		t.Fatalf("Failed to upsert item: %v", err)
	}

	// Verify centroid creation
	count := centroids.Count()
	if count != 1 {
		t.Fatalf("Expected 1 centroid, found %v", count)
	}

	// Double check vectors and content nodes
	vc := vectors.Count()
	if vc != 1 {
		t.Fatalf("Expected 1 vector, found %v", vc)
	}

	cc := content.Count()
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

	count = centroids.Count()
	if count != 1 {
		t.Fatalf("Expected exactly 1 root centroid still, found %v", count)
	}

	vc = vectors.Count()
	if vc != 2 {
		t.Fatalf("Expected exactly 2 vectors, found %v", vc)
	}
}
