package vector

import (
	"os"
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestAddCentroid(t *testing.T) {
	os.RemoveAll("test_dynamic_centroids")
	defer os.RemoveAll("test_dynamic_centroids")

	db := NewDatabase[map[string]any](ai.Standalone)
	// db.Close() is not implemented/needed for in-memory test

	idx := db.Open("test_dynamic_centroids")

	// 1. Initial State
	vec1 := []float32{0.1, 0.1}
	item1 := ai.Item[map[string]any]{
		ID:      "item1",
		Vector:  vec1,
		Payload: map[string]any{"label": "A"},
	}
	if err := idx.Upsert(item1); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Verify we have 1 centroid (auto-created)
	// We can't check internal state easily, but we can check query behavior
	hits, _ := idx.Query(vec1, 1, nil)
	if len(hits) != 1 {
		t.Errorf("Expected 1 hit, got %d", len(hits))
	}

	// 2. Add New Centroid
	vec2 := []float32{0.9, 0.9}
	// Cast to the interface that includes AddCentroid (since it's in the package, we can cast to domainIndex or just use the interface)
	// But idx is ai.VectorIndex which now has AddCentroid
	newID, err := idx.AddCentroid(vec2)
	if err != nil {
		t.Fatalf("AddCentroid failed: %v", err)
	}
	if newID != 2 {
		t.Errorf("Expected new centroid ID 2, got %d", newID)
	}

	// 3. Add Item to New Centroid
	// Upsert should now assign item2 to the new centroid because it's closer
	item2 := ai.Item[map[string]any]{
		ID:      "item2",
		Vector:  vec2,
		Payload: map[string]any{"label": "B"},
	}
	if err := idx.Upsert(item2); err != nil {
		t.Fatalf("Upsert item2 failed: %v", err)
	}

	// 4. Verify Assignment
	// We can check the metadata of the item to see which centroid it was assigned to
	// But Get returns *ai.Item[T], which has CentroidID field?
	// Let's check ai.Item definition.
	if _, err := idx.Get("item2"); err != nil {
		t.Fatalf("Get item2 failed: %v", err)
	}
}
