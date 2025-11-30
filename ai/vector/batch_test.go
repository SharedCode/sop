package vector

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestUpsertBatchCentroidPopulation(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-batch-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db := NewDatabase[map[string]any](ai.Standalone)
	db.SetStoragePath(tmpDir)
	idx := db.Open(context.Background(), "test_batch")

	// Create a batch of items that form 2 clusters
	var items []ai.Item[map[string]any]
	// Cluster 1: around (0,0)
	for i := 0; i < 10; i++ {
		items = append(items, ai.Item[map[string]any]{
			ID:      fmt.Sprintf("c1-%d", i),
			Vector:  []float32{0.1 * float32(i), 0.1 * float32(i)},
			Payload: map[string]any{"cluster": 1},
		})
	}
	// Cluster 2: around (10,10)
	for i := 0; i < 10; i++ {
		items = append(items, ai.Item[map[string]any]{
			ID:      fmt.Sprintf("c2-%d", i),
			Vector:  []float32{10.0 + 0.1*float32(i), 10.0 + 0.1*float32(i)},
			Payload: map[string]any{"cluster": 2},
		})
	}

	// UpsertBatch should trigger K-Means (k ~ sqrt(20) = 4)
	if err := idx.UpsertBatch(context.Background(), items); err != nil {
		t.Fatalf("UpsertBatch failed: %v", err)
	}

	// Verify Centroids were created
	// We can't easily access centroids directly from here without opening the store manually,
	// but we can check if Query works effectively.

	// Query near Cluster 1
	hits, err := idx.Query(context.Background(), []float32{0, 0}, 5, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("Query returned no hits")
	}
	// Should be from Cluster 1
	clusterVal := hits[0].Payload["cluster"]
	if v, ok := clusterVal.(float64); ok {
		if int(v) != 1 {
			t.Errorf("Expected cluster 1, got %v", clusterVal)
		}
	} else if v, ok := clusterVal.(int); ok {
		if v != 1 {
			t.Errorf("Expected cluster 1, got %v", clusterVal)
		}
	} else {
		t.Errorf("Unexpected type for cluster: %T", clusterVal)
	}

	// Query near Cluster 2
	hits2, err := idx.Query(context.Background(), []float32{10, 10}, 5, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(hits2) == 0 {
		t.Fatal("Query returned no hits")
	}
	// Should be from Cluster 2
	clusterVal2 := hits2[0].Payload["cluster"]
	if v, ok := clusterVal2.(float64); ok {
		if int(v) != 2 {
			t.Errorf("Expected cluster 2, got %v", clusterVal2)
		}
	} else if v, ok := clusterVal2.(int); ok {
		if v != 2 {
			t.Errorf("Expected cluster 2, got %v", clusterVal2)
		}
	} else {
		t.Errorf("Unexpected type for cluster: %T", clusterVal2)
	}
}

/*
func TestIndexAllCentroidPopulation(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-indexall-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db := NewDatabase[map[string]any](Standalone)
	db.SetStoragePath(tmpDir)
	idx := db.Open("test_indexall")
	dIdx := idx.(*domainIndex[map[string]any])

	// Create items with distinct directions
	var items []ai.Item[map[string]any]
	for i := 0; i < 20; i++ {
		// Use [1, i] to ensure different angles
		items = append(items, ai.Item[map[string]any]{
			ID:      fmt.Sprintf("item-%d", i),
			Vector:  []float32{1.0, float32(i)},
			Payload: map[string]any{"val": i},
		})
	}

	// 1. UpsertContent (Stages to TempVectors)
	if err := dIdx.UpsertContent(items); err != nil {
		t.Fatalf("UpsertContent failed: %v", err)
	}

	// 2. IndexAll (Should train centroids and index)
	if err := dIdx.IndexAll(); err != nil {
		t.Fatalf("IndexAll failed: %v", err)
	}

	// 3. Verify
	// Query for [1, 0] which matches item-0 exactly
	hits, err := idx.Query([]float32{1.0, 0.0}, 1, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("Query returned no hits")
	}
	if hits[0].ID != "item-0" {
		t.Errorf("Expected item-0, got %s (Score: %f)", hits[0].ID, hits[0].Score)
	}
}
*/
