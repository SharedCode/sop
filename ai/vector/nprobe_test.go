package vector

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
)

func TestNProbeAndFiltering(t *testing.T) {
	// 1. Setup
	// Setup temporary directory
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-nprobe-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db := NewDatabase()
	db.SetStoragePath(tmpDir)
	ctx := context.Background()
	idx := db.Open("test_nprobe")

	// Inject Centroids
	trans, err := db.beginTransaction(sop.ForWriting)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	arch, err := OpenDomainStore(ctx, trans, tmpDir, "test_nprobe")
	if err != nil {
		t.Fatalf("Failed to open domain store: %v", err)
	}
	arch.Centroids.Add(ctx, 1, []float32{0, 0})
	arch.Centroids.Add(ctx, 2, []float32{2, 2})   // Close to 1
	arch.Centroids.Add(ctx, 3, []float32{10, 10}) // Far
	trans.Commit(ctx)

	// 2. Upsert Vectors
	// Vec A: Near Centroid 1
	idx.Upsert("vecA", []float32{0.1, 0.1}, map[string]any{"type": "fruit", "name": "apple"})
	// Vec B: Near Centroid 2
	idx.Upsert("vecB", []float32{2.1, 2.1}, map[string]any{"type": "fruit", "name": "banana"})
	// Vec C: Near Centroid 2 but different type
	idx.Upsert("vecC", []float32{2.2, 2.2}, map[string]any{"type": "vegetable", "name": "carrot"})

	// 3. Test nprobe
	// Query at [1, 1]. Dist to C1 ~1.4, Dist to C2 ~1.4.
	// With nprobe=2, it should scan both C1 and C2.
	// We expect to find vecA and vecB.

	results, err := idx.Query([]float32{1, 1}, 5, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	foundA := false
	foundB := false
	for _, r := range results {
		if r.ID == "vecA" {
			foundA = true
		}
		if r.ID == "vecB" {
			foundB = true
		}
	}
	if !foundA || !foundB {
		t.Errorf("Expected to find vecA and vecB with nprobe. Got: %v", results)
	}

	// 4. Test Filtering
	// Query near Centroid 2, but filter for "fruit".
	// Should find vecB, but NOT vecC (vegetable).
	filters := map[string]any{"type": "fruit"}
	results2, err := idx.Query([]float32{2, 2}, 5, filters)
	if err != nil {
		t.Fatalf("Filtered Query failed: %v", err)
	}

	foundB = false
	foundC := false
	for _, r := range results2 {
		if r.ID == "vecB" {
			foundB = true
		}
		if r.ID == "vecC" {
			foundC = true
		}
	}
	if !foundB {
		t.Error("Expected to find vecB (fruit)")
	}
	if foundC {
		t.Error("Expected NOT to find vecC (vegetable)")
	}
}
