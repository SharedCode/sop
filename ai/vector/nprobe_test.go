package vector

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
)

func TestNProbeAndFiltering(t *testing.T) {
	// 1. Setup
	// Setup temporary directory
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-nprobe-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db := NewDatabase[map[string]any](ai.Standalone)
	db.SetStoragePath(tmpDir)
	ctx := context.Background()
	idx := db.Open(ctx, "test_nprobe")

	// Inject Centroids
	storePath := filepath.Join(tmpDir, "test_nprobe")
	trans, err := db.beginTransaction(ctx, sop.ForWriting, storePath)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	arch, err := OpenDomainStore(ctx, trans, "test_nprobe", 0, sop.MediumData)
	if err != nil {
		t.Fatalf("Failed to open domain store: %v", err)
	}
	arch.Centroids.Add(ctx, 1, ai.Centroid{Vector: []float32{0, 0}})
	arch.Centroids.Add(ctx, 2, ai.Centroid{Vector: []float32{2, 2}})   // Close to 1
	arch.Centroids.Add(ctx, 3, ai.Centroid{Vector: []float32{10, 10}}) // Far
	trans.Commit(ctx)

	// 2. Upsert Vectors
	// Vec A: Near Centroid 1
	idx.Upsert(ctx, ai.Item[map[string]any]{ID: "vecA", Vector: []float32{0.1, 0.1}, Payload: map[string]any{"type": "fruit", "name": "apple"}})
	// Vec B: Near Centroid 2
	idx.Upsert(ctx, ai.Item[map[string]any]{ID: "vecB", Vector: []float32{2.1, 2.1}, Payload: map[string]any{"type": "fruit", "name": "banana"}})
	// Vec C: Near Centroid 2 but different type
	idx.Upsert(ctx, ai.Item[map[string]any]{ID: "vecC", Vector: []float32{2.2, 2.2}, Payload: map[string]any{"type": "vegetable", "name": "carrot"}})

	// 3. Test nprobe
	// Query at [1, 1]. Dist to C1 ~1.4, Dist to C2 ~1.4.
	// With nprobe=2, it should scan both C1 and C2.
	// We expect to find vecA and vecB.

	results, err := idx.Query(ctx, []float32{1, 1}, 5, nil)
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
	filters := func(item map[string]any) bool {
		return item["type"] == "fruit"
	}
	results2, err := idx.Query(ctx, []float32{2, 2}, 5, filters)
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
