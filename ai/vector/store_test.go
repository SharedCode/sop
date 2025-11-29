package vector

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
)

func TestVectorStore(t *testing.T) {
	// Setup temporary directory
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize Database
	db := NewDatabase[map[string]any](ai.Standalone)
	db.SetStoragePath(tmpDir)
	// No need to set read mode explicitly, default is sop.NoCheck as once built, Vector DBs are read-only
	// in this Doctor/Nurse use case. And NoCheck avoids unnecessary overhead appropriate for single-writer,
	// write once, read forever scenarios.
	//db.SetReadMode(sop.ForReading) // Default

	index := db.Open("test_vectors")

	// Test Data
	id1 := "vec-1"
	vec1 := []float32{1.0, 0.0, 0.0}
	meta1 := map[string]any{"type": "A"}

	id2 := "vec-2"
	vec2 := []float32{0.0, 1.0, 0.0}
	meta2 := map[string]any{"type": "B"}

	// 1. Test Upsert
	if err := index.Upsert(ai.Item[map[string]any]{ID: id1, Vector: vec1, Payload: meta1}); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}
	if err := index.Upsert(ai.Item[map[string]any]{ID: id2, Vector: vec2, Payload: meta2}); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// 2. Test Get
	item, err := index.Get(id1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if item.ID != id1 {
		t.Errorf("Expected ID %s, got %s", id1, item.ID)
	}
	if len(item.Vector) != 3 {
		t.Errorf("Expected vector length 3, got %d", len(item.Vector))
	}

	// 3. Test Query (Exact Match)
	hits, err := index.Query(vec1, 1, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("Query returned no hits")
	}
	if hits[0].ID != id1 {
		t.Errorf("Expected top hit %s, got %s", id1, hits[0].ID)
	}
	if hits[0].Score < 0.99 {
		t.Errorf("Expected score ~1.0, got %f", hits[0].Score)
	}

	// 4. Test Query (Filter)
	hits, err = index.Query(vec1, 10, func(item map[string]any) bool {
		return item["type"] == "B"
	})
	if err != nil {
		t.Fatalf("Query with filter failed: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("Query with filter returned no hits")
	}
	if hits[0].ID != id2 {
		t.Errorf("Expected filtered hit %s, got %s", id2, hits[0].ID)
	}

	// 5. Test Delete
	if err := index.Delete(id1); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = index.Get(id1)
	if err == nil {
		t.Error("Expected error after delete, got nil")
	}

	// 6. Test Domains Isolation
	doctor := db.Open("doctor")
	mechanic := db.Open("mechanic")

	// Upsert to doctor
	if err := doctor.Upsert(ai.Item[map[string]any]{ID: "flu", Vector: []float32{1.0, 0.0, 0.0}, Payload: map[string]any{"desc": "flu"}}); err != nil {
		t.Fatalf("Doctor upsert failed: %v", err)
	}

	// Upsert to mechanic
	if err := mechanic.Upsert(ai.Item[map[string]any]{ID: "engine", Vector: []float32{0.0, 1.0, 0.0}, Payload: map[string]any{"desc": "engine"}}); err != nil {
		t.Fatalf("Mechanic upsert failed: %v", err)
	}

	// Query doctor
	docHits, err := doctor.Query([]float32{1.0, 0.0, 0.0}, 10, nil)
	if err != nil {
		t.Fatalf("Doctor query failed: %v", err)
	}
	if len(docHits) != 1 || docHits[0].ID != "flu" {
		t.Errorf("Doctor query expected 'flu', got %v", docHits)
	}

	// Query mechanic
	mechHits, err := mechanic.Query([]float32{0.0, 1.0, 0.0}, 10, nil)
	if err != nil {
		t.Fatalf("Mechanic query failed: %v", err)
	}
	if len(mechHits) != 1 || mechHits[0].ID != "engine" {
		t.Errorf("Mechanic query expected 'engine', got %v", mechHits)
	}
}

func TestDeleteUpdatesCentroidCount(t *testing.T) {
	// Setup temporary directory
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-delete-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize Database
	db := NewDatabase[map[string]any](ai.Standalone)
	db.SetStoragePath(tmpDir)

	index := db.Open("test_delete_count")
	// We need to cast to *domainIndex to access internal helpers
	di := index.(*domainIndex[map[string]any])

	id := "vec-1"
	vec := []float32{1.0, 0.0, 0.0}
	meta := map[string]any{"type": "A"}

	// 1. Upsert
	if err := index.Upsert(ai.Item[map[string]any]{ID: id, Vector: vec, Payload: meta}); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Helper to get centroid count
	getCentroidCount := func(centroidID int) int {
		storePath := filepath.Join(di.db.storagePath, di.name)
		trans, err := di.db.beginTransaction(sop.ForReading, storePath)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		defer trans.Rollback(di.db.ctx)

		version, err := di.getActiveVersion(di.db.ctx, trans)
		if err != nil {
			t.Fatalf("Failed to get active version: %v", err)
		}

		arch, err := OpenDomainStore(di.db.ctx, trans, version, sop.MediumData)
		if err != nil {
			t.Fatalf("Failed to open domain store: %v", err)
		}

		found, err := arch.Centroids.Find(di.db.ctx, centroidID, false)
		if err != nil {
			t.Fatalf("Failed to find centroid: %v", err)
		}
		if !found {
			t.Fatalf("Centroid %d not found", centroidID)
		}

		c, err := arch.Centroids.GetCurrentValue(di.db.ctx)
		if err != nil {
			t.Fatalf("Failed to get centroid value: %v", err)
		}
		return c.VectorCount
	}

	// 2. Verify Count is 1
	// The first centroid created is usually ID 1
	count := getCentroidCount(1)
	if count != 1 {
		t.Errorf("Expected centroid count 1, got %d", count)
	}

	// 3. Delete
	if err := index.Delete(id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 4. Verify Count is 0
	count = getCentroidCount(1)
	if count != 0 {
		t.Errorf("Expected centroid count 0, got %d", count)
	}
}
