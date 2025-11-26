package vector

import (
	"os"
	"testing"

	"github.com/sharedcode/sop"
)

func TestVectorStore(t *testing.T) {
	// Setup temporary directory
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize Database
	db := NewDatabase()
	db.SetStoragePath(tmpDir)
	db.SetReadMode(sop.ForReading) // Default

	index := db.Open("test_vectors")

	// Test Data
	id1 := "vec-1"
	vec1 := []float32{1.0, 0.0, 0.0}
	meta1 := map[string]any{"type": "A"}

	id2 := "vec-2"
	vec2 := []float32{0.0, 1.0, 0.0}
	meta2 := map[string]any{"type": "B"}

	// 1. Test Upsert
	if err := index.Upsert(id1, vec1, meta1); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}
	if err := index.Upsert(id2, vec2, meta2); err != nil {
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
	hits, err = index.Query(vec1, 10, map[string]any{"type": "B"})
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
	if err := doctor.Upsert("flu", []float32{1.0, 0.0, 0.0}, map[string]any{"desc": "flu"}); err != nil {
		t.Fatalf("Doctor upsert failed: %v", err)
	}

	// Upsert to mechanic
	if err := mechanic.Upsert("engine", []float32{0.0, 1.0, 0.0}, map[string]any{"desc": "engine"}); err != nil {
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
