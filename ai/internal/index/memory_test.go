package index

import (
	"testing"

	"github.com/sharedcode/sop/ai/internal/port"
)

func TestMemoryIndexBasic(t *testing.T) {
	idx := NewMemory()
	v1 := []float32{1, 0, 0}
	v2 := []float32{0, 1, 0}
	if err := idx.Upsert("a", v1, map[string]any{"kind": "alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Upsert("b", v2, map[string]any{"kind": "beta"}); err != nil {
		t.Fatal(err)
	}
	q := []float32{1, 0, 0}
	hits, err := idx.Query(q, 1, map[string]any{"kind": "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].ID != "a" {
		t.Fatalf("expected hit a")
	}
}

func TestMemoryIndexDelete(t *testing.T) {
	idx := NewMemory()
	if err := idx.Upsert("x", []float32{1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := idx.Delete("x"); err != nil {
		t.Fatal(err)
	}
	hits, err := idx.Query([]float32{1}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("expected 0 hits, got %d", len(hits))
	}
	_ = port.Hit{} // ensure port imported usage
}
