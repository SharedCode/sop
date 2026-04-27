package memory

import (
	"testing"

	"github.com/sharedcode/sop"
)

func TestCosineSimilarity(t *testing.T) {
	v1 := []float32{1, 0, 0}
	v2 := []float32{0, 1, 0}

	// orthogonal
	if CosineSimilarity(v1, v2) != 0 {
		t.Errorf("expected 0")
	}

	// identity
	if CosineSimilarity(v1, v1) != 1 {
		t.Errorf("expected 1")
	}

	// invalid lengths
	if CosineSimilarity(v1, []float32{1}) != 0 {
		t.Errorf("expected 0")
	}
	// zeros
	if CosineSimilarity([]float32{0, 0, 0}, v1) != 0 {
		t.Errorf("expected 0")
	}
}

func TestFindClosestCategories(t *testing.T) {
	categories := []*Category{
		{ID: sop.NewUUID(), CenterVector: []float32{1, 0, 0}},
		{ID: sop.NewUUID(), CenterVector: []float32{0, 1, 0}},
		{ID: sop.NewUUID(), CenterVector: []float32{0, 0, 1}},
	}

	c := FindClosestCategories([]float32{1, 0, 0}, categories, 2)
	if len(c) != 2 {
		t.Errorf("expected 2 elements")
	}

	cZero := FindClosestCategories([]float32{1, 0, 0}, []*Category{}, 2)
	if len(cZero) != 0 {
		t.Errorf("expected 0 elements")
	}
}
