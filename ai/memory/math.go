package memory

import (
	"math"
	"sort"
)

// EuclideanDistance computes the Euclidean distance between two vectors.
func EuclideanDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

// CosineSimilarity computes the mathematical cosine similarity between two vectors.
func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float32
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (float32(math.Sqrt(float64(na))) * float32(math.Sqrt(float64(nb))))
}

type categoryCandidate struct {
	category *Category
	dist     float32
}

// FindClosestCategory finds the nearest single category to the target vector using Euclidean distance.
func FindClosestCategoryFromPtrs(vec []float32, categories []*Category) (*Category, float32) {
	if len(categories) == 0 {
		return nil, 0
	}
	var closest *Category
	minDist := float32(math.MaxFloat32)

	for _, cat := range categories {
		if len(cat.CenterVector) == 0 {
			continue
		}
		dist := EuclideanDistance(vec, cat.CenterVector)
		if dist < minDist {
			minDist = dist
			closest = cat
		}
	}
	return closest, minDist
}

func FindClosestCategory(vec []float32, categories []Category) (*Category, float32) {
	if len(categories) == 0 {
		return nil, 0
	}
	var closest *Category
	minDist := float32(math.MaxFloat32)

	for i := range categories {
		cat := &categories[i]
		if len(cat.CenterVector) == 0 {
			continue
		}
		dist := EuclideanDistance(vec, cat.CenterVector)
		if dist < minDist {
			minDist = dist
			closest = cat
		}
	}
	return closest, minDist
}

// FindClosestCategories finds the nearest N categories to the target vector using Euclidean distance.
func FindClosestCategories(vec []float32, categories []*Category, n int) []*Category {
	if len(categories) == 0 {
		return nil
	}

	candidates := make([]categoryCandidate, 0, len(categories))
	for _, c := range categories {
		candidates = append(candidates, categoryCandidate{
			category: c,
			dist:     EuclideanDistance(vec, c.CenterVector),
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].dist < candidates[j].dist
	})

	if len(candidates) > n {
		candidates = candidates[:n]
	}

	result := make([]*Category, len(candidates))
	for i, c := range candidates {
		result[i] = c.category
	}
	return result
}
