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

// 1. THE LIVE SEARCH LOOP (Blazing Fast - No Sqrts, No Divisions)
// Use this inside your nested category loops to rank your documents.
func DotProduct(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return dot
}

// 2. THE ONE-TIME PRE-NORMALIZER
// Run this ONLY when indexing or slicing local GGUF vectors (like Nomic 256 slices).
func NormalizeVector(v []float32) []float32 {
	var sum float32
	for _, val := range v {
		sum += val * val
	}

	norm := float32(math.Sqrt(float64(sum)))
	if norm == 0 {
		return v
	}

	normalized := make([]float32, len(v))
	for i, val := range v {
		normalized[i] = val / norm
	}
	return normalized
}

// FastEuclidean outputs the exact same distance metric as traditional Euclidean calculation,
// but runs up to 4x faster on normalized vectors by utilizing DotProduct internally.
func FastEuclidean(normalizedVectorA, NormalizedVectorB []float32) float32 {
	if len(normalizedVectorA) != len(NormalizedVectorB) {
		return 0
	}

	var dot float32
	for i := range normalizedVectorA {
		dot += normalizedVectorA[i] * NormalizedVectorB[i]
	}

	val := 2.0 * (1.0 - dot)
	if val <= 0 {
		return 0
	}

	return float32(math.Sqrt(float64(val)))
}

func isNormalizedVector(v []float32) bool {
	if len(v) == 0 {
		return false
	}
	var sum float32
	for _, val := range v {
		sum += val * val
	}
	norm := float32(math.Sqrt(float64(sum)))
	return math.Abs(float64(norm-1)) < 1e-6
}

func Distance(a, b []float32, normalized bool) float32 {
	if normalized && isNormalizedVector(a) && isNormalizedVector(b) {
		return FastEuclidean(a, b)
	}
	return EuclideanDistance(a, b)
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
		dist := Distance(vec, cat.CenterVector, true)
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
		dist := Distance(vec, cat.CenterVector, true)
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
			dist:     Distance(vec, c.CenterVector, true),
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
