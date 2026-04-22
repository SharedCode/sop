package dynamic

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

type centroidCandidate struct {
	centroid *Centroid
	dist     float32
}

// FindClosestCentroid finds the nearest single centroid to the target vector using Euclidean distance.
func FindClosestCentroid(vec []float32, centroids []*Centroid) (*Centroid, float32) {
	if len(centroids) == 0 {
		return nil, 0
	}
	var closest *Centroid
	minDist := float32(math.MaxFloat32)

	for _, c := range centroids {
		d := EuclideanDistance(vec, c.CenterVector)
		if d < minDist {
			minDist = d
			closest = c
		}
	}
	return closest, minDist
}

// FindClosestCentroids finds the nearest N centroids to the target vector using Euclidean distance.
func FindClosestCentroids(vec []float32, centroids []*Centroid, n int) []*Centroid {
	if len(centroids) == 0 {
		return nil
	}

	candidates := make([]centroidCandidate, 0, len(centroids))
	for _, c := range centroids {
		candidates = append(candidates, centroidCandidate{
			centroid: c,
			dist:     EuclideanDistance(vec, c.CenterVector),
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].dist < candidates[j].dist
	})

	if len(candidates) > n {
		candidates = candidates[:n]
	}

	result := make([]*Centroid, len(candidates))
	for i, c := range candidates {
		result[i] = c.centroid
	}
	return result
}
