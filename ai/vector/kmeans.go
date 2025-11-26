package vector

import (
	"math/rand"
	"time"

	"github.com/sharedcode/sop/ai"
)

// ComputeCentroids performs K-Means clustering on the given items to find k centroids.
// It uses K-Means++ for initialization to improve convergence and cluster quality.
// The function iterates up to a maximum number of times (20) or until convergence.
func ComputeCentroids(items []ai.Item, k int) (map[int][]float32, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if k > len(items) {
		k = len(items)
	}

	// Initialize centroids using K-Means++
	// This is "smarter" than random initialization as it spreads centroids apart,
	// increasing the chance of capturing distinct categories (e.g. viral vs bacterial).
	centroids := initCentroidsKMeansPlusPlus(items, k)

	// Iterations
	maxIter := 20
	for iter := 0; iter < maxIter; iter++ {
		// Assign items to clusters
		clusters := make(map[int][][]float32)
		for _, item := range items {
			if len(item.Vector) == 0 {
				continue
			}
			cid, _ := findClosestCentroid(item.Vector, centroids)
			clusters[cid] = append(clusters[cid], item.Vector)
		}

		// Update centroids
		converged := true
		for cid, vecs := range clusters {
			if len(vecs) == 0 {
				continue
			}
			newCentroid := meanVector(vecs)
			if euclideanDistance(centroids[cid], newCentroid) > 1e-5 {
				converged = false
			}
			centroids[cid] = newCentroid
		}

		if converged {
			break
		}
	}

	return centroids, nil
}

func meanVector(vecs [][]float32) []float32 {
	if len(vecs) == 0 {
		return nil
	}
	dim := len(vecs[0])
	sum := make([]float32, dim)
	for _, v := range vecs {
		for i := 0; i < dim; i++ {
			sum[i] += v[i]
		}
	}
	for i := 0; i < dim; i++ {
		sum[i] /= float32(len(vecs))
	}
	return sum
}

func initCentroidsKMeansPlusPlus(items []ai.Item, k int) map[int][]float32 {
	rand.Seed(time.Now().UnixNano())
	centroids := make(map[int][]float32)
	if len(items) == 0 {
		return centroids
	}

	// 1. Choose first centroid randomly
	firstIdx := rand.Intn(len(items))
	centroids[1] = items[firstIdx].Vector

	// 2. Choose remaining k-1 centroids
	for i := 2; i <= k; i++ {
		// Calculate squared distances to nearest existing centroid
		distSq := make([]float64, len(items))
		var sumDistSq float64

		for j, item := range items {
			if len(item.Vector) == 0 {
				continue
			}
			_, d := findClosestCentroid(item.Vector, centroids)
			dSq := float64(d * d)
			distSq[j] = dSq
			sumDistSq += dSq
		}

		// Select next centroid with probability proportional to D^2
		r := rand.Float64() * sumDistSq
		var cumulative float64
		selectedIdx := -1

		for j, dSq := range distSq {
			cumulative += dSq
			if cumulative >= r {
				selectedIdx = j
				break
			}
		}
		// Fallback for floating point precision issues
		if selectedIdx == -1 {
			selectedIdx = len(items) - 1
		}

		centroids[i] = items[selectedIdx].Vector
	}

	return centroids
}
