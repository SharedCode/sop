package vector

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestCentroidCreation demonstrates how the Centroids Directory is built.
// In a real system, this is the "Training" phase.
// We use the K-Means algorithm to find the "natural centers" of the data.
func TestCentroidCreation(t *testing.T) {
	// 1. The Dataset
	// Imagine we have a bunch of 2D points (vectors) for simplicity.
	// We can see two clear clusters here:
	// Cluster A: around (1, 1)
	// Cluster B: around (10, 10)
	dataset := [][]float32{
		{1.0, 1.0}, {1.1, 1.1}, {0.9, 0.9}, {1.0, 1.2}, // Group A
		{10.0, 10.0}, {10.1, 10.1}, {9.9, 9.9}, {10.0, 9.8}, // Group B
	}

	fmt.Println("--- Step 1: Raw Data ---")
	for i, v := range dataset {
		fmt.Printf("Item %d: %v\n", i, v)
	}

	// 2. The Logic: K-Means Clustering
	// We want to find K=2 centroids that best represent this data.
	k := 2
	fmt.Printf("\n--- Step 2: Training (Finding %d Centroids) ---\n", k)

	centroids := trainKMeans(dataset, k, 10) // 10 iterations

	// 3. The Result: The Directory
	// These are the vectors that go into B-Tree #1.
	fmt.Println("\n--- Step 3: The Resulting Directory (B-Tree #1) ---")
	for id, vec := range centroids {
		fmt.Printf("CentroidID %d: %v\n", id, vec)
	}

	// Verification
	// We expect one centroid near [1, 1] and one near [10, 10]
	// (IDs are arbitrary in this simple implementation)
}

// trainKMeans implements a basic Lloyd's algorithm for K-Means.
// 1. Pick K random points as initial centroids.
// 2. Assign every item to closest centroid.
// 3. Move centroid to the average (mean) of its assigned items.
// 4. Repeat.
func trainKMeans(data [][]float32, k int, iterations int) map[int][]float32 {
	// Initialize Centroids randomly from data
	centroids := make(map[int][]float32)
	for i := 0; i < k; i++ {
		centroids[i] = data[rand.Intn(len(data))]
	}

	for iter := 0; iter < iterations; iter++ {
		// Buckets to hold items for each centroid
		buckets := make(map[int][][]float32)

		// Assignment Step
		for _, vec := range data {
			closestID, _ := findClosestCentroid(vec, centroids)
			buckets[closestID] = append(buckets[closestID], vec)
		}

		// Update Step (Move Centroid to Mean)
		for id, items := range buckets {
			if len(items) == 0 {
				continue
			}
			centroids[id] = calculateMean(items)
		}
		// fmt.Printf("Iteration %d complete.\n", iter+1)
	}
	return centroids
}

func calculateMean(vectors [][]float32) []float32 {
	if len(vectors) == 0 {
		return nil
	}
	dim := len(vectors[0])
	sum := make([]float32, dim)

	for _, v := range vectors {
		for i := 0; i < dim; i++ {
			sum[i] += v[i]
		}
	}

	mean := make([]float32, dim)
	count := float32(len(vectors))
	for i := 0; i < dim; i++ {
		mean[i] = sum[i] / count
	}
	return mean
}
