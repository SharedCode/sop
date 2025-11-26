package vector

import (
	"context"
	"sort"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/inredfs"
)

// Architecture demonstrates the 3-B-Tree layout for optimal performance.
// It uses three separate B-Trees to optimize for different access patterns:
// 1. Directory (Centroids): Fast lookup of centroids for narrowing down the search space.
// 2. Library (Vectors): Fast scanning of vectors within a centroid bucket. Kept compact for cache efficiency.
// 3. Content (Data): Storage of the actual item data (JSON/Document), retrieved only for the final results.
// 4. Lookup (Int -> ID): Maps integer IDs to string IDs, enabling efficient random sampling.
type Architecture struct {
	// Centroids stores the centroid vectors.
	// Key: CentroidID (int) -> Value: CentroidVector ([]float32)
	// Name: "{domain}_centroids"
	Centroids btree.BtreeInterface[int, []float32]

	// Vectors stores the item vectors, indexed by centroid and distance.
	// Key: CompositeKey{CentroidID, Distance, ItemID} -> Value: ItemVector ([]float32)
	// Name: "{domain}_vectors"
	// Optimization: Keeping this small allows more vectors to fit in CPU cache during scanning.
	Vectors btree.BtreeInterface[CompositeKey, []float32]

	// Content stores the actual item data.
	// Key: ItemID (string) -> Value: Document/JSON (string)
	// Name: "{domain}_content"
	Content btree.BtreeInterface[string, string]
}

// OpenDomainStore initializes the 3 B-Trees for the vertical.
func OpenDomainStore(ctx context.Context, trans sop.Transaction) (*Architecture, error) {
	// 1. Open Centroids Store
	centroids, err := inredfs.NewBtree[int, []float32](ctx, sop.StoreOptions{
		Name: "centroids",
	}, trans, func(a, b int) int { return a - b })
	if err != nil {
		return nil, err
	}

	// 2. Open Vectors Store (The "Library")
	vectors, err := inredfs.NewBtree[CompositeKey, []float32](ctx, sop.StoreOptions{
		Name: "vectors",
	}, trans, compositeKeyComparer)
	if err != nil {
		return nil, err
	}

	// 3. Open Content Store
	contentComparer := func(a, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	}
	content, err := inredfs.NewBtree[string, string](ctx, sop.StoreOptions{
		Name: "content",
	}, trans, contentComparer)
	if err != nil {
		return nil, err
	}

	return &Architecture{
		Centroids: centroids,
		Vectors:   vectors,
		Content:   content,
	}, nil
}

// Add demonstrates how to write to all 3 stores transactionally.
func (a *Architecture) Add(ctx context.Context, id string, vector []float32, data string) error {
	// Step 1: Assign to a Centroid (Logic omitted for brevity)
	centroidID := 1 // assume we found the best centroid

	// Step 2: Write to Vector Store (The Index)
	// This is lightweight because we only store the vector, not the data.
	vecKey := CompositeKey{CentroidID: centroidID, DistanceToCentroid: 0.0, ItemID: id}
	if _, err := a.Vectors.Add(ctx, vecKey, vector); err != nil {
		return err
	}

	// Step 3: Write to Content Store (The Data)
	if _, err := a.Content.Add(ctx, id, data); err != nil {
		return err
	}

	return nil
}

// Search demonstrates the optimized retrieval pipeline.
func (a *Architecture) Search(ctx context.Context, query []float32, k int) ([]string, error) {
	// Step 1: Scan Centroids to find the best bucket (e.g., ID 1)
	targetCentroid := 1

	// Step 2: Scan ONLY the Vectors in that bucket
	// We don't load the heavy content here, making this very fast.
	startKey := CompositeKey{CentroidID: targetCentroid, DistanceToCentroid: -1.0, ItemID: ""}

	type candidate struct {
		ID    string
		Score float32
	}
	var candidates []candidate

	// Find positions the cursor. If exact match not found, it positions at nearest.
	if _, err := a.Vectors.Find(ctx, startKey, true); err != nil {
		return nil, err
	}

	for {
		item, err := a.Vectors.GetCurrentItem(ctx)
		if err != nil {
			return nil, err
		}
		if item.ID.IsNil() {
			break
		}

		key := item.Key
		// If we are before the startKey (e.g. previous centroid), move next
		if compositeKeyComparer(key, startKey) < 0 {
			if ok, err := a.Vectors.Next(ctx); !ok || err != nil {
				break
			}
			continue
		}

		if key.CentroidID != targetCentroid {
			break // Left the bucket
		}

		// Compute Score
		itemVec := *item.Value
		score := cosine(query, itemVec)
		candidates = append(candidates, candidate{ID: key.ItemID, Score: score})

		if ok, _ := a.Vectors.Next(ctx); !ok {
			break
		}
	}

	// Sort by Score
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })

	// Top K
	if len(candidates) > k {
		candidates = candidates[:k]
	}

	// Step 3: Fetch Content ONLY for the winners
	var results []string
	for _, c := range candidates {
		found, err := a.Content.Find(ctx, c.ID, false)
		if err != nil {
			return nil, err
		}
		if found {
			data, _ := a.Content.GetCurrentValue(ctx)
			results = append(results, data)
		}
	}

	return results, nil
}
