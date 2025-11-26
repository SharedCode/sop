package vector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/inredfs"
)

// Database is a persistent vector database manager backed by SOP B-Trees.
// It manages the storage and retrieval of vectors and their associated metadata.
// It supports transactions and caching for performance.
type Database struct {
	ctx         context.Context
	cache       sop.Cache
	readMode    sop.TransactionMode
	storagePath string
}

// NewDatabase creates a new vector database manager.
// It initializes the cache (InMemory by default) and sets default transaction modes.
func NewDatabase() *Database {
	// Use InMemoryCache by default for standalone AI apps
	sop.SetCacheFactory(sop.InMemory)
	return &Database{
		ctx:         context.Background(),
		cache:       cache.NewInMemoryCache(),
		readMode:    sop.NoCheck,
		storagePath: "",
	}
}

// SetStoragePath configures the file system path for data persistence.
func (d *Database) SetStoragePath(path string) {
	d.storagePath = path
}

// SetReadMode configures the transaction mode for Query operations.
func (d *Database) SetReadMode(mode sop.TransactionMode) {
	d.readMode = mode
}

// Open returns an Index for the specified domain.
func (d *Database) Open(domain string) ai.VectorIndex {
	return &domainIndex{
		db:   d,
		name: domain,
	}
}

// domainIndex implements the Index interface for a specific domain using the 3-table layout.
type domainIndex struct {
	db   *Database
	name string
}

// Upsert adds or updates a vector in the store.
// It handles the full lifecycle:
// 1. Cleaning up any existing entry for the ID to prevent ghosts.
// 2. Assigning the vector to the closest centroid.
// 3. Updating the Vector Index (Library).
// 4. Updating the Content Store with metadata (including centroid info).
func (di *domainIndex) Upsert(id string, vec []float32, meta map[string]any) error {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(sop.ForWriting, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(di.db.ctx)

	arch, err := OpenDomainStore(di.db.ctx, trans, storePath)
	if err != nil {
		return err
	}

	// 0. Cleanup Old Entry (if exists) to prevent "ghost" vectors
	found, err := arch.Content.Find(di.db.ctx, id, false)
	if err != nil {
		return err
	}
	if found {
		// Retrieve old metadata to find old vector location
		oldJson, err := arch.Content.GetCurrentValue(di.db.ctx)
		if err != nil {
			return err
		}
		var oldMeta map[string]any
		if err := json.Unmarshal([]byte(oldJson), &oldMeta); err == nil {
			oldCid := 1
			oldDist := float32(0.0)
			if v, ok := oldMeta["_centroid_id"].(float64); ok {
				oldCid = int(v)
			}
			if v, ok := oldMeta["_distance"].(float64); ok {
				oldDist = float32(v)
			}

			// Remove old vector
			oldKey := CompositeKey{CentroidID: oldCid, DistanceToCentroid: oldDist, ItemID: id}
			if foundVec, _ := arch.Vectors.Find(di.db.ctx, oldKey, false); foundVec {
				if _, err := arch.Vectors.RemoveCurrentItem(di.db.ctx); err != nil {
					return err
				}
			}
		}
		// Remove old content to ensure clean insert
		if _, err := arch.Content.RemoveCurrentItem(di.db.ctx); err != nil {
			return err
		}
	}

	// 1. Assign Centroid
	// Load centroids to find the closest one
	centroids, err := loadCentroids(di.db.ctx, arch)
	if err != nil {
		return err
	}

	// If no centroids exist, create one (Auto-Initialization for MVP)
	if len(centroids) == 0 {
		// Create Centroid 1 at the position of this first vector
		// In a real system, we'd run K-Means on a batch.
		centroids[1] = vec
		if _, err := arch.Centroids.Add(di.db.ctx, 1, vec); err != nil {
			return err
		}
	}

	centroidID, dist := findClosestCentroid(vec, centroids)

	// 2. Update Vector Index
	// We use CompositeKey to allow multiple items per centroid
	key := CompositeKey{CentroidID: centroidID, DistanceToCentroid: dist, ItemID: id}
	if _, err := arch.Vectors.Add(di.db.ctx, key, vec); err != nil {
		return err
	}

	// 3. Update Content Store
	meta["_centroid_id"] = centroidID
	meta["_distance"] = dist
	// Serialize metadata to JSON
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal meta: %w", err)
	}
	if _, err := arch.Content.Add(di.db.ctx, id, string(data)); err != nil {
		return err
	}

	return trans.Commit(di.db.ctx)
}

// UpsertBatch adds or updates multiple vectors in a single transaction.
// This is more efficient than individual Upserts as it loads centroids once
// and commits all changes atomically.
func (di *domainIndex) UpsertBatch(items []ai.Item) error {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(sop.ForWriting, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(di.db.ctx)

	arch, err := OpenDomainStore(di.db.ctx, trans, storePath)
	if err != nil {
		return err
	}

	// Load centroids once for the batch
	centroids, err := loadCentroids(di.db.ctx, arch)
	if err != nil {
		return err
	}

	// Auto-init centroids if needed (handle first batch case)
	if len(centroids) == 0 && len(items) > 0 {
		// Create Centroid 1 at the position of this first vector
		// In a real system, we'd run K-Means on a batch.
		centroids[1] = items[0].Vector
		if _, err := arch.Centroids.Add(di.db.ctx, 1, items[0].Vector); err != nil {
			return err
		}
	}

	for _, item := range items {
		id := item.ID
		vec := item.Vector
		meta := item.Meta

		// 0. Cleanup Old Entry (if exists) to prevent "ghost" vectors
		found, err := arch.Content.Find(di.db.ctx, id, false)
		if err != nil {
			return err
		}
		if found {
			// Retrieve old metadata to find old vector location
			oldJson, err := arch.Content.GetCurrentValue(di.db.ctx)
			if err != nil {
				return err
			}
			var oldMeta map[string]any
			if err := json.Unmarshal([]byte(oldJson), &oldMeta); err == nil {
				oldCid := 1
				oldDist := float32(0.0)
				if v, ok := oldMeta["_centroid_id"].(float64); ok {
					oldCid = int(v)
				}
				if v, ok := oldMeta["_distance"].(float64); ok {
					oldDist = float32(v)
				}

				// Remove old vector
				oldKey := CompositeKey{CentroidID: oldCid, DistanceToCentroid: oldDist, ItemID: id}
				if foundVec, _ := arch.Vectors.Find(di.db.ctx, oldKey, false); foundVec {
					if _, err := arch.Vectors.RemoveCurrentItem(di.db.ctx); err != nil {
						return err
					}
				}
			}
			// Remove old content to ensure clean insert
			if _, err := arch.Content.RemoveCurrentItem(di.db.ctx); err != nil {
				return err
			}
		}

		// 1. Assign Centroid
		centroidID, dist := findClosestCentroid(vec, centroids)

		// 2. Update Vector Index
		key := CompositeKey{CentroidID: centroidID, DistanceToCentroid: dist, ItemID: id}
		if _, err := arch.Vectors.Add(di.db.ctx, key, vec); err != nil {
			return err
		}

		// 3. Update Content Store
		meta["_centroid_id"] = centroidID
		meta["_distance"] = dist
		// Serialize metadata to JSON
		data, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal meta: %w", err)
		}
		if _, err := arch.Content.Add(di.db.ctx, id, string(data)); err != nil {
			return err
		}
	}

	return trans.Commit(di.db.ctx)
}

// UpsertBatchWithLookup adds items and updates the integer lookup table for sampling.
func (di *domainIndex) UpsertBatchWithLookup(items []ai.Item, startID int) error {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(sop.ForWriting, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(di.db.ctx)

	arch, err := OpenDomainStore(di.db.ctx, trans, storePath)
	if err != nil {
		return err
	}

	// Load centroids once for the batch
	centroids, err := loadCentroids(di.db.ctx, arch)
	if err != nil {
		return err
	}

	// Auto-init centroids if needed
	if len(centroids) == 0 && len(items) > 0 {
		centroids[1] = items[0].Vector
		if _, err := arch.Centroids.Add(di.db.ctx, 1, items[0].Vector); err != nil {
			return err
		}
	}

	for i, item := range items {
		id := item.ID
		vec := item.Vector
		meta := item.Meta

		// 0. Cleanup Old Entry (if exists)
		found, err := arch.Content.Find(di.db.ctx, id, false)
		if err != nil {
			return err
		}
		if found {
			// Retrieve old metadata to find old vector location
			oldJson, err := arch.Content.GetCurrentValue(di.db.ctx)
			if err != nil {
				return err
			}
			var oldMeta map[string]any
			if err := json.Unmarshal([]byte(oldJson), &oldMeta); err == nil {
				oldCid := 1
				oldDist := float32(0.0)
				if v, ok := oldMeta["_centroid_id"].(float64); ok {
					oldCid = int(v)
				}
				if v, ok := oldMeta["_distance"].(float64); ok {
					oldDist = float32(v)
				}
				oldKey := CompositeKey{CentroidID: oldCid, DistanceToCentroid: oldDist, ItemID: id}
				if foundVec, _ := arch.Vectors.Find(di.db.ctx, oldKey, false); foundVec {
					if _, err := arch.Vectors.RemoveCurrentItem(di.db.ctx); err != nil {
						return err
					}
				}
			}
			if _, err := arch.Content.RemoveCurrentItem(di.db.ctx); err != nil {
				return err
			}
		}

		// 1. Assign Centroid
		centroidID, dist := findClosestCentroid(vec, centroids)

		// 2. Update Vector Index
		key := CompositeKey{CentroidID: centroidID, DistanceToCentroid: dist, ItemID: id}
		if _, err := arch.Vectors.Add(di.db.ctx, key, vec); err != nil {
			return err
		}

		// 3. Update Content Store
		meta["_centroid_id"] = centroidID
		meta["_distance"] = dist
		data, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal meta: %w", err)
		}
		if _, err := arch.Content.Add(di.db.ctx, id, string(data)); err != nil {
			return err
		}

		// 4. Update Lookup Store
		// We use startID + i as the integer key
		if _, err := arch.Lookup.Add(di.db.ctx, startID+i, id); err != nil {
			return err
		}
	}

	return trans.Commit(di.db.ctx)
}

// UpsertContent adds items to the Content store and stages vectors in TempVectors.
func (di *domainIndex) UpsertContent(items []ai.Item) error {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(sop.ForWriting, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(di.db.ctx)

	arch, err := OpenDomainStore(di.db.ctx, trans, storePath)
	if err != nil {
		return err
	}

	// Open TempVectors separately
	tempVectors, err := openTempVectors(di.db.ctx, trans, storePath)
	if err != nil {
		return err
	}

	for _, item := range items {
		// 1. Store vector in TempVectors for staging
		if _, err := tempVectors.Add(di.db.ctx, item.ID, item.Vector); err != nil {
			return err
		}

		// 2. Store metadata in Content (clean, no vector pollution)
		// Ensure we don't have _temp_vector in meta if it was passed in
		delete(item.Meta, "_temp_vector")

		data, err := json.Marshal(item.Meta)
		if err != nil {
			return fmt.Errorf("failed to marshal meta: %w", err)
		}
		if _, err := arch.Content.Add(di.db.ctx, item.ID, string(data)); err != nil {
			return err
		}
	}

	return trans.Commit(di.db.ctx)
}

// IndexAll iterates over all items in the TempVectors store, assigns them to centroids,
// and populates the Vectors store. This is typically used after a bulk load of content.
// It deletes the TempVectors store upon completion.
func (di *domainIndex) IndexAll() error {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(sop.ForWriting, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(di.db.ctx)

	arch, err := OpenDomainStore(di.db.ctx, trans, storePath)
	if err != nil {
		return err
	}

	// Open TempVectors
	tempVectors, err := openTempVectors(di.db.ctx, trans, storePath)
	if err != nil {
		return err
	}

	// Load centroids
	centroids, err := loadCentroids(di.db.ctx, arch)
	if err != nil {
		return err
	}
	if len(centroids) == 0 {
		return fmt.Errorf("no centroids found, cannot index vectors")
	}

	if ok, err := tempVectors.First(di.db.ctx); err != nil {
		return err
	} else if !ok {
		return nil
	}

	for {
		itemKV, err := tempVectors.GetCurrentItem(di.db.ctx)
		if err != nil {
			return err
		}

		id := itemKV.Key
		vec := *itemKV.Value

		if len(vec) > 0 {
			// Assign Centroid
			centroidID, dist := findClosestCentroid(vec, centroids)

			// Update Vector Index
			key := CompositeKey{CentroidID: centroidID, DistanceToCentroid: dist, ItemID: id}
			if _, err := arch.Vectors.Add(di.db.ctx, key, vec); err != nil {
				return err
			}

			// Update Content Store with Centroid Info
			// We need to fetch the content, update meta, and save it back.
			// This is the cost of separation, but it keeps content clean.
			found, err := arch.Content.Find(di.db.ctx, id, false)
			if err != nil {
				return err
			}
			if found {
				jsonStr, _ := arch.Content.GetCurrentValue(di.db.ctx)
				var meta map[string]any
				if err := json.Unmarshal([]byte(jsonStr), &meta); err == nil {
					meta["_centroid_id"] = centroidID
					meta["_distance"] = dist
					data, _ := json.Marshal(meta)
					if _, err := arch.Content.UpdateCurrentItem(di.db.ctx, string(data)); err != nil {
						return err
					}
				}
			}
		}

		if ok, err := tempVectors.Next(di.db.ctx); err != nil {
			return err
		} else if !ok {
			break
		}
	}

	if err := trans.Commit(di.db.ctx); err != nil {
		return err
	}

	// Cleanup: Delete TempVectors store
	// We ignore errors here as it's just cleanup and doesn't affect data integrity.
	_ = inredfs.RemoveBtree(di.db.ctx, storePath, "temp_vectors")

	return nil
}

// Get retrieves a vector by ID.
func (di *domainIndex) Get(id string) (*ai.Item, error) {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(di.db.readMode, storePath)
	if err != nil {
		return nil, err
	}
	defer trans.Rollback(di.db.ctx)

	arch, err := OpenDomainStore(di.db.ctx, trans, storePath)
	if err != nil {
		return nil, err
	}

	// 1. Fetch Content
	found, err := arch.Content.Find(di.db.ctx, id, false)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("item not found")
	}

	jsonStr, err := arch.Content.GetCurrentValue(di.db.ctx)
	if err != nil {
		return nil, err
	}

	var meta map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal meta: %w", err)
	}

	// 2. Fetch Vector (Optional, but good for completeness)
	// We assume Centroid 1 for MVP. In real app, we'd store CentroidID in Content or look it up.
	cid := 1
	dist := float32(0.0)
	if v, ok := meta["_centroid_id"].(float64); ok {
		cid = int(v)
	}
	if v, ok := meta["_distance"].(float64); ok {
		dist = float32(v)
	}

	vecKey := CompositeKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: id}
	foundVec, err := arch.Vectors.Find(di.db.ctx, vecKey, false)
	var vec []float32
	if foundVec {
		vec, _ = arch.Vectors.GetCurrentValue(di.db.ctx)
	}

	return &ai.Item{ID: id, Vector: vec, Meta: meta}, nil
}

// Delete removes a vector from the store.
func (di *domainIndex) Delete(id string) error {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(sop.ForWriting, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(di.db.ctx)

	arch, err := OpenDomainStore(di.db.ctx, trans, storePath)
	if err != nil {
		return err
	}

	// 1. Remove from Content
	found, err := arch.Content.Find(di.db.ctx, id, false)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	// Get meta to find vector key
	jsonStr, _ := arch.Content.GetCurrentValue(di.db.ctx)
	var meta map[string]any
	cid := 1
	dist := float32(0.0)
	if err := json.Unmarshal([]byte(jsonStr), &meta); err == nil {
		if v, ok := meta["_centroid_id"].(float64); ok {
			cid = int(v)
		}
		if v, ok := meta["_distance"].(float64); ok {
			dist = float32(v)
		}
	}

	if _, err := arch.Content.RemoveCurrentItem(di.db.ctx); err != nil {
		return err
	}

	// 2. Remove from Vectors
	vecKey := CompositeKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: id}
	if found, err := arch.Vectors.Find(di.db.ctx, vecKey, false); err != nil {
		return err
	} else if found {
		if _, err := arch.Vectors.RemoveCurrentItem(di.db.ctx); err != nil {
			return err
		}
	}

	return trans.Commit(di.db.ctx)
}

// Query searches for the nearest neighbors to the query vector.
// It uses an IVFFlat-like approach:
// 1. Identify the closest centroids (nprobe).
// 2. Scan vectors only within those centroid buckets.
// 3. Compute cosine similarity and sort candidates.
// 4. Fetch full content for the top K results and apply metadata filters.
func (di *domainIndex) Query(vec []float32, k int, filters map[string]any) ([]ai.Hit, error) {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(di.db.readMode, storePath)
	if err != nil {
		return nil, err
	}
	defer trans.Rollback(di.db.ctx)

	arch, err := OpenDomainStore(di.db.ctx, trans, storePath)
	if err != nil {
		return nil, err
	}

	// 1. Identify Target Centroids
	centroids, err := loadCentroids(di.db.ctx, arch)
	if err != nil {
		return nil, err
	}
	if len(centroids) == 0 {
		return nil, nil
	}

	// Find the closest centroids to the query vector (nprobe)
	// We use a default nprobe of 2 (or all if fewer) to ensure good recall.
	nprobe := 2
	if len(centroids) < nprobe {
		nprobe = len(centroids)
	}

	targetCentroids := findClosestCentroids(vec, centroids, nprobe)

	var candidates []ai.Hit

	for _, cid := range targetCentroids {
		// 2. Scan Vectors in the Bucket
		startKey := CompositeKey{CentroidID: cid, DistanceToCentroid: -1.0, ItemID: ""}

		// Find positions the cursor. If exact match not found, it positions at nearest.
		if _, err := arch.Vectors.Find(di.db.ctx, startKey, true); err != nil {
			return nil, err
		}

		for {
			item, err := arch.Vectors.GetCurrentItem(di.db.ctx)
			if err != nil {
				return nil, err
			}
			// If no current item, we might be at end or empty tree
			if item.Key.ItemID == "" && item.Key.CentroidID == 0 {
				if item.ID.IsNil() {
					break
				}
			}

			key := item.Key
			// If we are before the startKey (shouldn't happen with Find(true) unless wrapped?), move next
			if compositeKeyComparer(key, startKey) < 0 {
				if ok, err := arch.Vectors.Next(di.db.ctx); !ok || err != nil {
					break
				}
				continue
			}

			if key.CentroidID != cid {
				break // Left the bucket
			}

			// Compute Score
			if item.Value == nil {
				// Skip items with missing vectors (should not happen in healthy index)
				if ok, _ := arch.Vectors.Next(di.db.ctx); !ok {
					break
				}
				continue
			}
			itemVec := *item.Value
			score := cosine(vec, itemVec)
			candidates = append(candidates, ai.Hit{ID: key.ItemID, Score: score})

			if ok, _ := arch.Vectors.Next(di.db.ctx); !ok {
				break
			}
		}
	}

	// 3. Sort by Score
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })

	// 4. Fetch Content and Filter (until we have K results)
	var finalHits []ai.Hit
	for _, hit := range candidates {
		if len(finalHits) >= k {
			break
		}

		found, err := arch.Content.Find(di.db.ctx, hit.ID, false)
		if err != nil {
			return nil, err
		}
		if found {
			jsonStr, _ := arch.Content.GetCurrentValue(di.db.ctx)
			var meta map[string]any
			if err := json.Unmarshal([]byte(jsonStr), &meta); err == nil {
				if matchFilters(meta, filters) {
					hit.Meta = meta
					finalHits = append(finalHits, hit)
				}
			}
		}
	}

	return finalHits, nil
}

// --- Helpers ---

func (d *Database) beginTransaction(mode sop.TransactionMode, storePath string) (sop.Transaction, error) {
	storeFolder := storePath
	if err := os.MkdirAll(storeFolder, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data folder %s: %w", storeFolder, err)
	}

	to, err := inredfs.NewTransactionOptions(storeFolder, mode, -1, -1)
	if err != nil {
		return nil, err
	}
	to.Cache = d.cache

	trans, err := inredfs.NewTransaction(d.ctx, to)
	if err != nil {
		return nil, err
	}

	if err := trans.Begin(d.ctx); err != nil {
		return nil, fmt.Errorf("transaction begin failed: %w", err)
	}

	return trans, nil
}

func matchFilters(meta, filters map[string]any) bool {
	for k, v := range filters {
		if mv, ok := meta[k]; !ok || mv != v {
			return false
		}
	}
	return true
}

func cosine(a, b []float32) float32 {
	var dot, na, nb float32
	l := len(a)
	if len(b) < l {
		l = len(b)
	}
	for i := 0; i < l; i++ {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / float32(math.Sqrt(float64(na))*math.Sqrt(float64(nb)))
}

// CompositeKey allows us to group items by Centroid, but keep them unique.
type CompositeKey struct {
	CentroidID         int
	DistanceToCentroid float32
	ItemID             string
}

func compositeKeyComparer(a, b CompositeKey) int {
	if a.CentroidID != b.CentroidID {
		return a.CentroidID - b.CentroidID
	}
	if a.DistanceToCentroid < b.DistanceToCentroid {
		return -1
	}
	if a.DistanceToCentroid > b.DistanceToCentroid {
		return 1
	}
	if a.ItemID < b.ItemID {
		return -1
	}
	if a.ItemID > b.ItemID {
		return 1
	}
	return 0
}

// Helper functions

func loadCentroids(ctx context.Context, arch *Architecture) (map[int][]float32, error) {
	centroids := make(map[int][]float32)
	// Iterate over the Centroids B-Tree
	if ok, err := arch.Centroids.First(ctx); err != nil {
		return nil, err
	} else if ok {
		for {
			item, err := arch.Centroids.GetCurrentItem(ctx)
			if err != nil {
				return nil, err
			}
			centroids[item.Key] = *item.Value

			if ok, err := arch.Centroids.Next(ctx); err != nil {
				return nil, err
			} else if !ok {
				break
			}
		}
	}
	return centroids, nil
}

func findClosestCentroid(vec []float32, centroids map[int][]float32) (int, float32) {
	minDist := float32(math.MaxFloat32)
	closestID := -1

	for id, center := range centroids {
		d := euclideanDistance(vec, center)
		if d < minDist {
			minDist = d
			closestID = id
		}
	}
	return closestID, minDist
}

func findClosestCentroids(vec []float32, centroids map[int][]float32, n int) []int {
	type candidate struct {
		id   int
		dist float32
	}
	var candidates []candidate

	for id, center := range centroids {
		d := euclideanDistance(vec, center)
		candidates = append(candidates, candidate{id: id, dist: d})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].dist < candidates[j].dist
	})

	if len(candidates) > n {
		candidates = candidates[:n]
	}

	result := make([]int, len(candidates))
	for i, c := range candidates {
		result[i] = c.id
	}
	return result
}

func euclideanDistance(a, b []float32) float32 {
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

// SeedCentroids allows manual injection of centroids for testing or initialization.
func (d *Database) SeedCentroids(domain string, centroids map[int][]float32) error {
	storePath := filepath.Join(d.storagePath, domain)
	trans, err := d.beginTransaction(sop.ForWriting, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(d.ctx)

	arch, err := OpenDomainStore(d.ctx, trans, storePath)
	if err != nil {
		return err
	}

	for id, vec := range centroids {
		if _, err := arch.Centroids.Add(d.ctx, id, vec); err != nil {
			return err
		}
	}

	return trans.Commit(d.ctx)
}

// SeedCentroids allows manual injection of centroids for this domain.
func (di *domainIndex) SeedCentroids(centroids map[int][]float32) error {
	return di.db.SeedCentroids(di.name, centroids)
}

// IterateAll iterates over all items in the domain.
// It uses TempVectors if available to get the vector, otherwise tries to find it in Vectors.
func (di *domainIndex) IterateAll(cb func(item ai.Item) error) error {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(di.db.readMode, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(di.db.ctx)

	arch, err := OpenDomainStore(di.db.ctx, trans, storePath)
	if err != nil {
		return err
	}

	// Try to open TempVectors (ignore error if not found, as it might be post-indexing)
	tempVectors, _ := openTempVectors(di.db.ctx, trans, storePath)

	if ok, err := arch.Content.First(di.db.ctx); err != nil {
		return err
	} else if !ok {
		return nil
	}

	for {
		itemKV, err := arch.Content.GetCurrentItem(di.db.ctx)
		if err != nil {
			return err
		}

		// k is ID, v is JSON meta
		var meta map[string]any
		if err := json.Unmarshal([]byte(*itemKV.Value), &meta); err != nil {
			return fmt.Errorf("failed to unmarshal meta for %s: %w", itemKV.Key, err)
		}

		var vec []float32

		// 1. Try TempVectors first (Ingestion Phase)
		if tempVectors != nil {
			if found, err := tempVectors.Find(di.db.ctx, itemKV.Key, false); err == nil && found {
				v, _ := tempVectors.GetCurrentValue(di.db.ctx)
				vec = v
			}
		}

		// 2. If not in Temp, try Vectors (Post-Ingestion Phase)
		if len(vec) == 0 {
			cid := 1
			dist := float32(0.0)
			if val, ok := meta["_centroid_id"].(float64); ok {
				cid = int(val)
			}
			if val, ok := meta["_distance"].(float64); ok {
				dist = float32(val)
			}

			vecKey := CompositeKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: itemKV.Key}
			foundVec, err := arch.Vectors.Find(di.db.ctx, vecKey, false)
			if err != nil {
				return err
			}
			if foundVec {
				v, _ := arch.Vectors.GetCurrentValue(di.db.ctx)
				vec = v
			}
		}

		item := ai.Item{
			ID:     itemKV.Key,
			Vector: vec,
			Meta:   meta,
		}

		if err := cb(item); err != nil {
			return err
		}

		if ok, err := arch.Content.Next(di.db.ctx); err != nil {
			return err
		} else if !ok {
			break
		}
	}
	return nil
}
func (di *domainIndex) Count() (int64, error) {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(di.db.readMode, storePath)
	if err != nil {
		return 0, err
	}
	defer trans.Rollback(di.db.ctx)

	arch, err := OpenDomainStore(di.db.ctx, trans, storePath)
	if err != nil {
		return 0, err
	}

	return arch.Content.Count(), nil
}

// GetLookup retrieves the item ID for a given integer ID (used for sampling).
func (di *domainIndex) GetLookup(id int) (string, error) {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(di.db.readMode, storePath)
	if err != nil {
		return "", err
	}
	defer trans.Rollback(di.db.ctx)

	arch, err := OpenDomainStore(di.db.ctx, trans, storePath)
	if err != nil {
		return "", err
	}

	found, err := arch.Lookup.Find(di.db.ctx, id, false)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("lookup id %d not found", id)
	}

	return arch.Lookup.GetCurrentValue(di.db.ctx)
}

// openTempVectors opens or creates the temporary B-Tree for storing vectors during ingestion.
func openTempVectors(ctx context.Context, trans sop.Transaction, rootPath string) (btree.BtreeInterface[string, []float32], error) {
	contentComparer := func(a, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	}

	var tempVectors btree.BtreeInterface[string, []float32]
	var err error
	tempVectorsName := "temp_vectors"
	if _, errStat := os.Stat(filepath.Join(rootPath, tempVectorsName)); errStat == nil {
		tempVectors, err = inredfs.OpenBtree[string, []float32](ctx, tempVectorsName, trans, contentComparer)
	} else {
		tempVectors, err = inredfs.NewBtree[string, []float32](ctx, sop.StoreOptions{
			Name: tempVectorsName,
		}, trans, contentComparer)
	}
	if err != nil {
		return nil, err
	}
	return tempVectors, nil
}
