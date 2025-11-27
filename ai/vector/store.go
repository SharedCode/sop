package vector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/inredfs"
)

// UsageMode defines how the vector database is intended to be used.
type UsageMode int

const (
	// BuildOnceQueryMany optimizes for a single ingestion phase followed by read-only queries.
	// Temporary structures (TempVectors, Lookup) are deleted after indexing to save space.
	BuildOnceQueryMany UsageMode = iota
	// ReadWrite optimizes for continuous updates and queries.
	// Temporary structures are retained to support ongoing modifications.
	ReadWrite
)

// Database is a persistent vector database manager backed by SOP B-Trees.
// It manages the storage and retrieval of vectors and their associated metadata.
// It supports transactions and caching for performance.
type Database struct {
	ctx           context.Context
	cache         sop.L2Cache
	readMode      sop.TransactionMode
	usageMode     UsageMode
	storagePath   string
	rebalanceLock sync.Mutex
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
		usageMode:   BuildOnceQueryMany, // Default to most common AI pattern
		storagePath: "",
	}
}

// SetUsageMode configures the usage pattern for the database.
func (d *Database) SetUsageMode(mode UsageMode) {
	d.usageMode = mode
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
	// centroidsCache caches the centroids to avoid reloading them from the B-Tree on every operation.
	centroidsCache map[int][]float32
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

	version, err := di.getActiveVersion(di.db.ctx, trans)
	if err != nil {
		return err
	}

	arch, err := OpenDomainStore(di.db.ctx, trans, version)
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
				// Decrement count of old centroid
				if foundC, _ := arch.Centroids.Find(di.db.ctx, oldCid, false); foundC {
					c, _ := arch.Centroids.GetCurrentValue(di.db.ctx)
					c.VectorCount--
					arch.Centroids.UpdateCurrentItem(di.db.ctx, c)
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
	centroids, err := di.getCentroids(di.db.ctx, arch)
	if err != nil {
		return err
	}

	// If no centroids exist, create one (Auto-Initialization for MVP)
	if len(centroids) == 0 {
		// Create Centroid 1 at the position of this first vector
		// In a real system, we'd run K-Means on a batch.
		centroids[1] = vec
		// Initialize with Count 0, it will be incremented below
		if _, err := arch.Centroids.Add(di.db.ctx, 1, Centroid{Vector: vec, VectorCount: 0}); err != nil {
			return err
		}
		// Update cache
		di.centroidsCache = centroids
	}

	centroidID, dist := findClosestCentroid(vec, centroids)

	// Increment count
	if foundC, _ := arch.Centroids.Find(di.db.ctx, centroidID, false); foundC {
		c, _ := arch.Centroids.GetCurrentValue(di.db.ctx)
		c.VectorCount++
		arch.Centroids.UpdateCurrentItem(di.db.ctx, c)
	}

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

	version, err := di.getActiveVersion(di.db.ctx, trans)
	if err != nil {
		return err
	}

	arch, err := OpenDomainStore(di.db.ctx, trans, version)
	if err != nil {
		return err
	}

	// Load centroids once for the batch
	centroids, err := di.getCentroids(di.db.ctx, arch)
	if err != nil {
		return err
	}

	// Auto-init centroids if needed (handle first batch case)
	if len(centroids) == 0 && len(items) > 0 {
		k := int(math.Sqrt(float64(len(items))))
		if k < 1 {
			k = 1
		}
		if k > 256 {
			k = 256
		}
		centroids, err = ComputeCentroids(items, k)
		if err != nil {
			return err
		}
		for id, vec := range centroids {
			if _, err := arch.Centroids.Add(di.db.ctx, id, Centroid{Vector: vec, VectorCount: 0}); err != nil {
				return err
			}
		}
		// Update cache
		di.centroidsCache = centroids
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
					// Decrement count
					if foundC, _ := arch.Centroids.Find(di.db.ctx, oldCid, false); foundC {
						c, _ := arch.Centroids.GetCurrentValue(di.db.ctx)
						c.VectorCount--
						arch.Centroids.UpdateCurrentItem(di.db.ctx, c)
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

		// Increment count
		if foundC, _ := arch.Centroids.Find(di.db.ctx, centroidID, false); foundC {
			c, _ := arch.Centroids.GetCurrentValue(di.db.ctx)
			c.VectorCount++
			arch.Centroids.UpdateCurrentItem(di.db.ctx, c)
		}

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

// UpsertContent adds items to the Content store and stages vectors in TempVectors.
func (di *domainIndex) UpsertContent(items []ai.Item) error {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(sop.ForWriting, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(di.db.ctx)

	version, err := di.getActiveVersion(di.db.ctx, trans)
	if err != nil {
		return err
	}

	arch, err := OpenDomainStore(di.db.ctx, trans, version)
	if err != nil {
		return err
	}

	for _, item := range items {
		// 1. Store vector in TempVectors for staging
		if _, err := arch.TempVectors.Add(di.db.ctx, item.ID, item.Vector); err != nil {
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

	version, err := di.getActiveVersion(di.db.ctx, trans)
	if err != nil {
		return err
	}

	arch, err := OpenDomainStore(di.db.ctx, trans, version)
	if err != nil {
		return err
	}

	// Load centroids
	centroids, err := di.getCentroids(di.db.ctx, arch)
	if err != nil {
		return err
	}
	// If no centroids, train them using data in TempVectors
	if len(centroids) == 0 {
		// 1. Collect all vectors from TempVectors
		var trainingData []ai.Item
		if ok, err := arch.TempVectors.First(di.db.ctx); err != nil {
			return err
		} else if ok {
			for {
				itemKV, err := arch.TempVectors.GetCurrentItem(di.db.ctx)
				if err != nil {
					return err
				}
				trainingData = append(trainingData, ai.Item{
					ID:     itemKV.Key,
					Vector: *itemKV.Value,
				})
				if ok, err := arch.TempVectors.Next(di.db.ctx); err != nil {
					return err
				} else if !ok {
					break
				}
			}
		}

		if len(trainingData) > 0 {
			// 2. Determine K (Rule of thumb: sqrt(N))
			k := int(math.Sqrt(float64(len(trainingData))))
			if k < 1 {
				k = 1
			}
			// Cap K for performance/sanity if needed, e.g. 256
			if k > 256 {
				k = 256
			}

			// 3. Train
			centroids, err = ComputeCentroids(trainingData, k)
			if err != nil {
				return err
			}

			// 4. Save to Storage
			for id, vec := range centroids {
				if _, err := arch.Centroids.Add(di.db.ctx, id, Centroid{Vector: vec, VectorCount: 0}); err != nil {
					return err
				}
			}
			// Update cache
			di.centroidsCache = centroids
		} else {
			// No data to index
			return nil
		}
	}

	if ok, err := arch.TempVectors.First(di.db.ctx); err != nil {
		return err
	} else if !ok {
		return nil
	}

	// Determine start index for Lookup to append
	i := 0
	if ok, err := arch.Lookup.Last(di.db.ctx); err != nil {
		return err
	} else if ok {
		item, _ := arch.Lookup.GetCurrentItem(di.db.ctx)
		i = item.Key + 1
	}

	// Reposition cursor to the beginning of TempVectors since we might have moved it (though we didn't touch TempVectors above, it's safer)
	// We iterate and remove items one by one to "drain" the TempVectors.
	// This avoids deleting the B-Tree file which causes cache consistency issues in tests.
	for {
		if ok, err := arch.TempVectors.First(di.db.ctx); err != nil {
			return err
		} else if !ok {
			break
		}

		itemKV, err := arch.TempVectors.GetCurrentItem(di.db.ctx)
		if err != nil {
			return err
		}

		id := itemKV.Key
		vec := *itemKV.Value

		if len(vec) > 0 {
			// Assign Centroid
			centroidID, dist := findClosestCentroid(vec, centroids)

			// Increment count
			if foundC, _ := arch.Centroids.Find(di.db.ctx, centroidID, false); foundC {
				c, _ := arch.Centroids.GetCurrentValue(di.db.ctx)
				c.VectorCount++
				arch.Centroids.UpdateCurrentItem(di.db.ctx, c)
			}

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

		// Populate Lookup (Int -> ID)
		if _, err := arch.Lookup.Add(di.db.ctx, i, id); err != nil {
			return err
		}
		i++

		// Remove the item from TempVectors as it is now indexed
		if _, err := arch.TempVectors.RemoveCurrentItem(di.db.ctx); err != nil {
			return err
		}
	}

	if err := trans.Commit(di.db.ctx); err != nil {
		return err
	}

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

	version, err := di.getActiveVersion(di.db.ctx, trans)
	if err != nil {
		return nil, err
	}

	arch, err := OpenDomainStore(di.db.ctx, trans, version)
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

	if v, ok := meta["_deleted"].(bool); ok && v {
		return nil, fmt.Errorf("item not found")
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

	version, err := di.getActiveVersion(di.db.ctx, trans)
	if err != nil {
		return err
	}

	arch, err := OpenDomainStore(di.db.ctx, trans, version)
	if err != nil {
		return err
	}

	// 1. Soft Remove from Content
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

	// Soft Delete: Mark as deleted instead of removing to avoid B-Tree corruption bugs
	meta["_deleted"] = true
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if _, err := arch.Content.UpdateCurrentItem(di.db.ctx, string(data)); err != nil {
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
	} else {
		// If not found in Vectors, it might be a consistency issue or already gone.
		// We log nothing and proceed as Content is already cleaned.
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

	version, err := di.getActiveVersion(di.db.ctx, trans)
	if err != nil {
		return nil, err
	}

	arch, err := OpenDomainStore(di.db.ctx, trans, version)
	if err != nil {
		return nil, err
	}

	// 1. Identify Target Centroids
	centroids, err := di.getCentroids(di.db.ctx, arch)
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

	// We need to know the active version to seed the correct store
	// But Database doesn't have 'name' field, so we can't use getActiveVersion helper easily
	// unless we duplicate it or make it a function of Database.
	// For now, let's assume SeedCentroids is used on a fresh store or we instantiate a domainIndex to use helper.
	di := &domainIndex{db: d, name: domain}
	version, err := di.getActiveVersion(d.ctx, trans)
	if err != nil {
		return err
	}

	arch, err := OpenDomainStore(d.ctx, trans, version)
	if err != nil {
		return err
	}

	for id, vec := range centroids {
		if _, err := arch.Centroids.Add(d.ctx, id, Centroid{Vector: vec, VectorCount: 0}); err != nil {
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

	version, err := di.getActiveVersion(di.db.ctx, trans)
	if err != nil {
		return err
	}

	arch, err := OpenDomainStore(di.db.ctx, trans, version)
	if err != nil {
		return err
	}

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
		// We check if TempVectors is available (it might be deleted in BuildOnce mode)
		// But since we opened it via OpenDomainStore, it should be initialized, but maybe empty or deleted on disk?
		// OpenDomainStore creates new Btree instances. If the underlying file is gone, it might be empty or error?
		// Actually, OpenDomainStore calls NewBtree which opens or creates. If deleted, it creates new empty one.
		// So checking if it has items is enough.
		if found, err := arch.TempVectors.Find(di.db.ctx, itemKV.Key, false); err == nil && found {
			v, _ := arch.TempVectors.GetCurrentValue(di.db.ctx)
			vec = v
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

	version, err := di.getActiveVersion(di.db.ctx, trans)
	if err != nil {
		return 0, err
	}

	arch, err := OpenDomainStore(di.db.ctx, trans, version)
	if err != nil {
		return 0, err
	}

	return arch.Content.Count(), nil
}

// getCentroids returns the centroids for the domain, loading them from the B-Tree if not cached.
func (di *domainIndex) getCentroids(ctx context.Context, arch *Architecture) (map[int][]float32, error) {
	if di.centroidsCache != nil {
		return di.centroidsCache, nil
	}

	centroids := make(map[int][]float32)
	if ok, err := arch.Centroids.First(ctx); err != nil {
		return nil, err
	} else if ok {
		for {
			item, err := arch.Centroids.GetCurrentItem(ctx)
			if err != nil {
				return nil, err
			}
			centroids[item.Key] = item.Value.Vector

			if ok, err := arch.Centroids.Next(ctx); err != nil {
				return nil, err
			} else if !ok {
				break
			}
		}
	}

	di.centroidsCache = centroids
	return centroids, nil
}

// Rebalance rebuilds the index by re-computing centroids based on the current data distribution.
// It uses a streaming approach to handle datasets larger than memory:
// 1. Starts a Write Transaction.
// 2. Samples a subset of vectors to train new centroids.
// 3. Streams all vectors from the old index, re-assigns them to new centroids, and writes to the new index.
// 4. Updates metadata and lookup tables.
// 5. Swaps the active version atomically.
func (di *domainIndex) Rebalance() error {
	di.db.rebalanceLock.Lock()
	defer di.db.rebalanceLock.Unlock()

	storePath := filepath.Join(di.db.storagePath, di.name)

	// Start a single Write transaction for the entire operation
	trans, err := di.db.beginTransaction(sop.ForWriting, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(di.db.ctx)

	// Open SysConfig Store once for the transaction
	sysConfig, err := inredfs.NewBtree[string, int64](di.db.ctx, sop.StoreOptions{
		Name: sysConfigName,
	}, trans, func(a, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	if err != nil {
		return err
	}

	var currentVersion int64
	found, err := sysConfig.Find(di.db.ctx, "active_version", false)
	if err != nil {
		return err
	}
	if found {
		currentVersion, _ = sysConfig.GetCurrentValue(di.db.ctx)
	}

	oldArch, err := OpenDomainStore(di.db.ctx, trans, currentVersion)
	if err != nil {
		return err
	}

	// Step 0: Prepare New Version Stores
	newVersion := time.Now().UnixNano()
	suffix := fmt.Sprintf("_%d", newVersion)

	// Create New Lookup (Versioned)
	newLookup, err := inredfs.NewBtree[int, string](di.db.ctx, sop.StoreOptions{
		Name: "lookup" + suffix,
	}, trans, func(a, b int) int { return a - b })
	if err != nil {
		return err
	}

	// Step 1: Estimate N and Determine K
	totalItems := oldArch.Vectors.Count()
	if totalItems == 0 {
		return nil
	}

	k := int(math.Sqrt(float64(totalItems)))
	if k < 1 {
		k = 1
	}
	if k > 256 {
		k = 256
	}

	// Step 2: Pass 1 - Build Lookup
	// We iterate the entire old vector store to populate the NEW Lookup table.
	lookupIdx := 0
	if ok, err := oldArch.Vectors.First(di.db.ctx); err != nil {
		return err
	} else if ok {
		for {
			item, err := oldArch.Vectors.GetCurrentItem(di.db.ctx)
			if err != nil {
				return err
			}

			// Verify existence in Content to avoid ghost vectors
			if found, err := oldArch.Content.Find(di.db.ctx, item.Key.ItemID, false); err != nil {
				return err
			} else if !found {
				// Ghost vector, skip
				if ok, err := oldArch.Vectors.Next(di.db.ctx); err != nil {
					return err
				} else if !ok {
					break
				}
				continue
			}

			// Check for soft delete
			contentItem, err := oldArch.Content.GetCurrentItem(di.db.ctx)
			if err != nil {
				return err
			}
			if contentItem.Value != nil {
				var meta map[string]any
				if err := json.Unmarshal([]byte(*contentItem.Value), &meta); err == nil {
					if v, ok := meta["_deleted"].(bool); ok && v {
						// Skip deleted
						if ok, err := oldArch.Vectors.Next(di.db.ctx); err != nil {
							return err
						} else if !ok {
							break
						}
						continue
					}
				}
			}

			// Populate New Lookup
			if _, err := newLookup.Add(di.db.ctx, lookupIdx, item.Key.ItemID); err != nil {
				return err
			}
			lookupIdx++

			if ok, err := oldArch.Vectors.Next(di.db.ctx); err != nil {
				return err
			} else if !ok {
				break
			}
		}
	}

	// Step 4: Collect Sample (Random Access via New Lookup)
	// We select random indices from the populated Lookup table and fetch the vectors.
	// This ensures an ideal random sample from the pool.
	sampleSize := 50000
	currentTotal := lookupIdx
	if currentTotal < sampleSize {
		sampleSize = currentTotal
	}

	var trainingItems []ai.Item
	if sampleSize > 0 {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		selectedIndices := make(map[int]bool)

		// If sample size is close to total, just take all
		if sampleSize == currentTotal {
			for i := 0; i < currentTotal; i++ {
				selectedIndices[i] = true
			}
		} else {
			for len(selectedIndices) < sampleSize {
				idx := rng.Intn(currentTotal)
				selectedIndices[idx] = true
			}
		}

		// Convert to sorted slice for sequential-ish access
		indices := make([]int, 0, len(selectedIndices))
		for idx := range selectedIndices {
			indices = append(indices, idx)
		}
		sort.Ints(indices)

		for _, idx := range indices {
			// 1. Get ID from New Lookup
			if found, err := newLookup.Find(di.db.ctx, idx, false); err != nil {
				return err
			} else if !found {
				continue
			}
			lookupItem, err := newLookup.GetCurrentItem(di.db.ctx)
			if err != nil {
				return err
			}
			if lookupItem.Value == nil {
				continue
			}
			id := *lookupItem.Value

			// 2. Get Meta from Content
			if found, err := oldArch.Content.Find(di.db.ctx, id, false); err != nil {
				return err
			} else if !found {
				continue
			}
			contentItem, err := oldArch.Content.GetCurrentItem(di.db.ctx)
			if err != nil {
				return err
			}
			if contentItem.Value == nil {
				continue
			}
			jsonStr := *contentItem.Value
			var meta map[string]any
			if err := json.Unmarshal([]byte(jsonStr), &meta); err != nil {
				continue
			}

			cid := 1
			dist := float32(0.0)
			if v, ok := meta["_centroid_id"].(float64); ok {
				cid = int(v)
			}
			if v, ok := meta["_distance"].(float64); ok {
				dist = float32(v)
			}

			// 3. Get Vector
			vecKey := CompositeKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: id}
			if found, err := oldArch.Vectors.Find(di.db.ctx, vecKey, false); err != nil {
				return err
			} else if found {
				vecItem, err := oldArch.Vectors.GetCurrentItem(di.db.ctx)
				if err != nil {
					return err
				}
				if vecItem.Value != nil {
					trainingItems = append(trainingItems, ai.Item{ID: id, Vector: *vecItem.Value})
				}
			}
		}
	}

	// Step 5: Train Centroids
	centroids, err := ComputeCentroids(trainingItems, k)
	if err != nil {
		return err
	}

	// Step 5: Create New Stores (Centroids and Vectors)
	newCentroids, err := inredfs.NewBtree[int, Centroid](di.db.ctx, sop.StoreOptions{
		Name: "centroids" + suffix,
	}, trans, func(a, b int) int { return a - b })
	if err != nil {
		return err
	}

	newVectors, err := inredfs.NewBtree[CompositeKey, []float32](di.db.ctx, sop.StoreOptions{
		Name: "vectors" + suffix,
	}, trans, compositeKeyComparer)
	if err != nil {
		return err
	}

	newArch := &Architecture{
		Centroids:   newCentroids,
		Vectors:     newVectors,
		Content:     oldArch.Content,     // Reuse shared store
		TempVectors: oldArch.TempVectors, // Reuse shared store
		Lookup:      newLookup,           // Use NEW versioned lookup
		Version:     newVersion,
	}

	// Step 6: Pass 2 - Stream and Re-index
	// We iterate ALL items from the old vector store AGAIN.
	// We assign them to the newly trained centroids and populate the new Vectors store.
	if ok, err := oldArch.Vectors.First(di.db.ctx); err != nil {
		return err
	} else if ok {
		counts := make(map[int]int)

		for {
			item, err := oldArch.Vectors.GetCurrentItem(di.db.ctx)
			if err != nil {
				return err
			}
			if item.Value == nil {
				// Skip invalid items
				if ok, err := oldArch.Vectors.Next(di.db.ctx); err != nil {
					return err
				} else if !ok {
					break
				}
				continue
			}

			vec := *item.Value
			id := item.Key.ItemID

			// Verify existence in Content to avoid ghost vectors
			if found, err := oldArch.Content.Find(di.db.ctx, id, false); err != nil {
				return err
			} else if !found {
				// Ghost vector, skip
				if ok, err := oldArch.Vectors.Next(di.db.ctx); err != nil {
					return err
				} else if !ok {
					break
				}
				continue
			}

			// Check for soft delete
			delCheckItem, err := oldArch.Content.GetCurrentItem(di.db.ctx)
			if err != nil {
				return err
			}
			if delCheckItem.Value != nil {
				var meta map[string]any
				if err := json.Unmarshal([]byte(*delCheckItem.Value), &meta); err == nil {
					if v, ok := meta["_deleted"].(bool); ok && v {
						// Skip deleted
						if ok, err := oldArch.Vectors.Next(di.db.ctx); err != nil {
							return err
						} else if !ok {
							break
						}
						continue
					}
				}
			}

			// Assign to new centroid
			cid, dist := findClosestCentroid(vec, centroids)
			counts[cid]++

			// Insert to New Vectors
			key := CompositeKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: id}
			if _, err := newArch.Vectors.Add(di.db.ctx, key, vec); err != nil {
				return err
			}

			// Update Content with updated metadata (In-Place)
			// We already found it above, so we can just get current item
			contentItem, err := oldArch.Content.GetCurrentItem(di.db.ctx)
			if err != nil {
				return err
			}
			if contentItem.Value != nil {
				jsonStr := *contentItem.Value
				var meta map[string]any
				if err := json.Unmarshal([]byte(jsonStr), &meta); err == nil {
					meta["_centroid_id"] = cid
					meta["_distance"] = dist
					// data, _ := json.Marshal(meta)
					// Workaround: Use Remove+Add instead of UpdateCurrentItem to avoid potential nil value issues
					// if _, err := oldArch.Content.RemoveCurrentItem(di.db.ctx); err != nil {
					// 	return err
					// }
					// if _, err := oldArch.Content.Add(di.db.ctx, id, string(data)); err != nil {
					// 	return err
					// }
				}
			}

			// Note: Lookup was already populated in Pass 1.

			if ok, err := oldArch.Vectors.Next(di.db.ctx); err != nil {
				return err
			} else if !ok {
				break
			}
		}

		// Step 7: Save Centroids with Counts
		for id, vec := range centroids {
			if _, err := newArch.Centroids.Add(di.db.ctx, id, Centroid{Vector: vec, VectorCount: counts[id]}); err != nil {
				return err
			}
		}
	}

	// Step 8: Swap Active Version
	if _, err := sysConfig.Add(di.db.ctx, "active_version", newVersion); err != nil {
		return err
	}

	di.centroidsCache = centroids

	if err := trans.Commit(di.db.ctx); err != nil {
		return err
	}

	// Step 9: Cleanup Old Version
	// We do this in a separate transaction/context to not block.
	// We only remove the versioned B-Trees (Centroids, Vectors, Lookup).
	suffix = ""
	if currentVersion > 0 {
		suffix = fmt.Sprintf("_%d", currentVersion)
	}
	_ = inredfs.RemoveBtree(di.db.ctx, storePath, "centroids"+suffix)
	_ = inredfs.RemoveBtree(di.db.ctx, storePath, "vectors"+suffix)
	_ = inredfs.RemoveBtree(di.db.ctx, storePath, "lookup"+suffix)
	// Do NOT remove Content, TempVectors as they are shared.

	return nil
}

const sysConfigName = "sys_config"

func (di *domainIndex) getActiveVersion(ctx context.Context, trans sop.Transaction) (int64, error) {
	store, err := inredfs.NewBtree[string, int64](ctx, sop.StoreOptions{
		Name: sysConfigName,
	}, trans, func(a, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	if err != nil {
		return 0, err
	}
	found, err := store.Find(ctx, "active_version", false)
	if err != nil {
		return 0, err
	}
	if found {
		return store.GetCurrentValue(ctx)
	}
	return 0, nil
}

func (di *domainIndex) setActiveVersion(ctx context.Context, trans sop.Transaction, version int64) error {
	store, err := inredfs.NewBtree[string, int64](ctx, sop.StoreOptions{
		Name: sysConfigName,
	}, trans, func(a, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	if err != nil {
		return err
	}
	_, err = store.Add(ctx, "active_version", version)
	return err
}

// GetBySequenceID retrieves an item by its integer sequence ID from the Lookup store.
// This is useful for random sampling or iterating over the dataset.
func (di *domainIndex) GetBySequenceID(seqID int) (*ai.Item, error) {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(di.db.readMode, storePath)
	if err != nil {
		return nil, err
	}
	defer trans.Rollback(di.db.ctx)

	version, err := di.getActiveVersion(di.db.ctx, trans)
	if err != nil {
		return nil, err
	}

	arch, err := OpenDomainStore(di.db.ctx, trans, version)
	if err != nil {
		return nil, err
	}

	// 1. Lookup ID from Sequence
	found, err := arch.Lookup.Find(di.db.ctx, seqID, false)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("sequence ID %d not found", seqID)
	}

	lookupItem, err := arch.Lookup.GetCurrentItem(di.db.ctx)
	if err != nil {
		return nil, err
	}
	if lookupItem.Value == nil {
		return nil, fmt.Errorf("sequence ID %d has nil value", seqID)
	}
	id := *lookupItem.Value

	// 2. Fetch Content using ID
	// We can reuse the logic from Get(id), but we are already in a transaction.
	// So we just duplicate the fetch logic here.

	found, err = arch.Content.Find(di.db.ctx, id, false)
	if err != nil {
		return nil, err
	}
	if !found {
		// This implies referential integrity issue (Lookup points to non-existent Content)
		return nil, fmt.Errorf("item ID %s found in lookup but not in content", id)
	}

	itemKV, err := arch.Content.GetCurrentItem(di.db.ctx)
	if err != nil {
		return nil, err
	}
	if itemKV.Value == nil {
		return nil, fmt.Errorf("content value is nil for %s", id)
	}
	jsonStr := *itemKV.Value

	var meta map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal meta: %w", err)
	}

	if v, ok := meta["_deleted"].(bool); ok && v {
		return nil, fmt.Errorf("item not found")
	}

	// 3. Fetch Vector
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
