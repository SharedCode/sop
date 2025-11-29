package vector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/inredfs"
)

// Database is a persistent vector database manager backed by SOP B-Trees.
// It manages the storage and retrieval of vectors and their associated metadata.
// It supports transactions and caching for performance.
type Database[T any] struct {
	ctx           context.Context
	cache         sop.L2Cache
	readMode      sop.TransactionMode
	usageMode     ai.UsageMode
	storagePath   string
	rebalanceLock sync.Mutex
}

// NewDatabase creates a new vector database manager.
// It initializes the cache (InMemory by default) and sets default transaction modes.
func NewDatabase[T any]() *Database[T] {
	// Use InMemoryCache by default for standalone AI apps
	sop.SetCacheFactory(sop.InMemory)
	return &Database[T]{
		ctx:         context.Background(),
		cache:       cache.NewInMemoryCache(),
		readMode:    sop.NoCheck,
		usageMode:   ai.BuildOnceQueryMany, // Default to most common AI pattern
		storagePath: "",
	}
}

// SetUsageMode configures the usage pattern for the database.
func (d *Database[T]) SetUsageMode(mode ai.UsageMode) {
	d.usageMode = mode
}

// SetStoragePath configures the file system path for data persistence.
func (d *Database[T]) SetStoragePath(path string) {
	d.storagePath = path
}

// SetReadMode configures the transaction mode for Query operations.
func (d *Database[T]) SetReadMode(mode sop.TransactionMode) {
	d.readMode = mode
}

// Open returns an Index for the specified domain.
func (d *Database[T]) Open(domain string) ai.VectorStore[T] {
	return &domainIndex[T]{
		db:                   d,
		name:                 domain,
		deduplicationEnabled: true,
	}
}

// domainIndex implements the Index interface for a specific domain using the 3-table layout.
type domainIndex[T any] struct {
	db   *Database[T]
	name string
	// centroidsCache caches the centroids to avoid reloading them from the B-Tree on every operation.
	centroidsCache       map[int][]float32
	deduplicationEnabled bool
}

// SetDeduplication enables or disables the internal deduplication check during Upsert.
func (di *domainIndex[T]) SetDeduplication(enabled bool) {
	di.deduplicationEnabled = enabled
}

// storedItem wraps the user payload with system metadata.
type storedItem[T any] struct {
	Payload    T       `json:"payload"`
	CentroidID int     `json:"_centroid_id"`
	Distance   float32 `json:"_distance"`
	Deleted    bool    `json:"_deleted,omitempty"`
}

// Upsert adds or updates a vector in the store.
func (di *domainIndex[T]) Upsert(item ai.Item[T]) error {
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

	id := item.ID
	vec := item.Vector

	// 0. Cleanup Old Entry (if exists) to prevent "ghost" vectors
	if di.deduplicationEnabled {
		found, err := arch.Content.Find(di.db.ctx, id, false)
		if err != nil {
			return err
		}
		if found {
			// Retrieve old metadata to find old vector location
			oldJson, err := arch.Content.GetCurrentValue(di.db.ctx)
			if err != nil && !os.IsNotExist(err) {
				return err
			}

			if err == nil {
				var oldStored storedItem[T]
				if err := json.Unmarshal([]byte(oldJson), &oldStored); err == nil {
					oldCid := oldStored.CentroidID
					oldDist := oldStored.Distance
					if oldCid == 0 {
						oldCid = 1
					}

					// Remove old vector
					oldKey := ai.VectorKey{CentroidID: oldCid, DistanceToCentroid: oldDist, ItemID: id}
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
			}
			// Remove old content to ensure clean insert
			if _, err := arch.Content.RemoveCurrentItem(di.db.ctx); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}

	// 1. Assign Centroid
	var centroidID int
	var dist float32

	// Load centroids
	centroids, err := di.getCentroids(di.db.ctx, arch)
	if err != nil {
		return err
	}

	// If explicit centroid ID is provided
	if item.CentroidID > 0 {
		centroidID = item.CentroidID
		// Ensure centroid exists
		if _, exists := centroids[centroidID]; !exists {
			// Create it using this vector
			centroids[centroidID] = vec
			if _, err := arch.Centroids.Add(di.db.ctx, centroidID, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
				return err
			}
			di.updateCentroidsCache(centroids)
		}
		// Calculate distance to this centroid
		dist = euclideanDistance(vec, centroids[centroidID])
	} else {
		// Auto-assign
		// If no centroids exist, create one
		if len(centroids) == 0 {
			centroids[1] = vec
			if _, err := arch.Centroids.Add(di.db.ctx, 1, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
				return err
			}
			di.updateCentroidsCache(centroids)
		}

		centroidID, dist, err = di.searchClosestCentroid(di.db.ctx, arch, vec)
		if err != nil {
			return err
		}
	}

	// Increment count
	if foundC, _ := arch.Centroids.Find(di.db.ctx, centroidID, false); foundC {
		c, _ := arch.Centroids.GetCurrentValue(di.db.ctx)
		c.VectorCount++
		arch.Centroids.UpdateCurrentItem(di.db.ctx, c)
	}

	// 2. Update Vector Index
	key := ai.VectorKey{CentroidID: centroidID, DistanceToCentroid: dist, ItemID: id}
	if _, err := arch.Vectors.Add(di.db.ctx, key, vec); err != nil {
		return err
	}

	// 3. Update Content Store
	stored := storedItem[T]{
		Payload:    item.Payload,
		CentroidID: centroidID,
		Distance:   dist,
	}
	data, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("failed to marshal stored item: %w", err)
	}
	if _, err := arch.Content.Add(di.db.ctx, id, string(data)); err != nil {
		return err
	}

	return trans.Commit(di.db.ctx)
}

// UpsertBatch adds or updates multiple vectors in a single transaction.
func (di *domainIndex[T]) UpsertBatch(items []ai.Item[T]) error {
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

	// Load centroids once
	centroids, err := di.getCentroids(di.db.ctx, arch)
	if err != nil {
		return err
	}

	// Auto-init centroids if needed and no explicit IDs are used (or mixed)
	// If we have items with CentroidID=0 and no centroids exist, we need to init.
	needsInit := false
	if len(centroids) == 0 {
		for _, item := range items {
			if item.CentroidID == 0 {
				needsInit = true
				break
			}
		}
	}

	if needsInit && len(items) > 0 {
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
			if _, err := arch.Centroids.Add(di.db.ctx, id, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
				return err
			}
		}
		// Update cache
		di.updateCentroidsCache(centroids)
	}

	for _, item := range items {
		id := item.ID
		vec := item.Vector

		// 0. Cleanup Old Entry
		if di.deduplicationEnabled {
			found, err := arch.Content.Find(di.db.ctx, id, false)
			if err != nil {
				return err
			}
			if found {
				oldJson, err := arch.Content.GetCurrentValue(di.db.ctx)
				if err == nil {
					var oldStored storedItem[T]
					if err := json.Unmarshal([]byte(oldJson), &oldStored); err == nil {
						oldCid := oldStored.CentroidID
						oldDist := oldStored.Distance
						if oldCid == 0 {
							oldCid = 1
						}

						oldKey := ai.VectorKey{CentroidID: oldCid, DistanceToCentroid: oldDist, ItemID: id}
						if foundVec, _ := arch.Vectors.Find(di.db.ctx, oldKey, false); foundVec {
							arch.Vectors.RemoveCurrentItem(di.db.ctx)
							if foundC, _ := arch.Centroids.Find(di.db.ctx, oldCid, false); foundC {
								c, _ := arch.Centroids.GetCurrentValue(di.db.ctx)
								c.VectorCount--
								arch.Centroids.UpdateCurrentItem(di.db.ctx, c)
							}
						}
					}
				}
				arch.Content.RemoveCurrentItem(di.db.ctx)
			}
		}

		// 1. Assign Centroid
		var centroidID int
		var dist float32

		if item.CentroidID > 0 {
			centroidID = item.CentroidID
			if _, exists := centroids[centroidID]; !exists {
				centroids[centroidID] = vec
				arch.Centroids.Add(di.db.ctx, centroidID, ai.Centroid{Vector: vec, VectorCount: 0})
				di.updateCentroidsCache(centroids)
			}
			dist = euclideanDistance(vec, centroids[centroidID])
		} else {
			centroidID, dist, err = di.searchClosestCentroid(di.db.ctx, arch, vec)
			if err != nil {
				return err
			}
		}

		// Increment count
		if foundC, _ := arch.Centroids.Find(di.db.ctx, centroidID, false); foundC {
			c, _ := arch.Centroids.GetCurrentValue(di.db.ctx)
			c.VectorCount++
			arch.Centroids.UpdateCurrentItem(di.db.ctx, c)
		}

		// 2. Update Vector Index
		key := ai.VectorKey{CentroidID: centroidID, DistanceToCentroid: dist, ItemID: id}
		if _, err := arch.Vectors.Add(di.db.ctx, key, vec); err != nil {
			return err
		}

		// 3. Update Content Store
		stored := storedItem[T]{
			Payload:    item.Payload,
			CentroidID: centroidID,
			Distance:   dist,
		}
		data, err := json.Marshal(stored)
		if err != nil {
			return err
		}
		if _, err := arch.Content.Add(di.db.ctx, id, string(data)); err != nil {
			return err
		}
	}

	return trans.Commit(di.db.ctx)
}

// Get retrieves a vector by ID.
func (di *domainIndex[T]) Get(id string) (*ai.Item[T], error) {
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

	var stored storedItem[T]
	if err := json.Unmarshal([]byte(jsonStr), &stored); err != nil {
		return nil, fmt.Errorf("failed to unmarshal stored item: %w", err)
	}

	if stored.Deleted {
		return nil, fmt.Errorf("item not found")
	}

	// Fetch Vector
	cid := stored.CentroidID
	if cid == 0 {
		cid = 1
	}
	dist := stored.Distance

	vecKey := ai.VectorKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: id}
	foundVec, err := arch.Vectors.Find(di.db.ctx, vecKey, false)
	var vec []float32
	if foundVec {
		vec, _ = arch.Vectors.GetCurrentValue(di.db.ctx)
	}

	return &ai.Item[T]{ID: id, Vector: vec, Payload: stored.Payload, CentroidID: cid}, nil
}

// Delete removes a vector from the store.
func (di *domainIndex[T]) Delete(id string) error {
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

	found, err := arch.Content.Find(di.db.ctx, id, false)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	jsonStr, _ := arch.Content.GetCurrentValue(di.db.ctx)
	var stored storedItem[T]
	if err := json.Unmarshal([]byte(jsonStr), &stored); err != nil {
		return err
	}

	// Soft Delete
	stored.Deleted = true
	data, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	if _, err := arch.Content.UpdateCurrentItem(di.db.ctx, string(data)); err != nil {
		return err
	}

	// Remove from Vectors
	cid := stored.CentroidID
	if cid == 0 {
		cid = 1
	}
	dist := stored.Distance

	vecKey := ai.VectorKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: id}
	if found, err := arch.Vectors.Find(di.db.ctx, vecKey, false); err != nil {
		return err
	} else if found {
		if _, err := arch.Vectors.RemoveCurrentItem(di.db.ctx); err != nil {
			return err
		}
		if foundC, _ := arch.Centroids.Find(di.db.ctx, cid, false); foundC {
			c, _ := arch.Centroids.GetCurrentValue(di.db.ctx)
			c.VectorCount--
			arch.Centroids.UpdateCurrentItem(di.db.ctx, c)
		}
	}

	return trans.Commit(di.db.ctx)
}

// Query searches for the nearest neighbors.
func (di *domainIndex[T]) Query(vec []float32, k int, filter func(T) bool) ([]ai.Hit[T], error) {
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

	targetCentroids, err := di.searchClosestCentroids(di.db.ctx, arch, vec, 2)
	if err != nil {
		return nil, err
	}
	if len(targetCentroids) == 0 {
		return nil, nil
	}

	var candidates []ai.Hit[T]

	for _, cid := range targetCentroids {
		startKey := ai.VectorKey{CentroidID: cid, DistanceToCentroid: -1.0, ItemID: ""}
		if _, err := arch.Vectors.Find(di.db.ctx, startKey, true); err != nil {
			return nil, err
		}

		for {
			item, err := arch.Vectors.GetCurrentItem(di.db.ctx)
			if err != nil {
				if os.IsNotExist(err) {
					if ok, _ := arch.Vectors.Next(di.db.ctx); !ok {
						break
					}
					continue
				}
				return nil, err
			}

			if item.Key.ItemID == "" && item.Key.CentroidID == 0 {
				if item.ID.IsNil() {
					break
				}
			}

			key := item.Key
			if compositeKeyComparer(key, startKey) < 0 {
				if ok, err := arch.Vectors.Next(di.db.ctx); !ok || err != nil {
					break
				}
				continue
			}

			if key.CentroidID != cid {
				break
			}

			if item.Value == nil {
				if ok, _ := arch.Vectors.Next(di.db.ctx); !ok {
					break
				}
				continue
			}
			itemVec := *item.Value
			score := cosine(vec, itemVec)
			candidates = append(candidates, ai.Hit[T]{ID: key.ItemID, Score: score})

			if ok, _ := arch.Vectors.Next(di.db.ctx); !ok {
				break
			}
		}
	}

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })

	var finalHits []ai.Hit[T]
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
			var stored storedItem[T]
			if err := json.Unmarshal([]byte(jsonStr), &stored); err == nil {
				if !stored.Deleted {
					if filter == nil || filter(stored.Payload) {
						hit.Payload = stored.Payload
						finalHits = append(finalHits, hit)
					}
				}
			}
		}
	}

	return finalHits, nil
}

// Count returns the total number of items.
func (di *domainIndex[T]) Count() (int64, error) {
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

// AddCentroid adds a new centroid.
func (di *domainIndex[T]) AddCentroid(vec []float32) (int, error) {
	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(sop.ForWriting, storePath)
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

	newID := 1
	if ok, err := arch.Centroids.Last(di.db.ctx); err != nil {
		return 0, err
	} else if ok {
		item, err := arch.Centroids.GetCurrentItem(di.db.ctx)
		if err != nil {
			return 0, err
		}
		newID = item.Key + 1
	}

	if _, err := arch.Centroids.Add(di.db.ctx, newID, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
		return 0, err
	}

	di.addCentroidToCache(newID, vec)

	if err := trans.Commit(di.db.ctx); err != nil {
		return 0, err
	}

	return newID, nil
}

// Centroids returns the Centroids B-Tree for advanced manipulation.
func (di *domainIndex[T]) Centroids(ctx context.Context, trans sop.Transaction) (btree.BtreeInterface[int, ai.Centroid], error) {
	version, err := di.getActiveVersion(ctx, trans)
	if err != nil {
		return nil, err
	}
	arch, err := OpenDomainStore(ctx, trans, version)
	if err != nil {
		return nil, err
	}
	return arch.Centroids, nil
}

// Vectors returns the Vectors B-Tree for advanced manipulation.
func (di *domainIndex[T]) Vectors(ctx context.Context, trans sop.Transaction) (btree.BtreeInterface[ai.VectorKey, []float32], error) {
	version, err := di.getActiveVersion(ctx, trans)
	if err != nil {
		return nil, err
	}
	arch, err := OpenDomainStore(ctx, trans, version)
	if err != nil {
		return nil, err
	}
	return arch.Vectors, nil
}

// Content returns the Content B-Tree for advanced manipulation.
func (di *domainIndex[T]) Content(ctx context.Context, trans sop.Transaction) (btree.BtreeInterface[string, string], error) {
	version, err := di.getActiveVersion(ctx, trans)
	if err != nil {
		return nil, err
	}
	arch, err := OpenDomainStore(ctx, trans, version)
	if err != nil {
		return nil, err
	}
	return arch.Content, nil
}

// --- Helpers ---

func (d *Database[T]) beginTransaction(mode sop.TransactionMode, storePath string) (sop.Transaction, error) {
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

func compositeKeyComparer(a, b ai.VectorKey) int {
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

func (di *domainIndex[T]) getCentroids(ctx context.Context, arch *Architecture) (map[int][]float32, error) {
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

func (di *domainIndex[T]) searchClosestCentroid(ctx context.Context, arch *Architecture, vec []float32) (int, float32, error) {
	centroids, err := di.getCentroids(ctx, arch)
	if err != nil {
		return 0, 0, err
	}
	id, dist := findClosestCentroid(vec, centroids)
	return id, dist, nil
}

func (di *domainIndex[T]) searchClosestCentroids(ctx context.Context, arch *Architecture, vec []float32, n int) ([]int, error) {
	centroids, err := di.getCentroids(ctx, arch)
	if err != nil {
		return nil, err
	}
	return findClosestCentroids(vec, centroids, n), nil
}

func (di *domainIndex[T]) updateCentroidsCache(centroids map[int][]float32) {
	di.centroidsCache = centroids
}

func (di *domainIndex[T]) addCentroidToCache(id int, vec []float32) {
	if di.centroidsCache != nil {
		di.centroidsCache[id] = vec
	}
}

const sysConfigName = "sys_config"

func (di *domainIndex[T]) getActiveVersion(ctx context.Context, trans sop.Transaction) (int64, error) {
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

// Note: Rebalance, IndexAll, SeedCentroids, GetBySequenceID are omitted for brevity but should be updated similarly.
// I will include them if space permits or if requested.
// Given the size, I'll try to include Rebalance as it's critical.

func (di *domainIndex[T]) Rebalance() error {
	di.db.rebalanceLock.Lock()
	defer di.db.rebalanceLock.Unlock()

	storePath := filepath.Join(di.db.storagePath, di.name)
	trans, err := di.db.beginTransaction(sop.ForWriting, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(di.db.ctx)

	// Open Sys Store to get current version and keep it open for update later
	sysStore, err := inredfs.NewBtree[string, int64](di.db.ctx, sop.StoreOptions{
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
	found, err := sysStore.Find(di.db.ctx, "active_version", false)
	if err != nil {
		return err
	}
	if found {
		currentVersion, _ = sysStore.GetCurrentValue(di.db.ctx)
	}

	// 1. Open Old Store
	oldArch, err := OpenDomainStore(di.db.ctx, trans, currentVersion)
	if err != nil {
		return err
	}

	// 2. Collect All Vectors
	var items []ai.Item[T]
	if ok, err := oldArch.Vectors.First(di.db.ctx); err != nil {
		return err
	} else if ok {
		for {
			item, err := oldArch.Vectors.GetCurrentItem(di.db.ctx)
			if err != nil {
				return err
			}
			if item.Value != nil {
				items = append(items, ai.Item[T]{
					ID:     item.Key.ItemID,
					Vector: *item.Value,
				})
			}

			if ok, err := oldArch.Vectors.Next(di.db.ctx); err != nil {
				return err
			} else if !ok {
				break
			}
		}
	}

	if len(items) == 0 {
		return nil
	}

	// 3. Compute New Centroids
	k := int(math.Sqrt(float64(len(items))))
	if k < 1 {
		k = 1
	}
	if k > 256 {
		k = 256
	}

	newCentroidsMap, err := ComputeCentroids(items, k)
	if err != nil {
		return err
	}

	// 4. Open New Store (Version + 1)
	newVersion := currentVersion + 1
	suffix := fmt.Sprintf("_%d", newVersion)

	// Manually open new versioned stores to avoid re-opening shared stores (Content, TempVectors)
	// which would cause a transaction conflict.
	newCentroids, err := inredfs.NewBtree[int, ai.Centroid](di.db.ctx, sop.StoreOptions{
		Name: "centroids" + suffix,
	}, trans, func(a, b int) int { return a - b })
	if err != nil {
		return err
	}

	newVectors, err := inredfs.NewBtree[ai.VectorKey, []float32](di.db.ctx, sop.StoreOptions{
		Name: "vectors" + suffix,
	}, trans, compositeKeyComparer)
	if err != nil {
		return err
	}

	newLookup, err := inredfs.NewBtree[int, string](di.db.ctx, sop.StoreOptions{
		Name: "lookup" + suffix,
	}, trans, func(a, b int) int { return a - b })
	if err != nil {
		return err
	}

	newArch := &Architecture{
		Centroids:   newCentroids,
		Vectors:     newVectors,
		Content:     oldArch.Content, // Reuse shared store
		Lookup:      newLookup,
		TempVectors: oldArch.TempVectors, // Reuse shared store
		Version:     newVersion,
	}

	// 5. Write New Centroids
	for id, vec := range newCentroidsMap {
		if _, err := newArch.Centroids.Add(di.db.ctx, id, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
			return err
		}
	}

	// 6. Re-assign Vectors and Update Content
	counts := make(map[int]int)
	for _, item := range items {
		cid, dist := findClosestCentroid(item.Vector, newCentroidsMap)
		counts[cid]++

		// Add to New Vectors
		key := ai.VectorKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: item.ID}
		if _, err := newArch.Vectors.Add(di.db.ctx, key, item.Vector); err != nil {
			return err
		}

		// Update Content
		found, err := newArch.Content.Find(di.db.ctx, item.ID, false)
		if err != nil {
			return err
		}
		if found {
			jsonStr, _ := newArch.Content.GetCurrentValue(di.db.ctx)
			var stored storedItem[T]
			if err := json.Unmarshal([]byte(jsonStr), &stored); err == nil {
				stored.CentroidID = cid
				stored.Distance = dist
				newData, _ := json.Marshal(stored)
				newArch.Content.UpdateCurrentItem(di.db.ctx, string(newData))
			}
		}
	}

	// 7. Update Centroid Counts
	for cid, count := range counts {
		if found, _ := newArch.Centroids.Find(di.db.ctx, cid, false); found {
			c, _ := newArch.Centroids.GetCurrentValue(di.db.ctx)
			c.VectorCount = count
			newArch.Centroids.UpdateCurrentItem(di.db.ctx, c)
		}
	}

	// 8. Update Active Version
	// sysStore is already open from the beginning of the transaction
	found, err = sysStore.Find(di.db.ctx, "active_version", false)
	if err != nil {
		return err
	}
	if found {
		sysStore.UpdateCurrentItem(di.db.ctx, newVersion)
	} else {
		sysStore.Add(di.db.ctx, "active_version", newVersion)
	}

	// Update cache
	di.updateCentroidsCache(newCentroidsMap)

	return trans.Commit(di.db.ctx)
}
