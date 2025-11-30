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
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/inredfs"
	"github.com/sharedcode/sop/redis"
)

// Database is a persistent vector database manager backed by SOP B-Trees.
// It manages the storage and retrieval of vectors and their associated metadata.
// It supports transactions and caching for performance.
type Database[T any] struct {
	cache         sop.L2Cache
	readMode      sop.TransactionMode
	usageMode     ai.UsageMode
	storagePath   string
	optimizeLock  sync.Mutex
	contentSize   sop.ValueDataSize
	dbType        ai.DatabaseType
	erasureConfig map[string]fs.ErasureCodingConfig
	storesFolders []string
}

// NewDatabase creates a new vector database manager.
// It initializes the cache based on the provided type.
func NewDatabase[T any](dbType ai.DatabaseType) *Database[T] {
	var c sop.L2Cache
	if dbType == ai.Clustered {
		c = redis.NewClient()
	} else {
		// Use InMemory cache for Standalone to support locking required by StoreRepository
		c = cache.NewInMemoryCache()
	}

	return &Database[T]{
		cache:       c,
		readMode:    sop.NoCheck,
		usageMode:   ai.BuildOnceQueryMany, // Default to most common AI pattern
		storagePath: "",
		contentSize: sop.MediumData,
		dbType:      dbType,
	}
}

// SetUsageMode configures the usage pattern for the database.
func (d *Database[T]) SetUsageMode(mode ai.UsageMode) {
	d.usageMode = mode
}

// SetContentSize configures the value data size for the content store.
func (d *Database[T]) SetContentSize(size sop.ValueDataSize) {
	d.contentSize = size
}

// SetStoragePath configures the file system path for data persistence.
func (d *Database[T]) SetStoragePath(path string) {
	d.storagePath = path
}

// SetReadMode configures the transaction mode for Query operations.
func (d *Database[T]) SetReadMode(mode sop.TransactionMode) {
	d.readMode = mode
}

// SetReplicationConfig configures the replication settings for the database.
func (d *Database[T]) SetReplicationConfig(ec map[string]fs.ErasureCodingConfig, folders []string) {
	d.erasureConfig = ec
	d.storesFolders = folders
}

// Open returns an Index for the specified domain.
// It verifies that the database configuration matches the persisted state.
func (d *Database[T]) Open(ctx context.Context, domain string) ai.VectorStore[T] {
	// We no longer enforce strict config matching to allow flexibility.
	// We just ensure the config file exists for informational purposes.
	_ = d.ensureConfig()

	return &domainIndex[T]{
		db:                   d,
		name:                 domain,
		deduplicationEnabled: true,
	}
}

type dbConfig struct {
	Type string `json:"type"`
}

func (d *Database[T]) ensureConfig() error {
	if d.storagePath == "" {
		return nil // In-memory only or not yet configured
	}

	configPath := filepath.Join(d.storagePath, "sop_ai_config.json")
	if err := os.MkdirAll(d.storagePath, 0755); err != nil {
		return err
	}

	// Check if config exists
	data, err := os.ReadFile(configPath)
	if os.IsNotExist(err) {
		// Create new config
		typeStr := "Standalone"
		if d.dbType == ai.Clustered {
			typeStr = "Clustered"
		}
		cfg := dbConfig{Type: typeStr}
		bytes, _ := json.MarshalIndent(cfg, "", "  ")
		return os.WriteFile(configPath, bytes, 0644)
	} else if err != nil {
		return err
	}

	// We read it but don't enforce it anymore.
	var cfg dbConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Optional: We could update the config file if the type changed?
	// For now, we just leave it as is or maybe log a warning if we had a logger.
	return nil
}

// domainIndex implements the Index interface for a specific domain using the 3-table layout.
type domainIndex[T any] struct {
	db   *Database[T]
	name string
	// centroidsCache caches the centroids to avoid reloading them from the B-Tree on every operation.
	centroidsCache       map[int][]float32
	deduplicationEnabled bool
	externalTrans        sop.Transaction
}

// SetDeduplication enables or disables the internal deduplication check during Upsert.
func (di *domainIndex[T]) SetDeduplication(enabled bool) {
	di.deduplicationEnabled = enabled
}

// WithTransaction returns a new instance of the store bound to the provided transaction.
func (di *domainIndex[T]) WithTransaction(trans sop.Transaction) ai.VectorStore[T] {
	return &domainIndex[T]{
		db:                   di.db,
		name:                 di.name,
		centroidsCache:       di.centroidsCache,
		deduplicationEnabled: di.deduplicationEnabled,
		externalTrans:        trans,
	}
}

func (di *domainIndex[T]) getTransaction(ctx context.Context, mode sop.TransactionMode) (sop.Transaction, bool, error) {
	if di.externalTrans != nil {
		return di.externalTrans, false, nil
	}
	// Use the database root path for the transaction to allow global access (e.g. sys_config)
	// and domain-specific sub-stores.
	storePath := di.db.storagePath
	trans, err := di.db.beginTransaction(ctx, mode, storePath)
	return trans, true, err
}

// storedItem wraps the user payload with system metadata.
type storedItem[T any] struct {
	Payload    T       `json:"payload"`
	CentroidID int     `json:"_centroid_id"`
	Distance   float32 `json:"_distance"`
	Deleted    bool    `json:"_deleted,omitempty"`
}

func (di *domainIndex[T]) upsertItem(ctx context.Context, arch *Architecture, item ai.Item[T], centroids map[int][]float32) error {
	id := item.ID
	vec := item.Vector

	// 0. Cleanup Old Entry (if exists) to prevent "ghost" vectors
	if di.deduplicationEnabled {
		found, err := arch.Content.Find(ctx, id, false)
		if err != nil {
			return err
		}
		if found {
			// Retrieve old metadata to find old vector location
			oldJson, err := arch.Content.GetCurrentValue(ctx)
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
					if foundVec, _ := arch.Vectors.Find(ctx, oldKey, false); foundVec {
						if _, err := arch.Vectors.RemoveCurrentItem(ctx); err != nil {
							return err
						}
						// Decrement count of old centroid
						if foundC, _ := arch.Centroids.Find(ctx, oldCid, false); foundC {
							c, _ := arch.Centroids.GetCurrentValue(ctx)
							c.VectorCount--
							arch.Centroids.UpdateCurrentItem(ctx, c)
						}
					}
				}
			}
			// Remove old content to ensure clean insert
			if _, err := arch.Content.RemoveCurrentItem(ctx); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}

	// Optimization: In BuildOnceQueryMany mode, we stage vectors in TempVectors for faster ingestion.
	if di.db.usageMode == ai.BuildOnceQueryMany {
		if _, err := arch.TempVectors.Add(ctx, id, vec); err != nil {
			return err
		}

		stored := storedItem[T]{
			Payload:    item.Payload,
			CentroidID: 0,
			Distance:   0,
		}
		data, err := json.Marshal(stored)
		if err != nil {
			return fmt.Errorf("failed to marshal stored item: %w", err)
		}
		if _, err := arch.Content.Add(ctx, id, string(data)); err != nil {
			return err
		}
		return nil
	}

	// 1. Assign Centroid
	var centroidID int
	var dist float32

	// If explicit centroid ID is provided
	if item.CentroidID > 0 {
		centroidID = item.CentroidID
		// Ensure centroid exists
		if _, exists := centroids[centroidID]; !exists {
			// Create it using this vector
			centroids[centroidID] = vec
			if _, err := arch.Centroids.Add(ctx, centroidID, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
				return err
			}
			di.updateCentroidsCache(centroids)
		}
		// Calculate distance to this centroid
		dist = euclideanDistance(vec, centroids[centroidID])
	} else {
		// Auto-assign
		// If no centroids exist, create one (Fallback for single Upsert)
		if len(centroids) == 0 {
			centroids[1] = vec
			if _, err := arch.Centroids.Add(ctx, 1, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
				return err
			}
			di.updateCentroidsCache(centroids)
		}

		var err error
		centroidID, dist, err = di.searchClosestCentroid(ctx, arch, vec)
		if err != nil {
			return err
		}
	}

	// Increment count
	if foundC, _ := arch.Centroids.Find(ctx, centroidID, false); foundC {
		c, _ := arch.Centroids.GetCurrentValue(ctx)
		c.VectorCount++
		arch.Centroids.UpdateCurrentItem(ctx, c)
	}

	// 2. Update Vector Index
	key := ai.VectorKey{CentroidID: centroidID, DistanceToCentroid: dist, ItemID: id}
	if _, err := arch.Vectors.Add(ctx, key, vec); err != nil {
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
	if _, err := arch.Content.Add(ctx, id, string(data)); err != nil {
		return err
	}

	return nil
}

// Upsert adds or updates a vector in the store.
func (di *domainIndex[T]) Upsert(ctx context.Context, item ai.Item[T]) error {
	trans, isOwn, err := di.getTransaction(ctx, sop.ForWriting)
	if err != nil {
		return err
	}
	if isOwn {
		defer trans.Rollback(ctx)
	}

	version, err := di.getActiveVersion(ctx, trans)
	if err != nil {
		return err
	}

	arch, err := OpenDomainStore(ctx, trans, di.name, version, di.db.contentSize)
	if err != nil {
		return err
	}

	var centroids map[int][]float32
	if di.db.usageMode != ai.BuildOnceQueryMany {
		centroids, err = di.getCentroids(ctx, arch)
		if err != nil {
			return err
		}
	}

	if err := di.upsertItem(ctx, arch, item, centroids); err != nil {
		return err
	}

	if isOwn {
		return trans.Commit(ctx)
	}
	return nil
}

// UpsertBatch adds or updates multiple vectors in a single transaction.
func (di *domainIndex[T]) UpsertBatch(ctx context.Context, items []ai.Item[T]) error {
	trans, isOwn, err := di.getTransaction(ctx, sop.ForWriting)
	if err != nil {
		return err
	}
	if isOwn {
		defer trans.Rollback(ctx)
	}

	version, err := di.getActiveVersion(ctx, trans)
	if err != nil {
		return err
	}

	arch, err := OpenDomainStore(ctx, trans, di.name, version, di.db.contentSize)
	if err != nil {
		return err
	}

	var centroids map[int][]float32
	if di.db.usageMode != ai.BuildOnceQueryMany {
		// Load centroids once
		centroids, err = di.getCentroids(ctx, arch)
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
				if _, err := arch.Centroids.Add(ctx, id, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
					return err
				}
			}
			// Update cache
			di.updateCentroidsCache(centroids)
		}
	}

	for _, item := range items {
		if err := di.upsertItem(ctx, arch, item, centroids); err != nil {
			return err
		}
	}

	if isOwn {
		return trans.Commit(ctx)
	}
	return nil
}

// Get retrieves a vector by ID.
func (di *domainIndex[T]) Get(ctx context.Context, id string) (*ai.Item[T], error) {
	trans, isOwn, err := di.getTransaction(ctx, di.db.readMode)
	if err != nil {
		return nil, err
	}
	if isOwn {
		defer trans.Rollback(ctx)
	}

	version, err := di.getActiveVersion(ctx, trans)
	if err != nil {
		return nil, err
	}

	arch, err := OpenDomainStore(ctx, trans, di.name, version, di.db.contentSize)
	if err != nil {
		return nil, err
	}

	found, err := arch.Content.Find(ctx, id, false)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("item not found")
	}

	jsonStr, err := arch.Content.GetCurrentValue(ctx)
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
	var vec []float32

	if cid == 0 {
		// Try TempVectors first for unoptimized items
		if found, err := arch.TempVectors.Find(ctx, id, false); err != nil {
			return nil, err
		} else if found {
			vec, _ = arch.TempVectors.GetCurrentValue(ctx)
			return &ai.Item[T]{ID: id, Vector: vec, Payload: stored.Payload, CentroidID: 0}, nil
		}
		// Fallback: If not in TempVectors, assume it's in default centroid 1 (legacy behavior)
		cid = 1
	}
	dist := stored.Distance

	vecKey := ai.VectorKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: id}
	foundVec, err := arch.Vectors.Find(ctx, vecKey, false)
	if foundVec {
		vec, _ = arch.Vectors.GetCurrentValue(ctx)
	}

	return &ai.Item[T]{ID: id, Vector: vec, Payload: stored.Payload, CentroidID: cid}, nil
}

// Delete removes a vector from the store.
func (di *domainIndex[T]) Delete(ctx context.Context, id string) error {
	trans, isOwn, err := di.getTransaction(ctx, sop.ForWriting)
	if err != nil {
		return err
	}
	if isOwn {
		defer trans.Rollback(ctx)
	}

	version, err := di.getActiveVersion(ctx, trans)
	if err != nil {
		return err
	}

	arch, err := OpenDomainStore(ctx, trans, di.name, version, di.db.contentSize)
	if err != nil {
		return err
	}

	found, err := arch.Content.Find(ctx, id, false)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	jsonStr, _ := arch.Content.GetCurrentValue(ctx)
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
	if _, err := arch.Content.UpdateCurrentItem(ctx, string(data)); err != nil {
		return err
	}

	// Remove from Vectors
	cid := stored.CentroidID
	if cid == 0 {
		cid = 1
	}
	dist := stored.Distance

	vecKey := ai.VectorKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: id}
	if found, err := arch.Vectors.Find(ctx, vecKey, false); err != nil {
		return err
	} else if found {
		if _, err := arch.Vectors.RemoveCurrentItem(ctx); err != nil {
			return err
		}
		if foundC, _ := arch.Centroids.Find(ctx, cid, false); foundC {
			c, _ := arch.Centroids.GetCurrentValue(ctx)
			c.VectorCount--
			arch.Centroids.UpdateCurrentItem(ctx, c)
		}
	}

	if isOwn {
		return trans.Commit(ctx)
	}
	return nil
}

// Query searches for the nearest neighbors.
func (di *domainIndex[T]) Query(ctx context.Context, vec []float32, k int, filter func(T) bool) ([]ai.Hit[T], error) {
	trans, isOwn, err := di.getTransaction(ctx, di.db.readMode)
	if err != nil {
		return nil, err
	}
	if isOwn {
		defer trans.Rollback(ctx)
	}

	version, err := di.getActiveVersion(ctx, trans)
	if err != nil {
		return nil, err
	}

	arch, err := OpenDomainStore(ctx, trans, di.name, version, di.db.contentSize)
	if err != nil {
		return nil, err
	}

	targetCentroids, err := di.searchClosestCentroids(ctx, arch, vec, 2)
	if err != nil {
		return nil, err
	}

	var candidates []ai.Hit[T]

	// 1. Search Indexed Vectors (if any centroids exist)
	if len(targetCentroids) > 0 {
		for _, cid := range targetCentroids {
			startKey := ai.VectorKey{CentroidID: cid, DistanceToCentroid: -1.0, ItemID: ""}
			if _, err := arch.Vectors.Find(ctx, startKey, true); err != nil {
				return nil, err
			}

			for {
				item, err := arch.Vectors.GetCurrentItem(ctx)
				if err != nil {
					if os.IsNotExist(err) {
						if ok, _ := arch.Vectors.Next(ctx); !ok {
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
					if ok, err := arch.Vectors.Next(ctx); !ok || err != nil {
						break
					}
					continue
				}

				if key.CentroidID != cid {
					break
				}

				if item.Value == nil {
					if ok, _ := arch.Vectors.Next(ctx); !ok {
						break
					}
					continue
				}
				itemVec := *item.Value
				score := cosine(vec, itemVec)
				candidates = append(candidates, ai.Hit[T]{ID: key.ItemID, Score: score})

				if ok, _ := arch.Vectors.Next(ctx); !ok {
					break
				}
			}
		}
	}

	// 2. Search TempVectors (Brute Force)
	// This ensures we find items that are staged but not yet optimized.
	if ok, err := arch.TempVectors.First(ctx); err != nil {
		return nil, err
	} else if ok {
		for {
			item, err := arch.TempVectors.GetCurrentItem(ctx)
			if err != nil {
				return nil, err
			}

			score := cosine(vec, *item.Value)
			candidates = append(candidates, ai.Hit[T]{ID: item.Key, Score: score})

			if ok, err := arch.TempVectors.Next(ctx); err != nil {
				return nil, err
			} else if !ok {
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

		found, err := arch.Content.Find(ctx, hit.ID, false)
		if err != nil {
			return nil, err
		}
		if found {
			jsonStr, _ := arch.Content.GetCurrentValue(ctx)
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
func (di *domainIndex[T]) Count(ctx context.Context) (int64, error) {
	trans, isOwn, err := di.getTransaction(ctx, di.db.readMode)
	if err != nil {
		return 0, err
	}
	if isOwn {
		defer trans.Rollback(ctx)
	}

	version, err := di.getActiveVersion(ctx, trans)
	if err != nil {
		return 0, err
	}

	arch, err := OpenDomainStore(ctx, trans, di.name, version, di.db.contentSize)
	if err != nil {
		return 0, err
	}

	return arch.Content.Count(), nil
}

// AddCentroid adds a new centroid.
func (di *domainIndex[T]) AddCentroid(ctx context.Context, vec []float32) (int, error) {
	trans, isOwn, err := di.getTransaction(ctx, sop.ForWriting)
	if err != nil {
		return 0, err
	}
	if isOwn {
		defer trans.Rollback(ctx)
	}

	version, err := di.getActiveVersion(ctx, trans)
	if err != nil {
		return 0, err
	}

	arch, err := OpenDomainStore(ctx, trans, di.name, version, di.db.contentSize)
	if err != nil {
		return 0, err
	}

	newID := 1
	if ok, err := arch.Centroids.Last(ctx); err != nil {
		return 0, err
	} else if ok {
		item, err := arch.Centroids.GetCurrentItem(ctx)
		if err != nil {
			return 0, err
		}
		newID = item.Key + 1
	}

	if _, err := arch.Centroids.Add(ctx, newID, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
		return 0, err
	}

	di.addCentroidToCache(newID, vec)

	if isOwn {
		if err := trans.Commit(ctx); err != nil {
			return 0, err
		}
	}

	return newID, nil
}

// Centroids returns the Centroids B-Tree for advanced manipulation.
func (di *domainIndex[T]) Centroids(ctx context.Context, trans sop.Transaction) (btree.BtreeInterface[int, ai.Centroid], error) {
	version, err := di.getActiveVersion(ctx, trans)
	if err != nil {
		return nil, err
	}
	arch, err := OpenDomainStore(ctx, trans, di.name, version, di.db.contentSize)
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
	arch, err := OpenDomainStore(ctx, trans, di.name, version, di.db.contentSize)
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
	arch, err := OpenDomainStore(ctx, trans, di.name, version, di.db.contentSize)
	if err != nil {
		return nil, err
	}
	return arch.Content, nil
}

// --- Helpers ---

func (d *Database[T]) beginTransaction(ctx context.Context, mode sop.TransactionMode, storePath string) (sop.Transaction, error) {
	storeFolder := storePath
	if err := os.MkdirAll(storeFolder, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data folder %s: %w", storeFolder, err)
	}

	var c sop.L2Cache
	if d.dbType == ai.Clustered {
		c = redis.NewClient()
	} else {
		c = cache.NewInMemoryCache()
	}

	if len(d.storesFolders) > 0 {
		to, err := inredfs.NewTransactionOptionsWithReplication(mode, -1, -1, d.storesFolders, d.erasureConfig)
		if err != nil {
			return nil, err
		}
		to.Cache = c
		trans, err := inredfs.NewTransactionWithReplication(ctx, to)
		if err != nil {
			return nil, err
		}
		if err := trans.Begin(ctx); err != nil {
			return nil, fmt.Errorf("transaction begin failed: %w", err)
		}
		return trans, nil
	}

	to, err := inredfs.NewTransactionOptions(storeFolder, mode, -1, -1)
	if err != nil {
		return nil, err
	}
	to.Cache = c

	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		return nil, err
	}

	if err := trans.Begin(ctx); err != nil {
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

const sysConfigName = "vector_sys_config"

func (di *domainIndex[T]) getActiveVersion(ctx context.Context, trans sop.Transaction) (int64, error) {
	store, err := newBtree[string, int64](ctx, sop.ConfigureStore(sysConfigName, true, 1000, "System Config", sop.SmallData, ""), trans, func(a, b string) int {
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
	found, err := store.Find(ctx, di.name, false)
	if err != nil {
		return 0, err
	}
	if found {
		return store.GetCurrentValue(ctx)
	}
	return 0, nil
}

// Note: Optimize, IndexAll, SeedCentroids, GetBySequenceID are omitted for brevity but should be updated similarly.
// I will include them if space permits or if requested.

func (di *domainIndex[T]) Optimize(ctx context.Context) error {
	di.db.optimizeLock.Lock()
	defer di.db.optimizeLock.Unlock()

	// Use the database root path for the transaction to allow global access
	storePath := di.db.storagePath
	trans, err := di.db.beginTransaction(ctx, sop.ForWriting, storePath)
	if err != nil {
		return err
	}
	defer trans.Rollback(ctx)

	// Open Sys Store to get current version and keep it open for update later
	sysStore, err := newBtree[string, int64](ctx, sop.ConfigureStore(sysConfigName, true, 1000, "System Config", sop.SmallData, ""), trans, func(a, b string) int {
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
	found, err := sysStore.Find(ctx, di.name, false)
	if err != nil {
		return err
	}
	if found {
		currentVersion, _ = sysStore.GetCurrentValue(ctx)
	}

	// 1. Open Old Store
	oldArch, err := OpenDomainStore(ctx, trans, di.name, currentVersion, di.db.contentSize)
	if err != nil {
		return err
	}

	// 2. Prepare New Stores (Version + 1)
	newVersion := currentVersion + 1
	suffix := fmt.Sprintf("_%d", newVersion)

	newCentroids, err := newBtree[int, ai.Centroid](ctx, sop.ConfigureStore(di.name+"_centroids"+suffix, true, 100, "Centroids", sop.SmallData, ""), trans, func(a, b int) int { return a - b })
	if err != nil {
		return err
	}

	newVectors, err := newBtree[ai.VectorKey, []float32](ctx, sop.ConfigureStore(di.name+"_vecs"+suffix, true, 1000, "Vectors", sop.SmallData, ""), trans, compositeKeyComparer)
	if err != nil {
		return err
	}

	newLookup, err := newBtree[int, string](ctx, sop.ConfigureStore(di.name+"_lku"+suffix, true, 1000, "Lookup", sop.SmallData, ""), trans, func(a, b int) int { return a - b })
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

	// 3. Pass 1: Build Lookup Table
	// We iterate both TempVectors (staging) and Vectors (indexed) to build the sequence lookup.
	var count int
	var processedTempVectors bool

	// 3a. Iterate Vectors
	if ok, err := oldArch.Vectors.First(ctx); err != nil {
		return err
	} else if ok {
		for {
			item, err := oldArch.Vectors.GetCurrentItem(ctx)
			if err != nil {
				return err
			}

			// Add to Lookup (Sequence ID -> Item ID)
			if _, err := newArch.Lookup.Add(ctx, count, item.Key.ItemID); err != nil {
				return err
			}
			count++

			if ok, err := oldArch.Vectors.Next(ctx); err != nil {
				return err
			} else if !ok {
				break
			}
		}
	}

	// 3b. Iterate TempVectors (Only if Vectors was empty)
	if count == 0 {
		processedTempVectors = true
		if ok, err := oldArch.TempVectors.First(ctx); err != nil {
			return err
		} else if ok {
			for {
				item, err := oldArch.TempVectors.GetCurrentItem(ctx)
				if err != nil {
					return err
				}

				// Check if item is already in Vectors (via Content) to avoid duplicates
				shouldProcess := true
				if found, err := oldArch.Content.Find(ctx, item.Key, false); err != nil {
					return err
				} else if found {
					jsonStr, _ := oldArch.Content.GetCurrentValue(ctx)
					var stored storedItem[T]
					if err := json.Unmarshal([]byte(jsonStr), &stored); err == nil {
						if stored.CentroidID != 0 {
							shouldProcess = false
						}
					}
				}

				if shouldProcess {
					// Add to Lookup (Sequence ID -> Item ID)
					if _, err := newArch.Lookup.Add(ctx, count, item.Key); err != nil {
						return err
					}
					count++
				}

				if ok, err := oldArch.TempVectors.Next(ctx); err != nil {
					return err
				} else if !ok {
					break
				}
			}
		}
	}

	if count == 0 {
		return nil
	}

	// 4. Sampling using Lookup
	// We use the Lookup table to fetch random samples for K-Means clustering.
	const maxSamples = 50000
	var samples []ai.Item[T]
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Determine how many samples to take
	sampleCount := maxSamples
	if count < maxSamples {
		sampleCount = count
	}

	// Generate random indices
	indices := make(map[int]struct{})
	if count <= maxSamples {
		for i := 0; i < count; i++ {
			indices[i] = struct{}{}
		}
	} else {
		for len(indices) < sampleCount {
			idx := rng.Intn(count)
			indices[idx] = struct{}{}
		}
	}

	// Fetch samples
	for idx := range indices {
		// 4a. Get ItemID from Lookup
		if found, err := newArch.Lookup.Find(ctx, idx, false); err != nil {
			return err
		} else if !found {
			continue
		}
		itemID, err := newArch.Lookup.GetCurrentValue(ctx)
		if err != nil {
			return err
		}

		// 4b. Fetch Vector
		// Priority 1: Try TempVectors (Fastest)
		var vec []float32
		foundInTemp, err := oldArch.TempVectors.Find(ctx, itemID, false)
		if err != nil {
			return err
		}

		if foundInTemp {
			vec, err = oldArch.TempVectors.GetCurrentValue(ctx)
			if err != nil {
				return err
			}
		} else {
			// Priority 2: Try Vectors (Requires Content Lookup)
			if found, err := oldArch.Content.Find(ctx, itemID, false); err != nil {
				return err
			} else if !found {
				continue
			}

			contentJson, err := oldArch.Content.GetCurrentValue(ctx)
			if err != nil {
				return err
			}

			var stored storedItem[T]
			if err := json.Unmarshal([]byte(contentJson), &stored); err != nil {
				return err
			}

			vecKey := ai.VectorKey{
				CentroidID:         stored.CentroidID,
				DistanceToCentroid: stored.Distance,
				ItemID:             itemID,
			}

			if found, err := oldArch.Vectors.Find(ctx, vecKey, false); err != nil {
				return err
			} else if !found {
				continue
			}

			vec, err = oldArch.Vectors.GetCurrentValue(ctx)
			if err != nil {
				return err
			}
		}

		samples = append(samples, ai.Item[T]{
			ID:     itemID,
			Vector: vec,
		})
	}

	// 5. Compute New Centroids using Samples
	k := int(math.Sqrt(float64(count)))
	if k < 1 {
		k = 1
	}
	if k > 256 {
		k = 256
	}

	// Use the samples as the training set
	newCentroidsMap, err := ComputeCentroids(samples, k)
	if err != nil {
		return err
	}

	// 6. Write New Centroids
	for id, vec := range newCentroidsMap {
		if _, err := newArch.Centroids.Add(ctx, id, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
			return err
		}
	}

	// 7. Pass 2: Re-assign Vectors and Update Content (Streaming)
	counts := make(map[int]int)

	// Helper to process an item
	processItem := func(id string, vec []float32) error {
		cid, dist := findClosestCentroid(vec, newCentroidsMap)
		counts[cid]++

		// Add to New Vectors
		key := ai.VectorKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: id}
		if _, err := newArch.Vectors.Add(ctx, key, vec); err != nil {
			return err
		}

		// Update Content
		found, err := newArch.Content.Find(ctx, id, false)
		if err != nil {
			return err
		}
		if found {
			jsonStr, _ := newArch.Content.GetCurrentValue(ctx)
			var stored storedItem[T]
			if err := json.Unmarshal([]byte(jsonStr), &stored); err == nil {
				stored.CentroidID = cid
				stored.Distance = dist
				newData, _ := json.Marshal(stored)
				newArch.Content.UpdateCurrentItem(ctx, string(newData))
			}
		}
		return nil
	}

	// 7a. Stream Vectors
	if ok, err := oldArch.Vectors.First(ctx); err != nil {
		return err
	} else if ok {
		for {
			item, err := oldArch.Vectors.GetCurrentItem(ctx)
			if err != nil {
				return err
			}

			if err := processItem(item.Key.ItemID, *item.Value); err != nil {
				return err
			}

			if ok, err := oldArch.Vectors.Next(ctx); err != nil {
				return err
			} else if !ok {
				break
			}
		}
	}

	// 7b. Stream TempVectors
	if processedTempVectors {
		if ok, err := oldArch.TempVectors.First(ctx); err != nil {
			return err
		} else if ok {
			for {
				item, err := oldArch.TempVectors.GetCurrentItem(ctx)
				if err != nil {
					return err
				}

				// Check if item is already in Vectors (via Content)
				shouldProcess := true
				if found, err := oldArch.Content.Find(ctx, item.Key, false); err != nil {
					return err
				} else if found {
					jsonStr, _ := oldArch.Content.GetCurrentValue(ctx)
					var stored storedItem[T]
					if err := json.Unmarshal([]byte(jsonStr), &stored); err == nil {
						if stored.CentroidID != 0 {
							shouldProcess = false
						}
					}
				}

				if shouldProcess {
					if err := processItem(item.Key, *item.Value); err != nil {
						return err
					}
				}

				if ok, err := oldArch.TempVectors.Next(ctx); err != nil {
					return err
				} else if !ok {
					break
				}
			}
		}
	}

	// 8. Update Centroid Counts
	if di.db.usageMode == ai.DynamicWithVectorCountTracking {
		for cid, count := range counts {
			if found, _ := newArch.Centroids.Find(ctx, cid, false); found {
				c, _ := newArch.Centroids.GetCurrentValue(ctx)
				c.VectorCount = count
				newArch.Centroids.UpdateCurrentItem(ctx, c)
			}
		}
	}

	// 9. Update Active Version
	// sysStore is already open from the beginning of the transaction
	found, err = sysStore.Find(ctx, di.name, false)
	if err != nil {
		return err
	}
	if found {
		sysStore.UpdateCurrentItem(ctx, newVersion)
	} else {
		sysStore.Add(ctx, di.name, newVersion)
	}

	// Update cache
	di.updateCentroidsCache(newCentroidsMap)

	if err := trans.Commit(ctx); err != nil {
		return err
	}

	// 10. Cleanup TempVectors Store (File System)
	// Since we have successfully migrated everything to Vectors, we can delete the TempVectors store.
	// This avoids the expensive item-by-item deletion.
	if processedTempVectors && di.db.storagePath != "" {
		storeName := fmt.Sprintf("%s_temp_vectors", di.name)
		if err := inredfs.RemoveBtree(ctx, di.db.storagePath, storeName, di.db.cache); err != nil {
			fmt.Printf("Failed to remove TempVectors: %v\n", err)
		} else {
			fmt.Printf("Removed TempVectors %s\n", storeName)
		}
	}

	return nil
}
