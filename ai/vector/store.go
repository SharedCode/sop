package vector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/inredfs"
)

const (
	sysConfigSuffix   = "_sys_config"
	lookupSuffix      = "_lku"
	centroidsSuffix   = "_centroids"
	vectorsSuffix     = "_vecs"
	tempVectorsSuffix = "_tmp_vecs"
	dataSuffix        = "_data"

	sysConfigDesc   = "System Config"
	lookupDesc      = "Lookup"
	centroidsDesc   = "Centroids"
	vectorsDesc     = "Vectors"
	tempVectorsDesc = "Temp Vectors"
	dataDesc        = "Content"
)

// Config holds the configuration for the Vector Store.
type Config struct {
	Cache         sop.L2Cache
	StoragePath   string
	ContentSize   sop.ValueDataSize
	UsageMode     ai.UsageMode
	StoresFolders []string
	ErasureConfig map[string]fs.ErasureCodingConfig
	// EnableIngestionBuffer enables the initial "TempVectors" stage (Stage 0)
	// which buffers vectors for faster ingestion (O(1)) before they are indexed.
	// If false (default), vectors are written directly to the main index (Stage 1),
	// which is slower for ingestion but allows immediate querying and structure.
	EnableIngestionBuffer bool
}

// Open returns an Index for the specified domain.
// It verifies that the database configuration matches the persisted state.
func Open[T any](ctx context.Context, trans sop.Transaction, domain string, cfg Config) (ai.VectorStore[T], error) {
	sysStoreName := fmt.Sprintf("%s%s", domain, sysConfigSuffix)
	sysStore, err := newBtree[string, int64](ctx, sop.ConfigureStore(sysStoreName, true, 1000, sysConfigDesc, sop.SmallData, ""), trans, func(a, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	if err != nil {
		return nil, err
	}

	return &domainIndex[T]{
		config:               cfg,
		name:                 domain,
		deduplicationEnabled: true,
		trans:                trans,
		sysStore:             sysStore,
	}, nil
}

// domainIndex implements the Index interface for a specific domain using the 3-table layout.
type domainIndex[T any] struct {
	config Config
	name   string
	// centroidsCache caches the centroids to avoid reloading them from the B-Tree on every operation.
	centroidsCache       map[int][]float32
	deduplicationEnabled bool
	trans                sop.Transaction
	sysStore             btree.BtreeInterface[string, int64]
	archCache            map[int64]*Architecture
}

// SetDeduplication enables or disables the internal deduplication check during Upsert.
func (di *domainIndex[T]) SetDeduplication(enabled bool) {
	di.deduplicationEnabled = enabled
}

func (di *domainIndex[T]) getArchitecture(ctx context.Context, version int64) (*Architecture, error) {
	if di.archCache == nil {
		di.archCache = make(map[int64]*Architecture)
	}
	if arch, ok := di.archCache[version]; ok {
		return arch, nil
	}
	arch, err := OpenDomainStore(ctx, di.trans, di.name, version, di.config.ContentSize, !di.config.EnableIngestionBuffer)
	if err != nil {
		return nil, err
	}
	di.archCache[version] = arch
	return arch, nil
}

func (di *domainIndex[T]) upsertItem(ctx context.Context, arch *Architecture, item ai.Item[T], centroids map[int][]float32) error {
	id := item.ID
	vec := item.Vector

	// Optimization: Stage vectors in TempVectors for faster ingestion during initial pass (Version 0).
	if arch.TempVectors != nil {
		if _, err := arch.TempVectors.Add(ctx, id, vec); err != nil {
			return err
		}

		// For TempVectors, we use a default key.
		key := ai.ContentKey{
			ItemID: id,
		}
		// Payload is stored as JSON string in Value.
		data, err := json.Marshal(item.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal payload: %w", err)
		}
		if _, err := arch.Content.Add(ctx, key, string(data)); err != nil {
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

	shouldIncrement := true

	// 0. Cleanup Old Entry (if exists) to prevent "ghost" vectors
	if di.deduplicationEnabled {
		// Construct a search key (only ItemID matters for search)
		searchKey := ai.ContentKey{ItemID: id}
		found, err := arch.Content.Find(ctx, searchKey, false)
		if err != nil {
			return err
		}
		if found {
			// Retrieve old metadata from Key
			oldItem := arch.Content.GetCurrentKey()
			oldKey := oldItem.Key

			oldCid := oldKey.CentroidID
			oldDist := oldKey.Distance
			if oldCid == 0 {
				oldCid = 1
			}

			// Remove old vector
			oldVecKey := ai.VectorKey{CentroidID: oldCid, DistanceToCentroid: oldDist, ItemID: id}
			if foundVec, _ := arch.Vectors.Find(ctx, oldVecKey, false); foundVec {
				if _, err := arch.Vectors.RemoveCurrentItem(ctx); err != nil {
					return err
				}
				// Decrement count of old centroid
				if di.config.UsageMode == ai.DynamicWithVectorCountTracking {
					if oldCid == centroidID {
						shouldIncrement = false
					} else {
						if foundC, _ := arch.Centroids.Find(ctx, oldCid, false); foundC {
							c, _ := arch.Centroids.GetCurrentValue(ctx)
							c.VectorCount--
							arch.Centroids.UpdateCurrentValue(ctx, c)
						}
					}
				}
			}

			// We don't remove content here because we will overwrite it (Upsert)
			// But we need to be careful if Upsert doesn't replace Key.
			// Actually, we should use Upsert with the new Key.
		}
	}

	// Increment count
	if di.config.UsageMode == ai.DynamicWithVectorCountTracking && shouldIncrement {
		if foundC, _ := arch.Centroids.Find(ctx, centroidID, false); foundC {
			c, _ := arch.Centroids.GetCurrentValue(ctx)
			c.VectorCount++
			arch.Centroids.UpdateCurrentValue(ctx, c)
		}
	}

	// 2. Update Vector Index
	key := ai.VectorKey{CentroidID: centroidID, DistanceToCentroid: dist, ItemID: id}
	if _, err := arch.Vectors.Add(ctx, key, vec); err != nil {
		return err
	}

	// 3. Update Content Store
	// We use the new Key struct with metadata.
	// We assume Upsert will update the Key in the B-Tree node if it exists.
	contentKey := ai.ContentKey{
		ItemID:     id,
		CentroidID: centroidID,
		Distance:   dist,
		Version:    arch.Version, // Use current version
		// Next fields are zeroed out
	}

	data, err := json.Marshal(item.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Use Upsert to update Key and Value.
	// Upsert now updates the Key (metadata) as well if the item exists.
	if _, err := arch.Content.Upsert(ctx, contentKey, string(data)); err != nil {
		return err
	}

	return nil
}

// Upsert adds or updates a vector in the store.
func (di *domainIndex[T]) Upsert(ctx context.Context, item ai.Item[T]) error {
	if locked, err := di.isOptimizing(ctx); err != nil {
		return err
	} else if locked {
		return fmt.Errorf("vector store is currently optimizing, read-only mode active")
	}

	version, err := di.getActiveVersion(ctx, di.trans)
	if err != nil {
		return err
	}

	arch, err := di.getArchitecture(ctx, version)
	if err != nil {
		return err
	}

	var centroids map[int][]float32
	if arch.TempVectors == nil {
		centroids, err = di.getCentroids(ctx, arch)
		if err != nil {
			return err
		}
	}

	if err := di.upsertItem(ctx, arch, item, centroids); err != nil {
		return err
	}

	return nil
}

// UpsertBatch adds or updates multiple vectors in a single transaction.
func (di *domainIndex[T]) UpsertBatch(ctx context.Context, items []ai.Item[T]) error {
	if locked, err := di.isOptimizing(ctx); err != nil {
		return err
	} else if locked {
		return fmt.Errorf("vector store is currently optimizing, read-only mode active")
	}

	version, err := di.getActiveVersion(ctx, di.trans)
	if err != nil {
		return err
	}

	arch, err := di.getArchitecture(ctx, version)
	if err != nil {
		return err
	}

	var centroids map[int][]float32
	if arch.TempVectors == nil {
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

	return nil
}

// Get retrieves a vector by ID.
func (di *domainIndex[T]) Get(ctx context.Context, id string) (*ai.Item[T], error) {
	version, err := di.getActiveVersion(ctx, di.trans)
	if err != nil {
		return nil, err
	}

	arch, err := di.getArchitecture(ctx, version)
	if err != nil {
		return nil, err
	}

	found, err := arch.Content.Find(ctx, ai.ContentKey{ItemID: id}, false)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("item not found")
	}

	jsonString, err := arch.Content.GetCurrentValue(ctx)
	if err != nil {
		return nil, err
	}

	// Retrieve metadata from Key
	currentItem := arch.Content.GetCurrentKey()
	currentKey := currentItem.Key

	var payload T
	if err := json.Unmarshal([]byte(jsonString), &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal stored item: %w", err)
	}

	if currentKey.Deleted {
		return nil, fmt.Errorf("item not found")
	}

	// Fetch Vector
	cid := currentKey.CentroidID
	dist := currentKey.Distance

	// Check if we need to use the "Next" version (Lazy Migration)
	// If the stored version matches the active version, use standard fields.
	// If the stored NextVersion matches the active version, use Next fields.
	if currentKey.Version != version {
		if currentKey.NextVersion == version {
			cid = currentKey.NextCentroidID
			dist = currentKey.NextDistance
			// Note: We don't update the record here (lazy update) to avoid write-on-read penalty.
			// The cleanup can happen during the next Upsert or a background job.
		} else if version == 0 && currentKey.Version == 0 {
			// Legacy/Initial case: Version 0 matches.
		} else {
			// Version mismatch and Next doesn't match either.
			// This implies the item belongs to an old version that is no longer active,
			// OR the item was added during a migration that failed/switched.
			// However, if we are here, it means we found the item in Content.
			// If Content is shared, we should try to find the vector in the active version's Vectors store.
			// But we don't know the CentroidID for the active version if it's not recorded.
			// Fallback: If Version 0 is active, we might be in TempVectors mode.
		}
	}

	var vec []float32

	// Explicitly handle TempVectors OR Vectors based on Version (TempVectors existence)
	if arch.TempVectors != nil {
		if found, err := arch.TempVectors.Find(ctx, id, false); err != nil {
			return nil, err
		} else if found {
			vec, _ = arch.TempVectors.GetCurrentValue(ctx)
			return &ai.Item[T]{ID: id, Vector: vec, Payload: payload, CentroidID: 0}, nil
		}
		return nil, fmt.Errorf("item vector not found in TempVectors")
	}

	// Vectors Mode
	if cid == 0 {
		return nil, fmt.Errorf("item has invalid centroid ID (0) for Vectors mode")
	}

	vecKey := ai.VectorKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: id}
	foundVec, err := arch.Vectors.Find(ctx, vecKey, false)
	if err != nil {
		return nil, err
	}
	if foundVec {
		vec, _ = arch.Vectors.GetCurrentValue(ctx)
	} else {
		// Fallback: If not found, it might be that the item was deleted or the version logic is off.
		return nil, fmt.Errorf("item vector not found in Vectors (cid=%d)", cid)
	}

	return &ai.Item[T]{ID: id, Vector: vec, Payload: payload, CentroidID: cid}, nil
}

// Delete removes a vector from the store.
func (di *domainIndex[T]) Delete(ctx context.Context, id string) error {
	if locked, err := di.isOptimizing(ctx); err != nil {
		return err
	} else if locked {
		return fmt.Errorf("vector store is currently optimizing, read-only mode active")
	}

	version, err := di.getActiveVersion(ctx, di.trans)
	if err != nil {
		return err
	}

	arch, err := di.getArchitecture(ctx, version)
	if err != nil {
		return err
	}

	found, err := arch.Content.Find(ctx, ai.ContentKey{ItemID: id}, false)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	// Retrieve current key and value
	currentItem := arch.Content.GetCurrentKey()
	currentKey := currentItem.Key
	// jsonStr, _ := arch.Content.GetCurrentValue(ctx)

	// Soft Delete: Update Key
	currentKey.Deleted = true

	// Update the Key to reflect the deleted status.
	if _, err := arch.Content.UpdateCurrentKey(ctx, currentKey); err != nil {
		return err
	}

	// Remove from Vectors
	cid := currentKey.CentroidID

	// Explicitly handle TempVectors OR Vectors based on Version
	if arch.TempVectors != nil {
		if found, err := arch.TempVectors.Find(ctx, id, false); err != nil {
			return err
		} else if found {
			if _, err := arch.TempVectors.RemoveCurrentItem(ctx); err != nil {
				return err
			}
		}
		return nil
	}

	// Vectors Mode
	if cid == 0 {
		// Invalid for Vectors mode, cannot delete from Vectors if we don't know the key
		return nil
	}
	dist := currentKey.Distance

	vecKey := ai.VectorKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: id}
	if found, err := arch.Vectors.Find(ctx, vecKey, false); err != nil {
		return err
	} else if found {
		if _, err := arch.Vectors.RemoveCurrentItem(ctx); err != nil {
			return err
		}
		if di.config.UsageMode == ai.DynamicWithVectorCountTracking {
			if foundC, _ := arch.Centroids.Find(ctx, cid, false); foundC {
				c, _ := arch.Centroids.GetCurrentValue(ctx)
				c.VectorCount--
				arch.Centroids.UpdateCurrentValue(ctx, c)
			}
		}
	}

	return nil
}

// Query searches for the nearest neighbors.
func (di *domainIndex[T]) Query(ctx context.Context, vec []float32, k int, filter func(T) bool) ([]ai.Hit[T], error) {
	version, err := di.getActiveVersion(ctx, di.trans)
	if err != nil {
		return nil, err
	}

	arch, err := di.getArchitecture(ctx, version)
	if err != nil {
		return nil, err
	}

	var candidates []ai.Hit[T]

	// Explicitly handle TempVectors OR Vectors based on Version
	if arch.TempVectors != nil {
		// Search TempVectors (Brute Force)
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
	} else {
		// 1. Search Indexed Vectors (if any centroids exist)
		targetCentroids, err := di.searchClosestCentroids(ctx, arch, vec, 2)
		if err != nil {
			return nil, err
		}

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
	}

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })

	var finalHits []ai.Hit[T]
	for _, hit := range candidates {
		if len(finalHits) >= k {
			break
		}

		found, err := arch.Content.Find(ctx, ai.ContentKey{ItemID: hit.ID}, false)
		if err != nil {
			return nil, err
		}
		if found {
			currentItem := arch.Content.GetCurrentKey()
			if !currentItem.Key.Deleted {
				jsonStr, _ := arch.Content.GetCurrentValue(ctx)
				var payload T
				if err := json.Unmarshal([]byte(jsonStr), &payload); err == nil {
					if filter == nil || filter(payload) {
						hit.Payload = payload
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
	version, err := di.getActiveVersion(ctx, di.trans)
	if err != nil {
		return 0, err
	}

	arch, err := di.getArchitecture(ctx, version)
	if err != nil {
		return 0, err
	}

	return arch.Content.Count(), nil
}

// AddCentroid adds a new centroid.
func (di *domainIndex[T]) AddCentroid(ctx context.Context, vec []float32) (int, error) {
	version, err := di.getActiveVersion(ctx, di.trans)
	if err != nil {
		return 0, err
	}

	arch, err := di.getArchitecture(ctx, version)
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

	return newID, nil
}

// Centroids returns the Centroids B-Tree for advanced manipulation.
func (di *domainIndex[T]) Centroids(ctx context.Context) (btree.BtreeInterface[int, ai.Centroid], error) {
	version, err := di.getActiveVersion(ctx, di.trans)
	if err != nil {
		return nil, err
	}
	arch, err := di.getArchitecture(ctx, version)
	if err != nil {
		return nil, err
	}
	return arch.Centroids, nil
}

// Vectors returns the Vectors B-Tree for advanced manipulation.
func (di *domainIndex[T]) Vectors(ctx context.Context) (btree.BtreeInterface[ai.VectorKey, []float32], error) {
	version, err := di.getActiveVersion(ctx, di.trans)
	if err != nil {
		return nil, err
	}
	arch, err := di.getArchitecture(ctx, version)
	if err != nil {
		return nil, err
	}
	return arch.Vectors, nil
}

// Content returns the Content B-Tree for advanced manipulation.
func (di *domainIndex[T]) Content(ctx context.Context) (btree.BtreeInterface[ai.ContentKey, string], error) {
	version, err := di.getActiveVersion(ctx, di.trans)
	if err != nil {
		return nil, err
	}
	arch, err := di.getArchitecture(ctx, version)
	if err != nil {
		return nil, err
	}
	return arch.Content, nil
}

// Lookup returns the Sequence-to-ID B-Tree for advanced manipulation.
//
// Note: This B-Tree is primarily used for random sampling during the Optimize phase (K-Means clustering).
// It is a snapshot of the items at the time of the last optimization and is NOT updated in real-time
// during standard Upsert/Delete operations.
func (di *domainIndex[T]) Lookup(ctx context.Context) (btree.BtreeInterface[int, string], error) {
	version, err := di.getActiveVersion(ctx, di.trans)
	if err != nil {
		return nil, err
	}
	arch, err := di.getArchitecture(ctx, version)
	if err != nil {
		return nil, err
	}
	return arch.Lookup, nil
}

// --- Helpers ---

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

var optimizingDomains sync.Map

func (di *domainIndex[T]) isOptimizing(ctx context.Context) (bool, error) {
	_, ok := optimizingDomains.Load(di.name)
	return ok, nil
}

func (di *domainIndex[T]) getActiveVersion(ctx context.Context, trans sop.Transaction) (int64, error) {
	found, err := di.sysStore.Find(ctx, di.name, false)
	if err != nil {
		return 0, err
	}
	if found {
		return di.sysStore.GetCurrentValue(ctx)
	}
	return 0, nil
}

func (di *domainIndex[T]) beginTransaction(ctx context.Context) (sop.Transaction, error) {
	var t sop.Transaction
	var err error
	if len(di.config.StoresFolders) > 0 || len(di.config.ErasureConfig) > 0 {
		t, err = inredfs.NewTransactionWithReplication(ctx, inredfs.TransationOptionsWithReplication{
			Mode:              sop.ForWriting,
			StoresBaseFolders: di.config.StoresFolders,
			ErasureConfig:     di.config.ErasureConfig,
			Cache:             di.config.Cache,
		})
	} else {
		t, err = inredfs.NewTransaction(ctx, inredfs.TransationOptions{
			Mode:             sop.ForWriting,
			StoresBaseFolder: di.config.StoragePath,
			Cache:            di.config.Cache,
		})
	}
	if err != nil {
		return nil, err
	}
	if err := t.Begin(ctx); err != nil {
		return nil, err
	}
	return t, nil
}
