package vector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/inredfs"
)

// Optimize reorganizes the index to improve query performance by rebuilding the vector structures.
//
// This operation uses a batched approach to handle large datasets without hitting transaction limits.
// It commits the current transaction immediately to persist pending changes, then manages its own
// sequence of transactions to process the data in batches.
//
// Note: During optimization, the store enters a read-only mode. Write operations (Upsert, Delete)
// will return an error until optimization completes.
//
// Recommendation: The provided context should have a sufficient timeout or be background context,
// as optimization can take a significant amount of time depending on the dataset size.
//
// Warning: This method commits the transaction passed to Open(). The caller should not attempt
// to use the original transaction after calling Optimize().
func (di *domainIndex[T]) Optimize(ctx context.Context) error {
	log.Printf("DEBUG: Optimize started for domain %s", di.name)
	// 1. Commit the current transaction to ensure any pending writes are saved.
	if di.trans.HasBegun() {
		log.Println("DEBUG: Committing initial transaction")
		if err := di.trans.Commit(ctx); err != nil {
			return err
		}
	}

	// Helper to open the architecture in the current transaction
	openArch := func(tx sop.Transaction, ver int64) (*Architecture, error) {
		// Clear cache to force re-open with new tx
		di.archCache = nil
		di.trans = tx
		return di.getArchitecture(ctx, ver)
	}

	// Helper to open SysStore
	openSysStore := func(tx sop.Transaction) (btree.BtreeInterface[string, int64], error) {
		sysStoreName := fmt.Sprintf("%s%s", di.name, sysConfigSuffix)
		return newBtree[string, int64](ctx, sop.ConfigureStore(sysStoreName, true, 1000, sysConfigDesc, sop.SmallData, ""), tx, func(a, b string) int {
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		})
	}

	// 2. Phase 1: Build Lookup Table (Batched)

	// Set In-Memory Lock
	optimizingDomains.Store(di.name, true)
	defer optimizingDomains.Delete(di.name)

	// We need to know the current version to start.
	// We start a short TX just to get the version.
	log.Println("DEBUG: Starting check transaction")
	tx, err := di.beginTransaction(ctx)
	if err != nil {
		return err
	}
	di.trans = tx

	// Re-open SysStore
	sysStore, err := openSysStore(tx)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}
	di.sysStore = sysStore

	currentVersion, err := di.getActiveVersion(ctx, tx)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}
	log.Printf("DEBUG: Current version: %d", currentVersion)

	// Prepare New Stores (Version + 1)
	newVersion := currentVersion + 1
	suffix := fmt.Sprintf("_%d", newVersion)

	// Check if previous optimization failed (stores exist)
	// If so, we should clean them up before starting.
	// We can check if the new lookup store exists.
	lookupName := di.name + lookupSuffix + suffix
	found, err := inredfs.IsStoreExists(ctx, tx, lookupName)
	if err != nil {
		// If we can't check, assume it doesn't exist or we can't proceed safely
		// But IsStoreExists might return error if store not found in some implementations?
		// Assuming standard SOP behavior: error means something wrong with DB.
		tx.Rollback(ctx)
		return err
	}
	if found {
		log.Println("DEBUG: Found previous failed optimization, cleaning up")
		// Previous optimization failed. Cleanup.
		// We need to remove the stores.
		// Since we are in a transaction, we can use the store repository to remove them.
		// But we need to know the names.
		storesToRemove := []string{
			di.name + centroidsSuffix + suffix,
			di.name + vectorsSuffix + suffix,
			di.name + lookupSuffix + suffix,
		}

		pt := tx.GetPhasedTransaction()
		if ct, ok := pt.(*common.Transaction); ok {
			for _, name := range storesToRemove {
				// Ignore errors if store doesn't exist (partial failure)
				_ = ct.StoreRepository.Remove(ctx, name)
			}
		}
		// Commit the cleanup
		if err := tx.Commit(ctx); err != nil {
			return err
		}

		// Start a new transaction for the actual work
		tx, err = di.beginTransaction(ctx)
		if err != nil {
			return err
		}
		di.trans = tx
	} else {
		// No previous failed attempt found, rollback the check TX
		log.Println("DEBUG: No previous failed optimization, rolling back check TX")
		tx.Rollback(ctx)

		// Start a new transaction for the actual work
		// tx, err = di.beginTransaction(ctx)
		// if err != nil {
		// 	return err
		// }
		// di.trans = tx
	}

	// We need to create the stores in the first transaction to ensure they exist for subsequent batches.
	// Note: We are NOT using the helper openArch yet because we are creating NEW stores.

	// Create/Open New Stores
	// We use a loop to process batches.

	// State for iteration
	var count int
	const batchSize = 200 // As requested

	// We need to iterate the OLD stores.
	// Problem: We can't easily "pause" a B-Tree iterator across transactions.
	// We have to seek to the last key.

	// Let's close the initial check TX and start the loop fresh.
	// tx.Rollback(ctx) // Already rolled back or committed above?
	// Wait, if found was true, tx is new. If found was false, tx is rolled back.
	// The original code had:
	/*
		} else {
			tx.Rollback(ctx)
			tx, err = di.beginTransaction(ctx)
			di.trans = tx
		}
		tx.Rollback(ctx)
	*/
	// This means we ALWAYS enter the loop with NO active transaction.
	// So I should remove the `tx.Rollback(ctx)` if I want to keep the logic consistent,
	// OR just acknowledge that the loop starts its own transaction.

	// --- Phase 1: Build Lookup Table (Batched) ---
	log.Println("DEBUG: Phase 1: Build Lookup Table")
	// Source depends on version:
	// - Version 1 (Initial Optimization): Source is TempVectors (if enabled)
	// - Version > 1 (Re-Optimization): Source is Vectors
	// - Version 1 (Direct Ingestion): Source is Vectors (if TempVectors disabled)
	useTempVectors := newVersion == 1 && di.config.EnableIngestionBuffer

	var lastVectorKey *ai.VectorKey
	var lastTempKey string

	for {
		log.Println("DEBUG: Starting batch transaction")
		tx, err := di.beginTransaction(ctx)
		if err != nil {
			return err
		}

		oldArch, err := openArch(tx, currentVersion)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}

		newLookup, err := newBtree[int, string](ctx, sop.ConfigureStore(di.name+lookupSuffix+suffix, true, 1000, lookupDesc, sop.SmallData, ""), tx, func(a, b int) int { return a - b })
		if err != nil {
			tx.Rollback(ctx)
			return err
		}

		var ok bool
		var b3 btree.BtreeInterface[ai.VectorKey, []float32]
		var b3Temp btree.BtreeInterface[string, []float32]

		if useTempVectors {
			if oldArch.TempVectors == nil {
				// Should not happen if version == 0, but handle gracefully
				tx.Rollback(ctx)
				break
			}
			b3Temp = oldArch.TempVectors
			if lastTempKey == "" {
				ok, err = b3Temp.First(ctx)
			} else {
				ok, err = b3Temp.Find(ctx, lastTempKey, false)
				if err == nil && ok {
					ok, err = b3Temp.Next(ctx)
				}
			}
		} else {
			b3 = oldArch.Vectors
			if lastVectorKey == nil {
				ok, err = b3.First(ctx)
			} else {
				ok, err = b3.Find(ctx, *lastVectorKey, false)
				if err == nil && ok {
					ok, err = b3.Next(ctx)
				}
			}
		}

		if err != nil {
			tx.Rollback(ctx)
			return err
		}

		processed := 0
		if ok {
			for {
				var itemID string
				var key interface{}

				if useTempVectors {
					item, err := b3Temp.GetCurrentItem(ctx)
					if err != nil {
						tx.Rollback(ctx)
						return err
					}
					itemID = item.Key
					key = item.Key

					// Check Content for duplicates (logic from original Optimize)
					shouldProcess := true
					if di.deduplicationEnabled {
						if found, err := oldArch.Content.Find(ctx, item.Key, false); err != nil {
							tx.Rollback(ctx)
							return err
						} else if found {
							jsonStr, _ := oldArch.Content.GetCurrentValue(ctx)
							var stored StoredItem[T]
							if err := json.Unmarshal([]byte(jsonStr), &stored); err == nil {
								if stored.CentroidID != 0 {
									shouldProcess = false
								}
							}
						}
					}

					if shouldProcess {
						if _, err := newLookup.Add(ctx, count, itemID); err != nil {
							tx.Rollback(ctx)
							return err
						}
						count++
					}
				} else {
					item, err := b3.GetCurrentItem(ctx)
					if err != nil {
						tx.Rollback(ctx)
						return err
					}
					itemID = item.Key.ItemID
					key = item.Key

					if _, err := newLookup.Add(ctx, count, itemID); err != nil {
						tx.Rollback(ctx)
						return err
					}
					count++
				}

				processed++
				if useTempVectors {
					lastTempKey = key.(string)
				} else {
					k := key.(ai.VectorKey)
					lastVectorKey = &k
				}

				if processed >= batchSize {
					break
				}

				var nextOk bool
				if useTempVectors {
					nextOk, err = b3Temp.Next(ctx)
				} else {
					nextOk, err = b3.Next(ctx)
				}

				if err != nil {
					tx.Rollback(ctx)
					return err
				} else if !nextOk {
					break
				}
			}
		}

		log.Printf("DEBUG: Committing batch, processed %d items", processed)
		if err := tx.Commit(ctx); err != nil {
			return err
		}

		if processed < batchSize {
			break
		}
	}

	if count == 0 {
		log.Println("DEBUG: No items to optimize")
		return nil
	}

	// --- Phase 2: Sampling & Centroids (Single Batch) ---
	log.Println("DEBUG: Phase 2: Sampling & Centroids")
	// We assume sampling fits in one TX (read-only mostly) + writing centroids (small).
	{
		tx, err := di.beginTransaction(ctx)
		if err != nil {
			return err
		}

		oldArch, err := openArch(tx, currentVersion)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}

		newLookup, err := newBtree[int, string](ctx, sop.ConfigureStore(di.name+lookupSuffix+suffix, true, 1000, lookupDesc, sop.SmallData, ""), tx, func(a, b int) int { return a - b })
		if err != nil {
			tx.Rollback(ctx)
			return err
		}

		// Sampling Logic (Copied from store.go)
		const maxSamples = 50000
		var samples []ai.Item[T]
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		sampleCount := maxSamples
		if count < maxSamples {
			sampleCount = count
		}
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

		for idx := range indices {
			if found, err := newLookup.Find(ctx, idx, false); err != nil {
				tx.Rollback(ctx)
				return err
			} else if !found {
				continue
			}
			itemID, _ := newLookup.GetCurrentValue(ctx)

			// Fetch Vector (Strict: TempVectors for V0, Vectors for V>0)
			var vec []float32
			if currentVersion == 0 && oldArch.TempVectors != nil {
				if found, err := oldArch.TempVectors.Find(ctx, itemID, false); err != nil {
					tx.Rollback(ctx)
					return err
				} else if found {
					vec, _ = oldArch.TempVectors.GetCurrentValue(ctx)
				}
			} else {
				if found, err := oldArch.Content.Find(ctx, itemID, false); err != nil {
					tx.Rollback(ctx)
					return err
				} else if !found {
					continue
				}
				contentJson, _ := oldArch.Content.GetCurrentValue(ctx)
				var stored StoredItem[T]
				json.Unmarshal([]byte(contentJson), &stored)
				vecKey := ai.VectorKey{CentroidID: stored.CentroidID, DistanceToCentroid: stored.Distance, ItemID: itemID}
				if found, _ := oldArch.Vectors.Find(ctx, vecKey, false); found {
					vec, _ = oldArch.Vectors.GetCurrentValue(ctx)
				}
			}
			samples = append(samples, ai.Item[T]{ID: itemID, Vector: vec})
		}

		// Compute Centroids
		k := int(math.Sqrt(float64(count)))
		if k < 1 {
			k = 1
		}
		if k > 256 {
			k = 256
		}
		newCentroidsMap, err := ComputeCentroids(samples, k)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}

		// Write Centroids
		newCentroids, err := newBtree[int, ai.Centroid](ctx, sop.ConfigureStore(di.name+centroidsSuffix+suffix, true, 100, centroidsDesc, sop.SmallData, ""), tx, func(a, b int) int { return a - b })
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
		for id, vec := range newCentroidsMap {
			if _, err := newCentroids.Add(ctx, id, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
				tx.Rollback(ctx)
				return err
			}
		}

		// Update Cache
		di.updateCentroidsCache(newCentroidsMap)

		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}

	// --- Phase 3: Migration (Batched) ---
	log.Println("DEBUG: Phase 3: Migration")
	// We need to reload centroids map for processing
	// We can use the cache since we just updated it.
	newCentroidsMap := di.centroidsCache

	// 3a. Migrate Vectors
	if !useTempVectors {
		lastVectorKey = nil // Reset
		for {
			tx, err := di.beginTransaction(ctx)
			if err != nil {
				return err
			}

			oldArch, err := openArch(tx, currentVersion)
			if err != nil {
				tx.Rollback(ctx)
				return err
			}

			newVectors, err := newBtree[ai.VectorKey, []float32](ctx, sop.ConfigureStore(di.name+vectorsSuffix+suffix, true, 1000, vectorsDesc, sop.SmallData, ""), tx, compositeKeyComparer)
			if err != nil {
				tx.Rollback(ctx)
				return err
			}

			var newCentroids btree.BtreeInterface[int, ai.Centroid]
			if di.config.UsageMode == ai.DynamicWithVectorCountTracking {
				newCentroids, err = newBtree[int, ai.Centroid](ctx, sop.ConfigureStore(di.name+centroidsSuffix+suffix, true, 100, centroidsDesc, sop.SmallData, ""), tx, func(a, b int) int { return a - b })
				if err != nil {
					tx.Rollback(ctx)
					return err
				}
			}

			b3 := oldArch.Vectors
			var ok bool
			if lastVectorKey == nil {
				ok, err = b3.First(ctx)
			} else {
				ok, err = b3.Find(ctx, *lastVectorKey, false)
				if err == nil && ok {
					ok, err = b3.Next(ctx)
				}
			}

			if err != nil {
				tx.Rollback(ctx)
				return err
			}

			processed := 0
			if ok {
				for {
					item, err := b3.GetCurrentItem(ctx)
					if err != nil {
						tx.Rollback(ctx)
						return err
					}

					// Process Item
					vec := *item.Value
					cid, dist := findClosestCentroid(vec, newCentroidsMap)

					// Add to New Vectors
					key := ai.VectorKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: item.Key.ItemID}
					if _, err := newVectors.Add(ctx, key, vec); err != nil {
						tx.Rollback(ctx)
						return err
					}

					if newCentroids != nil {
						if found, _ := newCentroids.Find(ctx, cid, false); found {
							c, _ := newCentroids.GetCurrentValue(ctx)
							c.VectorCount++
							newCentroids.UpdateCurrentItem(ctx, c)
						}
					}

					// Update Content
					if found, _ := oldArch.Content.Find(ctx, item.Key.ItemID, false); found {
						jsonStr, _ := oldArch.Content.GetCurrentValue(ctx)
						var stored StoredItem[T]
						if err := json.Unmarshal([]byte(jsonStr), &stored); err == nil {
							stored.CentroidID = cid
							stored.Distance = dist
							newData, _ := json.Marshal(stored)
							oldArch.Content.UpdateCurrentItem(ctx, string(newData))
						}
					}

					processed++
					k := item.Key
					lastVectorKey = &k

					if processed >= batchSize {
						break
					}

					if ok, err := b3.Next(ctx); err != nil {
						tx.Rollback(ctx)
						return err
					} else if !ok {
						break
					}
				}
			}

			if err := tx.Commit(ctx); err != nil {
				return err
			}

			if processed < batchSize {
				break
			}
		}
	}

	// 3b. Migrate TempVectors
	if useTempVectors {
		lastTempKey = "" // Reset
		for {
			tx, err := di.beginTransaction(ctx)
			if err != nil {
				return err
			}

			oldArch, err := openArch(tx, currentVersion)
			if err != nil {
				tx.Rollback(ctx)
				return err
			}

			if oldArch.TempVectors == nil {
				tx.Rollback(ctx)
				break
			}

			newVectors, err := newBtree[ai.VectorKey, []float32](ctx, sop.ConfigureStore(di.name+vectorsSuffix+suffix, true, 1000, vectorsDesc, sop.SmallData, ""), tx, compositeKeyComparer)
			if err != nil {
				tx.Rollback(ctx)
				return err
			}

			var newCentroids btree.BtreeInterface[int, ai.Centroid]
			if di.config.UsageMode == ai.DynamicWithVectorCountTracking {
				newCentroids, err = newBtree[int, ai.Centroid](ctx, sop.ConfigureStore(di.name+centroidsSuffix+suffix, true, 100, centroidsDesc, sop.SmallData, ""), tx, func(a, b int) int { return a - b })
				if err != nil {
					tx.Rollback(ctx)
					return err
				}
			}

			b3 := oldArch.TempVectors
			var ok bool
			if lastTempKey == "" {
				ok, err = b3.First(ctx)
			} else {
				ok, err = b3.Find(ctx, lastTempKey, false)
				if err == nil && ok {
					ok, err = b3.Next(ctx)
				}
			}

			if err != nil {
				tx.Rollback(ctx)
				return err
			}

			processed := 0
			if ok {
				for {
					item, err := b3.GetCurrentItem(ctx)
					if err != nil {
						tx.Rollback(ctx)
						return err
					}

					// Check Duplicates
					shouldProcess := true
					if di.deduplicationEnabled {
						if found, _ := oldArch.Content.Find(ctx, item.Key, false); found {
							jsonStr, _ := oldArch.Content.GetCurrentValue(ctx)
							var stored StoredItem[T]
							if err := json.Unmarshal([]byte(jsonStr), &stored); err == nil {
								if stored.CentroidID != 0 {
									shouldProcess = false
								}
							}
						}
					}

					if shouldProcess {
						vec := *item.Value
						cid, dist := findClosestCentroid(vec, newCentroidsMap)

						key := ai.VectorKey{CentroidID: cid, DistanceToCentroid: dist, ItemID: item.Key}
						if _, err := newVectors.Add(ctx, key, vec); err != nil {
							tx.Rollback(ctx)
							return err
						}

						if newCentroids != nil {
							if found, _ := newCentroids.Find(ctx, cid, false); found {
								c, _ := newCentroids.GetCurrentValue(ctx)
								c.VectorCount++
								newCentroids.UpdateCurrentItem(ctx, c)
							}
						}

						if found, _ := oldArch.Content.Find(ctx, item.Key, false); found {
							jsonStr, _ := oldArch.Content.GetCurrentValue(ctx)
							var stored StoredItem[T]
							if err := json.Unmarshal([]byte(jsonStr), &stored); err == nil {
								stored.CentroidID = cid
								stored.Distance = dist
								newData, _ := json.Marshal(stored)
								oldArch.Content.UpdateCurrentItem(ctx, string(newData))
							}
						}
					}

					processed++
					lastTempKey = item.Key

					if processed >= batchSize {
						break
					}

					if ok, err := b3.Next(ctx); err != nil {
						tx.Rollback(ctx)
						return err
					} else if !ok {
						break
					}
				}
			}

			if err := tx.Commit(ctx); err != nil {
				return err
			}

			if processed < batchSize {
				break
			}
		}
	}

	// --- Phase 4: Finalize ---
	log.Println("DEBUG: Phase 4: Finalize")
	{
		tx, err := di.beginTransaction(ctx)
		if err != nil {
			return err
		}
		di.trans = tx // Update di.trans to the final one

		// Open Sys Store
		sysStore, err := openSysStore(tx)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
		di.sysStore = sysStore

		// Update Version
		if found, _ := sysStore.Find(ctx, di.name, false); found {
			sysStore.UpdateCurrentItem(ctx, newVersion)
		} else {
			sysStore.Add(ctx, di.name, newVersion)
		}

		// Register OnCommit hook
		tx.OnCommit(func(ctx context.Context) error {
			pt := tx.GetPhasedTransaction()
			ct, ok := pt.(*common.Transaction)
			if !ok {
				return nil
			}

			if useTempVectors {
				storeName := fmt.Sprintf("%s%s", di.name, tempVectorsSuffix)
				// Ignore error if store doesn't exist (already removed or never created)
				_ = ct.StoreRepository.Remove(ctx, storeName)
			}

			// Cleanup old version stores
			suffix := ""
			if currentVersion > 0 {
				suffix = fmt.Sprintf("_%d", currentVersion)
			}
			storesToRemove := []string{
				fmt.Sprintf("%s%s%s", di.name, centroidsSuffix, suffix),
				fmt.Sprintf("%s%s%s", di.name, vectorsSuffix, suffix),
				fmt.Sprintf("%s%s%s", di.name, lookupSuffix, suffix),
			}
			for _, name := range storesToRemove {
				// Ignore error if store doesn't exist
				_ = ct.StoreRepository.Remove(ctx, name)
			}
			return nil
		})

		// Commit Final
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}

	log.Println("DEBUG: Optimize completed successfully")
	return nil
}
