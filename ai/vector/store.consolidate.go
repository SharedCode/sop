package vector

import (
	"context"
	"encoding/json"
	"fmt"

	log "log/slog"

	"github.com/sharedcode/sop/ai"
)

// Consolidate acts as the Sleep Cycle. It reads all items from TempVectors (Short-Term Memory),
// feeds them into AssignAndIndex (Vectors/Centroids) for Long-Term Memory,
// and deletes them from TempVectors.
func (di *domainIndex[T]) Consolidate(ctx context.Context) error {
	log.Debug("Consolidate started", "domain", di.name)

	if locked, err := di.isOptimizing(ctx); err != nil {
		return err
	} else if locked {
		return fmt.Errorf("vector store is currently optimizing, read-only mode active")
	}

	tx, err := di.beginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for consolidation: %w", err)
	}

	// Make sure we commit or rollback
	var commitErr error
	defer func() {
		if commitErr != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
	}()

	sysStore, err := di.openSysStore(ctx, tx)
	if err != nil {
		commitErr = err
		return err
	}

	found, err := sysStore.Find(ctx, di.name, false)
	if err != nil {
		commitErr = err
		return err
	}

	var version int64
	if found {
		version, err = sysStore.GetCurrentValue(ctx)
		if err != nil {
			commitErr = err
			return err
		}
	} else {
		version = 0
	}

	// We MUST open the architecture with TempVectors implicitly available
	arch, err := di.openArch(ctx, tx, version)
	if err != nil {
		commitErr = err
		return err
	}

	if arch.TempVectors == nil {
		// Nothing to consolidate
		return nil
	}

	// 1. Load Centroids cache
	centroids, err := di.getCentroids(ctx, arch)
	if err != nil {
		commitErr = err
		return err
	}

	// If no centroids exist, we need to defer to Optimize() to build the initial K-Means graph,
	// or we can auto-init a single centroid from the first vector.
	// For Active Memory, let's auto-init if empty.
	if len(centroids) == 0 {
		ok, _ := arch.TempVectors.First(ctx)
		if !ok {
			return nil
		}
		vec, _ := arch.TempVectors.GetCurrentValue(ctx)
		centroids[1] = vec
		if _, err := arch.Centroids.Add(ctx, 1, ai.Centroid{Vector: vec, VectorCount: 0}); err != nil {
			commitErr = err
			return err
		}
		di.updateCentroidsCache(centroids)
	}

	batchSize := 100 // Manageable transaction batch
	var itemsToMigrate []ai.Item[T]
	var keysToDelete []string

	// Scan TempVectors
	ok, err := arch.TempVectors.First(ctx)
	if err != nil {
		commitErr = err
		return err
	}

	for ok && len(itemsToMigrate) < batchSize {
		k := arch.TempVectors.GetCurrentKey().Key
		v, err := arch.TempVectors.GetCurrentValue(ctx)
		if err != nil {
			commitErr = err
			return err
		}

		// Fetch payload from Content tree
		contentKey := ai.ContentKey{ItemID: k}
		payloadFound, err := arch.Content.Find(ctx, contentKey, false)
		if err != nil {
			commitErr = err
			return err
		}

		if payloadFound {
			payloadJSON, err := arch.Content.GetCurrentValue(ctx)
			if err != nil {
				commitErr = err
				return err
			}
			var payload T
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err == nil {
				itemsToMigrate = append(itemsToMigrate, ai.Item[T]{
					ID:      k,
					Vector:  v,
					Payload: payload,
				})
				keysToDelete = append(keysToDelete, k)
			} else {
				log.Warn("Consolidate: failed to unmarshal payload", "id", k, "error", err)
				// Even if unmarshal fails, we probably should delete to avoid infinite loop
				keysToDelete = append(keysToDelete, k)
			}
		} else {
			// Orphaned TempVector, delete it
			keysToDelete = append(keysToDelete, k)
		}

		ok, err = arch.TempVectors.Next(ctx)
		if err != nil {
			commitErr = err
			return err
		}
	}

	// We MUST temporarily hide TempVectors from arch to force upsertItem to use the Long-Term path
	// (AssignAndIndex: Centroids/Vectors B-Trees)
	tempBuffer := arch.TempVectors
	arch.TempVectors = nil

	for _, item := range itemsToMigrate {
		// Log
		log.Debug("Consolidating memory to long-term", "id", item.ID)

		// Migrate to long term via AssignAndIndex semantic path
		if err := di.upsertItem(ctx, arch, item, centroids); err != nil {
			commitErr = fmt.Errorf("failed to migrate item %s: %w", item.ID, err)
			return commitErr
		}
	}

	// Restore TempVectors to arch and delete the items we migrated
	arch.TempVectors = tempBuffer
	for _, key := range keysToDelete {
		if ok, err := arch.TempVectors.Remove(ctx, key); err != nil || !ok {
			log.Warn("Consolidate: failed to remove migrated key from TempVectors", "key", key, "error", err)
		}
	}

	log.Debug("Consolidate finished batch", "domain", di.name, "migrated", len(itemsToMigrate))
	fmt.Printf("Consolidate migrated %d items\n", len(itemsToMigrate))
	fmt.Printf("Consolidate migrated %d items\n", len(itemsToMigrate))
	fmt.Printf("Consolidate migrated %d items\n", len(itemsToMigrate))
	return nil
}
