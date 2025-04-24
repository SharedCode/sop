package fs

import (
	"context"
	"fmt"
	log "log/slog"

	"github.com/SharedCode/sop"
)

type registryOnDisk struct {
	hashmap *hashmap[sop.UUID, sop.Handle]
	replicatorTracker                *replicationTracker
	cache sop.Cache
}

const(
	registryFilename = "registry"
	// Study whether we need this configurable.
	hashModValue = 250000
)

// TODO: Implement this to do "hash map on disk" (file based) registry entries storage & management.

// NewRegistry manages the Handle in memory for mocking.
func NewRegistry(rt *replicationTracker, cache sop.Cache) sop.Registry {
	return &registryOnDisk{
		hashmap: newHashmap(registryFilename, hashModValue, rt),
		replicatorTracker: rt,
		cache: cache,
	}
}

func (r *registryOnDisk) Add(ctx context.Context, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	for _, sh := range storesHandles {
		for _, h := range sh.IDs {
			if err := r.hashmap.set(h.LogicalID, h); err != nil {
				return err
			}
			// Tolerate Redis cache failure.
			if err := r.cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry Add (redis setstruct) failed, details: %v", err))
			}
		}
	}
	return nil
}

func (r *registryOnDisk) Update(ctx context.Context, allOrNothing bool, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	if len(storesHandles) == 0 {
		return nil
	}

	//TODO

	// Logged batch will do all or nothing. This is the only one "all or nothing" operation in the Commit process.
	if allOrNothing {
		// For now, keep it simple and rely on transaction commit's optimistic locking & multi-phase checks,
		// together with the logged batch update as shown below.

		// Enforce a Redis based version check as Cassandra logged transaction does not allow "conditional" check across partitions.
		handleKeys := make([]*sop.LockKey, 0)

		for _, sh := range storesHandles {
			for _, h := range sh.IDs {
				var h2 sop.Handle
				if err := v.cache.GetStruct(ctx, h.LogicalID.String(), &h2); err != nil {
					// Unlock the object Keys before return.
					v.cache.Unlock(ctx, handleKeys...)
					return err
				}
				newVersion := h.Version
				// Version ID is incremental, 'thus we can compare with -1 the previous.
				newVersion--
				if newVersion != h2.Version || !h.IsEqual(&h2) {
					// Unlock the object Keys before return.
					v.cache.Unlock(ctx, handleKeys...)
					return &sop.UpdateAllOrNothingError{
						Err: fmt.Errorf("Registry Update failed, handle logical ID(%v) version conflict detected", h.LogicalID.String()),
					}
				}
				// Attempt to lock in Redis the registry object, if we can't attain a lock, it means there is another transaction elsewhere
				// that already attained a lock, thus, we can cause rollback of this transaction due to conflict).
				lk := v.cache.CreateLockKeys(h.LogicalID.String())
				handleKeys = append(handleKeys, lk[0])

				if err := v.cache.Lock(ctx, updateAllOrNothingOfHandleSetLockTimeout, lk[0]); err != nil {
					// Unlock the object Keys before return.
					v.cache.Unlock(ctx, handleKeys...)
					return err
				}
			}
		}

		// Do the actual batch logged transaction update in Cassandra.
		batch := connection.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
		if connection.Config.ConsistencyBook.RegistryUpdate > gocql.Any {
			batch.SetConsistency(connection.Config.ConsistencyBook.RegistryUpdate)
		}

		for _, sh := range storesHandles {
			updateStatement := fmt.Sprintf("UPDATE %s.%s SET is_idb = ?, p_ida = ?, p_idb = ?, ver = ?, wip_ts = ?, is_del = ? WHERE lid = ?;",
				connection.Config.Keyspace, sh.RegistryTable)
			for _, h := range sh.IDs {
				// Enqueue update registry record cmd.
				batch.Query(updateStatement, h.IsActiveIDB, gocql.UUID(h.PhysicalIDA), gocql.UUID(h.PhysicalIDB),
					h.Version, h.WorkInProgressTimestamp, h.IsDeleted, gocql.UUID(h.LogicalID))
			}
		}
		// Execute the batch query, all or nothing.
		if err := connection.Session.ExecuteBatch(batch); err != nil {
			// Unlock the object Keys before return.
			v.cache.Unlock(ctx, handleKeys...)
			// Failed update all, thus, return err to cause rollback.
			return err
		}
		// Unlock the object Keys before return.
		v.cache.Unlock(ctx, handleKeys...)
	} else {
		for _, sh := range storesHandles {
			updateStatement := fmt.Sprintf("UPDATE %s.%s SET is_idb = ?, p_ida = ?, p_idb = ?, ver = ?, wip_ts = ?, is_del = ? WHERE lid = ?;",
				connection.Config.Keyspace, sh.RegistryTable)
			// Fail on 1st encountered error. It is non-critical operation, SOP can "heal" those got left.
			for _, h := range sh.IDs {

				qry := connection.Session.Query(updateStatement, h.IsActiveIDB, gocql.UUID(h.PhysicalIDA), gocql.UUID(h.PhysicalIDB),
					h.Version, h.WorkInProgressTimestamp, h.IsDeleted, gocql.UUID(h.LogicalID)).WithContext(ctx)
				if connection.Config.ConsistencyBook.RegistryUpdate > gocql.Any {
					qry.Consistency(connection.Config.ConsistencyBook.RegistryUpdate)
				}

				// Update registry record.
				if err := qry.Exec(); err != nil {
					return err
				}
			}
		}
	}

	// Update redis cache.
	for _, sh := range storesHandles {
		for _, h := range sh.IDs {
			// Tolerate Redis cache failure.
			if err := v.cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry Update (redis setstruct) failed, details: %v", err))
			}
		}
	}
	return nil
}

func (r *registryOnDisk) Get(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	return nil, nil
}
func (r *registryOnDisk) Remove(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) error {
	return nil
}
