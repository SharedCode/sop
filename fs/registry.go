package fs

import (
	"context"
	"fmt"
	"io"
	log "log/slog"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/cache"
)

type registryOnDisk struct {
	hashmap            *registryMap
	replicationTracker *replicationTracker
	l2Cache            sop.Cache
	l1Cache            *cache.L1Cache
}

// Registry interface needs to have close method so registry can get closed when not needed anymore, e.g. transaction is completed.
type Registry interface {
	sop.Registry
	io.Closer
}

const (
	// Lock time out for the cache based conflict check routine in update (handles) function.
	updateAllOrNothingOfHandleSetLockTimeout = time.Duration(10 * time.Minute)
)

// NewRegistry instantiates a new Registry that manages handle records in a file using hashmap on disk.
func NewRegistry(readWrite bool, hashModValue int, rt *replicationTracker, l2Cache sop.Cache) Registry {
	return &registryOnDisk{
		hashmap:            newRegistryMap(readWrite, hashModValue, rt, l2Cache),
		replicationTracker: rt,
		l2Cache:            l2Cache,
		l1Cache:            cache.GetGlobalCache(),
	}
}

// Close all opened file handles.
func (r registryOnDisk) Close() error {
	return r.hashmap.close()
}

func (r *registryOnDisk) Add(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	for _, sh := range storesHandles {
		if err := r.hashmap.add(ctx, sop.Tuple[string, []sop.Handle]{First: sh.RegistryTable, Second: sh.IDs}); err != nil {
			return err
		}
		r.l1Cache.Handles.Set(convertToKvp(sh.IDs))
		for _, h := range sh.IDs {
			if err := r.l2Cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry UpdateNoLocks (redis setstruct) failed, details: %v", err))
			}
		}
	}
	return nil
}

func (r *registryOnDisk) Update(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	for _, sh := range storesHandles {
		// Fail on 1st encountered error. It is non-critical operation, SOP can "heal" those got left.
		for _, h := range sh.IDs {
			// Update registry record.
			lk := r.l2Cache.CreateLockKeys([]string{h.LogicalID.String()})
			if ok, err := r.l2Cache.Lock(ctx, updateAllOrNothingOfHandleSetLockTimeout, lk); !ok || err != nil {
				if err == nil {
					err = fmt.Errorf("lock failed, key %v is already locked by another", lk[0].Key)
				}
				return err
			}
			if err := r.hashmap.set(ctx, sop.Tuple[string, []sop.Handle]{First: sh.RegistryTable, Second: []sop.Handle{h}}); err != nil {
				r.l1Cache.Handles.Delete([]sop.UUID{h.LogicalID})
				r.l2Cache.Delete(ctx, []string{h.LogicalID.String()})
				// Unlock the object Keys before return.
				r.l2Cache.Unlock(ctx, lk)
				return err
			}
			// Tolerate Redis cache failure.
			if err := r.l2Cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry Update (redis setstruct) failed, details: %v", err))
			}
			// Unlock the object Keys.
			if err := r.l2Cache.Unlock(ctx, lk); err != nil {
				return err
			}
		}
		r.l1Cache.Handles.Set(convertToKvp(sh.IDs))
	}
	return nil
}

func (r *registryOnDisk) UpdateNoLocks(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	for _, sh := range storesHandles {
		if err := r.hashmap.set(ctx, sop.Tuple[string, []sop.Handle]{First: sh.RegistryTable, Second: sh.IDs}); err != nil {
			for _, h := range sh.IDs {
				r.l1Cache.Handles.Delete([]sop.UUID{h.LogicalID})
				if _, err := r.l2Cache.Delete(ctx, []string{h.LogicalID.String()}); err != nil {
					log.Warn(fmt.Sprintf("Registry UpdateNoLocks (redis delete) failed, details: %v", err))
				}
			}
			return err
		}
		r.l1Cache.Handles.Set(convertToKvp(sh.IDs))
		for _, h := range sh.IDs {
			// Tolerate Redis cache failure.
			if err := r.l2Cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry UpdateNoLocks (redis setstruct) failed, details: %v", err))
			}
		}
	}
	return nil
}

func (r *registryOnDisk) Get(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	storesHandles := make([]sop.RegistryPayload[sop.Handle], 0, len(storesLids))
	for _, storeLids := range storesLids {
		handles := make([]sop.Handle, 0, len(storeLids.IDs))
		lids := make([]sop.UUID, 0, len(storeLids.IDs))
		for i := range storeLids.IDs {
			h := sop.Handle{}
			var err error
			var found bool
			if storeLids.IsCacheTTL {
				found, err = r.l2Cache.GetStructEx(ctx, storeLids.IDs[i].String(), &h, storeLids.CacheDuration)
			} else {
				found, err = r.l2Cache.GetStruct(ctx, storeLids.IDs[i].String(), &h)
			}
			if !found || err != nil {
				if err != nil {
					log.Warn(fmt.Sprintf("Registry Get (redis getstruct) failed, details: %v", err))
				}
				lids = append(lids, storeLids.IDs[i])
				continue
			}
			handles = append(handles, h)
		}

		if len(lids) == 0 {
			storesHandles = append(storesHandles, sop.RegistryPayload[sop.Handle]{
				RegistryTable: storeLids.RegistryTable,
				BlobTable:     storeLids.BlobTable,
				CacheDuration: storeLids.CacheDuration,
				IsCacheTTL:    storeLids.IsCacheTTL,
				IDs:           handles,
			})
			continue
		}

		mh, err := r.hashmap.fetch(ctx, sop.Tuple[string, []sop.UUID]{First: storeLids.RegistryTable, Second: lids})
		if err != nil {
			return nil, err
		}

		// Add to the handles list the "missing from cache" handles read from registry file.
		for _, handle := range mh[0].Second {
			handles = append(handles, handle)
			if err := r.l2Cache.SetStruct(ctx, handle.LogicalID.String(), &handle, storeLids.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry Set (redis setstruct) failed, details: %v", err))
			}
		}
		storesHandles = append(storesHandles, sop.RegistryPayload[sop.Handle]{
			RegistryTable: storeLids.RegistryTable,
			BlobTable:     storeLids.BlobTable,
			CacheDuration: storeLids.CacheDuration,
			IsCacheTTL:    storeLids.IsCacheTTL,
			IDs:           handles,
		})
	}
	return storesHandles, nil
}
func (r *registryOnDisk) Remove(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) error {
	// Flush out the failing records from cache.
	deleteFromCache := func(storeLids sop.RegistryPayload[sop.UUID]) {
		for _, id := range storeLids.IDs {
			if _, err := r.l2Cache.Delete(ctx, []string{id.String()}); err != nil {
				log.Warn(fmt.Sprintf("Registry Delete (redis delete) failed, details: %v", err))
			}
		}
	}
	for _, storeLids := range storesLids {
		r.l1Cache.Handles.Delete(storeLids.IDs)
		if err := r.hashmap.remove(ctx, sop.Tuple[string, []sop.UUID]{First: storeLids.RegistryTable, Second: storeLids.IDs}); err != nil {
			deleteFromCache(storeLids)
			return err
		}
		deleteFromCache(storeLids)
	}
	return nil
}

/*
	Replication events:
	- IO (reading or writing) to active drive generated an IO error. SOP should be able to detect that special error and decide to failover if warranted.
		- perhaps the deciding factor is, if rollback to undo file changes fail as well then we can decide that the active drives are unworkable.
		Then failover to passive, make that active, log the event as error/fatal & needing manual intervention on the previous active drive
		that is now unusable. If rollback works then active drive is still intact.
	- Writing to passive drive errored, log an error/fatal then stop writing to the passive targets. Until a manual reset of the flag is done.

	Handling stories:
	- on rollback error, do a failover to the passive drive.
	If failed, then log FATAL and stop on succeeding runs.

	Model this on a smaller setup. Perhaps create a simulator so we can synthesize failures, failover and cut out of failing passive IO.
	We need to also detect manual intervention to cause "recover" (the opposite of failover).
*/

// Write the nodes handles to the target passive destinations.
func (r *registryOnDisk) Replicate(ctx context.Context, newRootNodesHandles, addedNodesHandles,
	updatedNodesHandles, removedNodesHandles []sop.RegistryPayload[sop.Handle]) {

	if !r.replicationTracker.replicate || r.replicationTracker.FailedToReplicate {
		log.Debug(fmt.Sprintf("replicate %v, FailedToReplicate %v, current target %s",
			r.replicationTracker.replicate, r.replicationTracker.FailedToReplicate,
			r.replicationTracker.getActiveBaseFolder()))
		return
	}

	// Open the hashmaps on the passive destination(s). Write the nodes' handle(s) on each.
	// Close the hashmaps files.
	af := r.replicationTracker.ActiveFolderToggler

	// Force tracker to treat passive as active folder so replication can write to the passive destinations.
	r.replicationTracker.ActiveFolderToggler = !af
	rm := newRegistryMap(true, r.hashmap.hashmap.hashModValue, r.replicationTracker, r.l2Cache)

	for i := range newRootNodesHandles {
		if err := r.hashmap.add(ctx, sop.Tuple[string, []sop.Handle]{First: newRootNodesHandles[i].RegistryTable,
			Second: newRootNodesHandles[i].IDs}); err != nil {
			log.Error(fmt.Sprintf("error replicating new root nodes, details: %v", err))
			r.replicationTracker.handleFailedToReplicate(ctx)
		}
	}
	for i := range addedNodesHandles {
		if err := r.hashmap.add(ctx, sop.Tuple[string, []sop.Handle]{First: addedNodesHandles[i].RegistryTable,
			Second: addedNodesHandles[i].IDs}); err != nil {
			log.Error(fmt.Sprintf("error replicating new nodes, details: %v", err))
			r.replicationTracker.handleFailedToReplicate(ctx)
		}
	}
	for i := range updatedNodesHandles {
		if err := r.hashmap.set(ctx, sop.Tuple[string, []sop.Handle]{First: updatedNodesHandles[i].RegistryTable,
			Second: updatedNodesHandles[i].IDs}); err != nil {
			log.Error(fmt.Sprintf("error replicating updated nodes, details: %v", err))
			r.replicationTracker.handleFailedToReplicate(ctx)
		}
	}

	for i := range removedNodesHandles {

		if err := r.hashmap.remove(ctx, sop.Tuple[string, []sop.UUID]{First: removedNodesHandles[i].RegistryTable,
			Second: getIDs(removedNodesHandles[i].IDs)}); err != nil {
			log.Error(fmt.Sprintf("error replicating removed nodes, details: %v", err))
			r.replicationTracker.handleFailedToReplicate(ctx)
		}
	}

	rm.close()

	// Restore to the proper active destination(s).
	r.replicationTracker.ActiveFolderToggler = af
}

func convertToKvp(handles []sop.Handle) []sop.KeyValuePair[sop.UUID, sop.Handle] {
	items := make([]sop.KeyValuePair[sop.UUID, sop.Handle], len(handles))
	for i := range handles {
		items[i] = sop.KeyValuePair[sop.UUID, sop.Handle]{Key: handles[i].LogicalID, Value: handles[i]}
	}
	return items
}
