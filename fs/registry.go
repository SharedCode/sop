package fs

import (
	"context"
	"fmt"
	"io"
	log "log/slog"
	"time"

	"github.com/SharedCode/sop"
)

type registryOnDisk struct {
	hashmap            *registryMap
	replicationTracker *replicationTracker
	cache              sop.Cache
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

// NewRegistry manages the Handle in memory for mocking.
func NewRegistry(readWrite bool, hashModValue int, rt *replicationTracker, cache sop.Cache) Registry {
	return &registryOnDisk{
		hashmap:            newRegistryMap(readWrite, hashModValue, rt, cache),
		replicationTracker: rt,
		cache:              cache,
	}
}

// Close all opened file handles.
func (r registryOnDisk) Close() error {
	return r.hashmap.close()
}

func (r registryOnDisk) Add(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	for _, sh := range storesHandles {
		if err := r.hashmap.add(ctx, sop.Tuple[string, []sop.Handle]{First: sh.RegistryTable, Second: sh.IDs}); err != nil {
			return err
		}
		for _, h := range sh.IDs {
			// Tolerate Redis cache failure.
			if err := r.cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry UpdateNoLocks (redis setstruct) failed, details: %v", err))
			}
		}
	}
	return nil
}

func (r registryOnDisk) Update(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	if len(storesHandles) == 0 {
		return nil
	}

	for _, sh := range storesHandles {
		// Fail on 1st encountered error. It is non-critical operation, SOP can "heal" those got left.
		for _, h := range sh.IDs {
			// Update registry record.
			lk := r.cache.CreateLockKeys([]string{h.LogicalID.String()})
			if ok, err := r.cache.Lock(ctx, updateAllOrNothingOfHandleSetLockTimeout, lk); !ok || err != nil {
				if err == nil {
					err = fmt.Errorf("lock failed, key %v is already locked by another", lk[0].Key)
				}
				return err
			}
			if err := r.hashmap.set(ctx, sop.Tuple[string, []sop.Handle]{First: sh.RegistryTable, Second: []sop.Handle{h}}); err != nil {
				// Unlock the object Keys before return.
				r.cache.Unlock(ctx, lk)
				return err
			}
			// Tolerate Redis cache failure.
			if err := r.cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry Update (redis setstruct) failed, details: %v", err))
			}
			// Unlock the object Keys.
			if err := r.cache.Unlock(ctx, lk); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r registryOnDisk) UpdateNoLocks(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	if len(storesHandles) == 0 {
		return nil
	}

	for _, sh := range storesHandles {
		if err := r.hashmap.set(ctx, sop.Tuple[string, []sop.Handle]{First: sh.RegistryTable, Second: sh.IDs}); err != nil {
			return err
		}
		for _, h := range sh.IDs {
			// Tolerate Redis cache failure.
			if err := r.cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry UpdateNoLocks (redis setstruct) failed, details: %v", err))
			}
		}
	}
	return nil
}

func (r registryOnDisk) Get(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	storesHandles := make([]sop.RegistryPayload[sop.Handle], 0, len(storesLids))
	for _, storeLids := range storesLids {
		handles := make([]sop.Handle, 0, len(storeLids.IDs))
		lids := make([]sop.UUID, 0, len(storeLids.IDs))
		for i := range storeLids.IDs {
			h := sop.Handle{}
			var err error
			if storeLids.IsCacheTTL {
				err = r.cache.GetStructEx(ctx, storeLids.IDs[i].String(), &h, storeLids.CacheDuration)
			} else {
				err = r.cache.GetStruct(ctx, storeLids.IDs[i].String(), &h)
			}
			if err != nil {
				if !r.cache.KeyNotFound(err) {
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

		mh, err := r.hashmap.get(ctx, sop.Tuple[string, []sop.UUID]{First: storeLids.RegistryTable, Second: lids})
		if err != nil {
			return nil, err
		}

		// Add to the handles list the "missing from cache" handles read from registry file.
		for _, handle := range mh[0].Second {
			handles = append(handles, handle)
			if err := r.cache.SetStruct(ctx, handle.LogicalID.String(), &handle, storeLids.CacheDuration); err != nil {
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
func (r registryOnDisk) Remove(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) error {
	for _, storeLids := range storesLids {
		// Flush out the failing records from cache.
		deleteFromCache := func(storeLids sop.RegistryPayload[sop.UUID]) {
			for _, id := range storeLids.IDs {
				if err := r.cache.Delete(ctx, []string{id.String()}); err != nil && !r.cache.KeyNotFound(err) {
					log.Warn(fmt.Sprintf("Registry Delete (redis delete) failed, details: %v", err))
				}
			}
		}
		if err := r.hashmap.remove(ctx, sop.Tuple[string, []sop.UUID]{First: storeLids.RegistryTable, Second: storeLids.IDs}); err != nil {
			deleteFromCache(storeLids)
			return err
		}
		deleteFromCache(storeLids)
	}
	return nil
}

// Write the nodes handles to the target passive destinations.
func (r *registryOnDisk) Replicate(ctx context.Context, newRootNodesHandles, addedNodesHandles,
	updatedNodesHandles, removedNodesHandles []sop.RegistryPayload[sop.Handle]) {

	// Open the hashmaps on the passive destination(s).
	// Write the nodes' handle(s) on each.
	// Close the hashmaps files.
	af := r.replicationTracker.isFirstFolderActive

	// Force tracker to treat passive as active folder so replication can write to the passive destinations.
	r.replicationTracker.isFirstFolderActive = !af
	rm := newRegistryMap(true, r.hashmap.hashmap.hashModValue, r.replicationTracker, r.cache)
	r.replicate(ctx, rm, newRootNodesHandles)
	r.replicate(ctx, rm, addedNodesHandles)
	r.replicate(ctx, rm, updatedNodesHandles)
	r.replicateRemove(ctx, rm, removedNodesHandles)
	rm.close()

	// Restore to the proper active destination(s).
	r.replicationTracker.isFirstFolderActive = af
}

func (r *registryOnDisk) replicate(ctx context.Context, rm *registryMap, nodesHandles []sop.RegistryPayload[sop.Handle]) {
	// for i := range nodesHandles {
	// 	rm.add()
	// 	rm.set(ctx, )
	// }
}
func (r *registryOnDisk) replicateRemove(ctx context.Context, rm *registryMap, nodesHandles []sop.RegistryPayload[sop.Handle]) {

}
