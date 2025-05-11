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
func NewRegistry(readWrite bool, hashModValue int, rt *replicationTracker, cache sop.Cache, useCacheForFileRegionLocks bool) Registry {
	return &registryOnDisk{
		hashmap:            newRegistryMap(readWrite, hashModValue, rt, cache, useCacheForFileRegionLocks),
		replicationTracker: rt,
		cache:              cache,
	}
}

// Close all opened file handles.
func (r registryOnDisk) Close() error {
	return r.hashmap.close()
}

func (r registryOnDisk) Add(ctx context.Context, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	for _, sh := range storesHandles {
		for _, h := range sh.IDs {
			if err := r.hashmap.add(ctx, sop.Tuple[string, []sop.Handle]{First: sh.RegistryTable, Second: []sop.Handle{h}}); err != nil {
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

func (r registryOnDisk) Update(ctx context.Context, allOrNothing bool, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	if len(storesHandles) == 0 {
		return nil
	}

	// Logged batch will do all or nothing. This is the only one "all or nothing" operation in the Commit process.
	if allOrNothing {
		handleKeys := make([]*sop.LockKey, 0, len(storesHandles)*4)

		for _, sh := range storesHandles {
			for _, h := range sh.IDs {
				var h2 sop.Handle
				if err := r.cache.GetStruct(ctx, h.LogicalID.String(), &h2); err != nil {
					if r.cache.KeyNotFound(err) {
						err = &sop.UpdateAllOrNothingError{
							Err: fmt.Errorf("Registry Update failed, handle %s not in cache", h.LogicalID.String()),
						}
					} else {
						err = &sop.UpdateAllOrNothingError{
							Err: fmt.Errorf("Registry Update failed, err getting handle %s data from cache, details: %v", h.LogicalID.String(), err),
						}
					}
					// Unlock the object Keys before return.
					r.cache.Unlock(ctx, handleKeys...)
					return err
				}
				newVersion := h.Version
				// Version ID is incremental, 'thus we can compare with -1 the previous.
				newVersion--
				if newVersion != h2.Version || !h.IsEqual(&h2) {
					// Unlock the object Keys before return.
					r.cache.Unlock(ctx, handleKeys...)
					return &sop.UpdateAllOrNothingError{
						Err: fmt.Errorf("Registry Update failed, handle logical ID(%v) version conflict detected", h.LogicalID.String()),
					}
				}
				// Attempt to lock in Redis the registry object, if we can't attain a lock, it means there is another transaction elsewhere
				// that already attained a lock, thus, we can cause rollback of this transaction due to conflict).
				lk := r.cache.CreateLockKeys(h.LogicalID.String())
				handleKeys = append(handleKeys, lk[0])

				if ok, err := r.cache.Lock(ctx, updateAllOrNothingOfHandleSetLockTimeout, lk[0]); !ok || err != nil {
					if err == nil {
						err = &sop.UpdateAllOrNothingError{
							Err: fmt.Errorf("lock allOrNothing failed, key %v is already locked by another", lk[0].Key),
						}
					}
					// Unlock the object Keys before return.
					r.cache.Unlock(ctx, handleKeys...)
					return err
				}
			}
		}

		batch := make([]sop.Tuple[string, []sop.Handle], 0, len(storesHandles))
		for _, sh := range storesHandles {
			batch = append(batch, sop.Tuple[string, []sop.Handle]{First: sh.RegistryTable, Second: sh.IDs})
		}

		// Check the locks to cater for potential race condition.
		if ok, err := r.cache.IsLocked(ctx, handleKeys...); !ok || err != nil {
			if err == nil {
				err = &sop.UpdateAllOrNothingError{
					Err: fmt.Errorf("isLocked allOrNothing failed, key(s) locked by another"),
				}
			}
			// Unlock the object Keys before return.
			r.cache.Unlock(ctx, handleKeys...)
			return err
		}

		// Execute the batch set, all or nothing.
		if err := r.hashmap.set(ctx, true, batch...); err != nil {
			// Unlock the object Keys before return.
			r.cache.Unlock(ctx, handleKeys...)
			// Failed update all, thus, return err to cause rollback.
			return err
		}

		// Update redis cache.
		for _, sh := range storesHandles {
			for _, h := range sh.IDs {
				// Tolerate Redis cache failure.
				if err := r.cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
					log.Warn(fmt.Sprintf("Registry Update (redis setstruct) failed, details: %v", err))
				}
			}
		}

		// Unlock the object Keys before return.
		r.cache.Unlock(ctx, handleKeys...)
	} else {
		for _, sh := range storesHandles {
			// Fail on 1st encountered error. It is non-critical operation, SOP can "heal" those got left.
			for _, h := range sh.IDs {
				// Update registry record.
				lk := r.cache.CreateLockKeys(h.LogicalID.String())
				if ok, err := r.cache.Lock(ctx, updateAllOrNothingOfHandleSetLockTimeout, lk[0]); !ok || err != nil {
					if err == nil {
						err = &sop.UpdateAllOrNothingError{
							Err: fmt.Errorf("lock failed, key %v is already locked by another", lk[0].Key),
						}
					}
					return err
				}
				if err := r.hashmap.set(ctx, false, sop.Tuple[string, []sop.Handle]{First: sh.RegistryTable, Second: []sop.Handle{h}}); err != nil {
					// Unlock the object Keys before return.
					r.cache.Unlock(ctx, lk[0])
					return err
				}
				// Tolerate Redis cache failure.
				if err := r.cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
					log.Warn(fmt.Sprintf("Registry Update (redis setstruct) failed, details: %v", err))
				}
				// Unlock the object Keys.
				if err := r.cache.Unlock(ctx, lk[0]); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r registryOnDisk) Get(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
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
func (r registryOnDisk) Remove(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) error {
	for _, storeLids := range storesLids {
		// Flush out the failing records from cache.
		deleteFromCache := func(storeLids sop.RegistryPayload[sop.UUID]) {
			for _, id := range storeLids.IDs {
				if err := r.cache.Delete(ctx, id.String()); err != nil && !r.cache.KeyNotFound(err) {
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
