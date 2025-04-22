package fs

import (
	"context"
	"fmt"
	log "log/slog"
	"os"
	"time"

	retry "github.com/sethvargo/go-retry"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/in_memory"
)

// storeRepository is a File System based implementation of store repository.
type storeRepository struct {
	cache       sop.Cache
	fileIO      FileIO
	manageStore sop.ManageStore
	// Array so we can use in replication across two folders, if in replication mode.
	storesBaseFolders []string
	// If true, folder as specified in storesBaseFolders[0] will be the active folder,
	// otherwise the 2nd folder, as specified in storesBaseFolders[1].
	isFirstFolderActive bool
	replicate           bool
}

const (
	lockStoreListKey  = "sr_infs"
	lockDuration      = time.Duration(10 * time.Minute)
	storeListFilename = "storelist.txt"
	storeInfoFilename = "storeinfo.txt"
	// Lock time out for the cache based locking of update store set function.
	updateStoresLockDuration = time.Duration(15 * time.Minute)
)

// NewStoreRepository manages the StoreInfo in a File System.
func NewStoreRepository(storesBaseFolder []string, manageStore sop.ManageStore, cache sop.Cache, replicate bool) (sop.StoreRepository, error) {
	if replicate && len(storesBaseFolder) != 2 {
		return nil, fmt.Errorf("'storesBaseFolder' needs to be exactly two elements if 'replicate' parameter is true")
	}
	isFirstFolderActive := true
	if replicate {
		isFirstFolderActive = detectIfFirstIsActiveFolder(storesBaseFolder)
	}
	return &storeRepository{
		cache:               cache,
		manageStore:         manageStore,
		fileIO:              NewDefaultFileIO(DefaultToFilePath),
		storesBaseFolders:   storesBaseFolder,
		replicate:           replicate,
		isFirstFolderActive: isFirstFolderActive,
	}, nil
}

func detectIfFirstIsActiveFolder(storesBaseFolders []string) bool {
	return true
}

// In the File System implementation, Add function manages the store list in its own file in the base folder
// each store is allocated a sub-folder where store info file is persisted.
//
// Store list is not cached since adding/removing store(s) are rare events.
func (sr *storeRepository) Add(ctx context.Context, stores ...sop.StoreInfo) error {
	// 1. Lock Store List.
	lk := sr.cache.CreateLockKeys(lockStoreListKey)
	defer sr.cache.Unlock(ctx, lk...)
	if err := sr.cache.Lock(ctx, lockDuration, lk...); err != nil {
		return err
	}

	// 2. Get Store List & convert to a map for lookup.
	sl, err := sr.GetAll(ctx)
	if err != nil {
		return err
	}
	storesLookup := make(map[string]byte, len(sl))
	for _, s := range sl {
		storesLookup[s] = 1
	}

	// 3. Merge added items to Store List. Only allow add of store with unique name.
	for _, store := range stores {
		if _, ok := storesLookup[store.Name]; ok {
			return fmt.Errorf("can't add store %s, an existing item with such name exists", store.Name)
		}
		storesLookup[store.Name] = 1
	}

	// 4. Write Store List to tmp file.
	storeWriter := newFileIOWithReplication(sr.replicate, sr.cache, sr.manageStore)
	storeList := make([]string, len(storesLookup))
	i := 0
	for k := range storesLookup {
		storeList[i] = k
		i++
	}
	ba, _ := encoding.Marshal(storeList)
	storeWriter.write(ba, sr.storesBaseFolders, storeListFilename)

	// 5-6. Create folders and write store info to its tmp file, for each added item.
	for _, store := range stores {
		if err := storeWriter.createStore(ctx, sr.storesBaseFolders, store.Name); err != nil {
			return err
		}

		// Persist store info into a JSON text file.
		ba, err := encoding.Marshal(store)
		if err != nil {
			return err
		}

		if err := storeWriter.write(ba, sr.storesBaseFolders, fmt.Sprintf("%c%s%c%s", os.PathSeparator, store.Name, os.PathSeparator, storeInfoFilename)); err != nil {
			return err
		}
	}

	// 7. Replicate the files if configured to.
	if err := storeWriter.replicate(); err != nil {
		return err
	}

	// Cache each of the stores.
	for _, store := range stores {
		if err := sr.cache.SetStruct(ctx, store.Name, &store, store.CacheConfig.StoreInfoCacheDuration); err != nil {
			log.Warn(fmt.Sprintf("StoreRepository Add failed (redis setstruct), details: %v", err))
		}
	}
	return nil
	// 8. Unlock Store List. The defer statement will unlock store list.
}

func (sr *storeRepository) Update(ctx context.Context, stores ...sop.StoreInfo) error {
	// Sort the stores info so we can commit them in same sort order across transactions,
	// thus, reduced chance of deadlock.
	b3 := in_memory.NewBtree[string, sop.StoreInfo](true)
	for i := range stores {
		b3.Add(stores[i].Name, stores[i])
	}
	b3.First()
	keys := make([]string, len(stores))
	i := 0
	for {
		keys[i] = b3.GetCurrentKey()
		stores[i] = b3.GetCurrentValue()
		if !b3.Next() {
			break
		}
		i++
	}

	// Create lock IDs that we can use to logically lock and prevent other updates.
	lockKeys := sr.cache.CreateLockKeys(keys...)

	b := retry.NewFibonacci(1 * time.Second)

	// Lock all keys.
	if err := retry.Do(ctx, retry.WithMaxRetries(5, b), func(ctx context.Context) error {
		// 15 minutes to lock, merge/update details then unlock.
		if err := sr.cache.Lock(ctx, updateStoresLockDuration, lockKeys...); err != nil {
			log.Warn(err.Error() + ", will retry")
			return retry.RetryableError(err)
		}
		return nil
	}); err != nil {
		log.Warn(err.Error() + ", gave up")
		// Unlock keys since we failed locking all of them.
		sr.cache.Unlock(ctx, lockKeys...)
		return err
	}

	storeWriter := newFileIOWithReplication(sr.replicate, sr.cache, sr.manageStore)

	undo := func(endIndex int, original []sop.StoreInfo) {
		// Attempt to undo changes, 'ignores error as it is a last attempt to cleanup.
		for ii := 0; ii < endIndex; ii++ {
			log.Debug(fmt.Sprintf("undo occured for store %s", stores[ii].Name))

			sis, _ := sr.GetWithTTL(ctx, stores[ii].CacheConfig.IsStoreInfoCacheTTL, stores[ii].CacheConfig.StoreInfoCacheDuration, stores[ii].Name)
			if len(sis) == 0 {
				continue
			}

			si := sis[0]
			// Reverse the count delta should restore to true count value.
			si.Count = si.Count - stores[ii].CountDelta
			si.Timestamp = original[ii].Timestamp

			// Persist store info into a JSON text file.
			ba, err := encoding.Marshal(si)
			if err != nil {
				log.Warn(fmt.Sprintf("StoreRepository Update Undo store %s failed Marshal, details: %v", si.Name, err))
				continue
			}

			if err := storeWriter.write(ba, sr.storesBaseFolders, fmt.Sprintf("%c%s%c%s", os.PathSeparator, si.Name, os.PathSeparator, storeInfoFilename)); err != nil {
				log.Warn(fmt.Sprintf("StoreRepository Update Undo store %s failed write, details: %v", si.Name, err))
				continue
			}

			// Tolerate redis error since we've successfully updated the master table.
			if err := sr.cache.SetStruct(ctx, si.Name, &si, si.CacheConfig.StoreInfoCacheDuration); err != nil {
				log.Warn(fmt.Sprintf("StoreRepository Update Undo (redis setstruct) store %s failed, details: %v", si.Name, err))
			}
		}
	}

	beforeUpdateStores := make([]sop.StoreInfo, 0, len(stores))
	// Unlock all keys before going out of scope.
	defer sr.cache.Unlock(ctx, lockKeys...)

	for i := range stores {
		sis, err := sr.GetWithTTL(ctx, stores[i].CacheConfig.IsStoreInfoCacheTTL, stores[i].CacheConfig.StoreInfoCacheDuration, stores[i].Name)
		if len(sis) == 0 {
			undo(i, beforeUpdateStores)
			return err
		}
		beforeUpdateStores = append(beforeUpdateStores, sis...)

		si := sis[0]
		// Merge or apply the "count delta".
		stores[i].Count = si.Count + stores[i].CountDelta

		// Persist store info into a JSON text file.
		ba, err := encoding.Marshal(si)
		if err != nil {
			// Undo changes.
			undo(i, beforeUpdateStores)
			return err
		}

		if err := storeWriter.write(ba, sr.storesBaseFolders, fmt.Sprintf("%c%s%c%s", os.PathSeparator, si.Name, os.PathSeparator, storeInfoFilename)); err != nil {
			// Undo changes.
			undo(i, beforeUpdateStores)
			return err
		}
	}

	// Replicate the files if configured to.
	if err := storeWriter.replicate(); err != nil {
		return err
	}

	// Cache each of the stores.
	for _, store := range stores {
		if err := sr.cache.SetStruct(ctx, store.Name, &store, store.CacheConfig.StoreInfoCacheDuration); err != nil {
			log.Warn(fmt.Sprintf("StoreRepository Update (redis setstruct) failed, details: %v", err))
		}
	}

	return nil
}

func (sr *storeRepository) Get(ctx context.Context, names ...string) ([]sop.StoreInfo, error) {
	return sr.GetWithTTL(ctx, false, 0, names...)
}

func (sr *storeRepository) GetAll(ctx context.Context) ([]string, error) {
	fio := newFileIOWithReplication(sr.replicate, sr.cache, sr.manageStore)
	ba, err := fio.read(sr.storesBaseFolders, storeListFilename)
	if err != nil {
		return nil, err
	}
	var storeList []string
	err = encoding.Unmarshal(ba, &storeList)
	// No need to cache the store list. (by intent, for now)
	return storeList, err
}

func (sr *storeRepository) GetWithTTL(ctx context.Context, isCacheTTL bool, cacheDuration time.Duration, names ...string) ([]sop.StoreInfo, error) {
	stores := make([]sop.StoreInfo, 0, len(names))
	storesNotInCache := make([]string, 0)
	for i := range names {
		store := sop.StoreInfo{}
		var err error
		if isCacheTTL {
			err = sr.cache.GetStructEx(ctx, names[i], &store, cacheDuration)
		} else {
			err = sr.cache.GetStruct(ctx, names[i], &store)
		}
		if err != nil {
			if !sr.cache.KeyNotFound(err) {
				log.Warn(fmt.Sprintf("StoreRepository Get (redis getstruct) failed, details: %v", err))
			}
			storesNotInCache = append(storesNotInCache, names[i])
			continue
		}
		stores = append(stores, store)
	}
	if len(storesNotInCache) == 0 {
		return stores, nil
	}

	sio := newFileIOWithReplication(sr.replicate, sr.cache, sr.manageStore)
	for _, s := range storesNotInCache {

		ba, err := sio.read(sr.storesBaseFolders, fmt.Sprintf("%c%s%c%s", os.PathSeparator, s, os.PathSeparator, storeInfoFilename))
		if err != nil {
			return nil, err
		}

		var store sop.StoreInfo
		if err = encoding.Unmarshal(ba, &store); err != nil {
			return nil, err
		}

		if err := sr.cache.SetStruct(ctx, store.Name, &store, store.CacheConfig.StoreInfoCacheDuration); err != nil {
			log.Warn(fmt.Sprintf("StoreRepository GetWithTTL (redis setstruct) failed, details: %v", err))
		}

		stores = append(stores, store)
	}

	return stores, nil
}

// Remove is destructive and shold only be done in an exclusive (admin only) operation.
// Any deleted tables can't be rolled back. This is equivalent to DDL SQL script, which we
// don't do part of a transaction.
func (sr *storeRepository) Remove(ctx context.Context, storeNames ...string) error {
	lk := sr.cache.CreateLockKeys(lockStoreListKey)
	defer sr.cache.Unlock(ctx, lk...)
	if err := sr.cache.Lock(ctx, lockDuration, lk...); err != nil {
		return err
	}

	// Get Store List & convert to a map for lookup.
	sl, err := sr.GetAll(ctx)
	if err != nil {
		return err
	}
	storesLookup := make(map[string]byte, len(sl))
	for _, s := range sl {
		storesLookup[s] = 1
	}

	// Remove store(s) that exists.
	for _, storeName := range storeNames {
		if _, ok := storesLookup[storeName]; !ok {
			log.Warn(fmt.Sprintf("can't remove store %s, there is no item with such name", storeName))
			continue
		}

		// Tolerate Redis cache failure.
		if err := sr.cache.Delete(ctx, storeName); err != nil && !sr.cache.KeyNotFound(err) {
			log.Warn(fmt.Sprintf("StoreRepository Remove (redis Delete) failed, details: %v", err))
		}
		// Delete store folder (contains blobs, store config & registry data files).
		if err := sr.manageStore.RemoveStore(ctx, storeName); err != nil {
			return err
		}

		delete(storesLookup, storeName)
	}

	// Update Store list file of removed entries.
	storeWriter := newFileIOWithReplication(sr.replicate, sr.cache, sr.manageStore)
	storeList := make([]string, len(storesLookup))
	i := 0
	for k := range storesLookup {
		storeList[i] = k
		i++
	}
	ba, _ := encoding.Marshal(storeList)
	storeWriter.write(ba, sr.storesBaseFolders, storeListFilename)

	// Replicate the files if configured to.
	if err := storeWriter.replicate(); err != nil {
		return err
	}

	return nil
}
