package fs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

// storeRepository is a simple in-memory implementation of store repository to demonstrate
// or mockup the structure composition, so we can define it in preparation of v2.
type storeRepository struct {
	cache            sop.Cache
	fileIO           FileIO
	manageStore      sop.ManageStore
	// Array so we can use in replication across two folders, if in replication mode.
	storesBaseFolders []string
	// If true, folder as specified in storesFolders[0] will be the active folder, otherwise the 2nd folder, as specified in storesFolders[1].
	isFirstFolderActive bool
	replicate bool
}

const (
	lockStoreListKey = "sr_infs"
	lockDuration     = 5 * time.Minute
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
		cache:            cache,
		manageStore:      manageStore,
		fileIO:           NewDefaultFileIO(DefaultToFilePath),
		storesBaseFolders: storesBaseFolder,
		replicate: replicate,
		isFirstFolderActive: isFirstFolderActive,
	}, nil
}

func detectIfFirstIsActiveFolder(storesBaseFolders []string) bool {
	return true
}

func (sr *storeRepository) Add(ctx context.Context, stores ...sop.StoreInfo) error {

	/*
		1. Lock Store List
		2. Get Store List
		3. Merge added items to Store List
		4. Write Store List to tmp file
		5. Create folders and write store info to its tmp file, for each added item
		6. Finalize added items' tmp files. Ensure to delete items' tmp files
		7. Finalize Store List tmp file. Ensure to delete Store List tmp file
		8. Unlock Store List
	*/

	lk := sr.cache.CreateLockKeys(lockStoreListKey)
	if err := sr.cache.Lock(ctx, lockDuration, lk...); err != nil {
		return err
	}
	//defer sr.cache.Unlock(ctx, lk...)

	storesLookup, err := sr.getAll(ctx)
	if err != nil {
		return err
	}

	// Only allow add of store with unique name.
	for _, store := range stores {
		if _, ok := storesLookup[store.Name]; ok {
			return fmt.Errorf("can't add store %s, an existing item with such name exists", store.Name)
		}
		storesLookup[store.Name] = 1
	}

	// Write Store List to tmp file.
	storeListWriter := newFileWriterAndReplicator(sr.replicate)
	storeList := make([]string, len(storesLookup))
	i := 0
	for k, _ := range storesLookup {
		storeList[i] = k
		i++
	}
	slfn1 := fmt.Sprintf("%s%cstorelist.txt", sr.storesBaseFolders[0], os.PathSeparator)
	slfn2 := ""
	if sr.replicate {
		slfn2 = fmt.Sprintf("%s%cstorelist.txt", sr.storesBaseFolders[0], os.PathSeparator)
	}
	ba, _ := encoding.Marshal(storeList)
	storeListWriter.writeToTemp(ba, slfn1, slfn2)

	// Create folders and write store info to its tmp file, for each added item
	if err := sr.Update(ctx, stores...); err != nil {
		return err
	}

	// Finalize added items' tmp files. Ensure to delete items' tmp files

	// 7. Finalize Store List tmp file. Ensure to delete Store List tmp file
	// 8. Unlock Store List

	if err := storeListWriter.finalize(); err != nil {

	}

	// // Persist stores info into a JSON text file.
	// ba, err := json.Marshal(stores)
	// if err != nil {
	// 	return err
	// }
	// if err := sr.fileIO.WriteFile(fn, ba, permission); err != nil {
	// 	return err
	// }

	// for _, store := range stores {
	// 	if err := sr.manageStore.CreateStore(ctx, store.BlobTable); err != nil {
	// 		return err
	// 	}
	// 	// Persist store info into a JSON text file.
	// 	fn := fmt.Sprintf("%s%cstoreinfo.txt", store.BlobTable, os.PathSeparator)
	// 	ba, err := json.Marshal(store)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if err := sr.fileIO.WriteFile(fn, ba, permission); err != nil {
	// 		return err
	// 	}
	// 	// TODO: add to cache.
	// }
	return nil
}

func (sr *storeRepository) Update(ctx context.Context, stores ...sop.StoreInfo) error {
	for _, store := range stores {
		si, err := sr.Get(ctx, store.Name)
		if err != nil {
			return err
		}
		// Merge or apply the "count delta".
		store.Count = si[0].Count + store.CountDelta
		store.CountDelta = 0
		if err := sr.Update(ctx, store); err != nil {
			return err
		}
		// Persiste store info into a JSON text file.
		fn := fmt.Sprintf("%s%cstoreinfo.txt", store.BlobTable, os.PathSeparator)
		ba, err := json.Marshal(store)
		if err != nil {
			return err
		}
		if err := sr.fileIO.WriteFile(fn, ba, permission); err != nil {
			return err
		}
		// TODO: add to cache.
	}
	return nil
}

func (sr *storeRepository) update(ctx context.Context, stores ...sop.StoreInfo) error {
	for _, store := range stores {
		si, err := sr.Get(ctx, store.Name)
		if err != nil {
			return err
		}
		// Merge or apply the "count delta".
		store.Count = si[0].Count + store.CountDelta
		store.CountDelta = 0
		if err := sr.Update(ctx, store); err != nil {
			return err
		}
		// Persiste store info into a JSON text file.
		fn := fmt.Sprintf("%s%cstoreinfo.txt", store.BlobTable, os.PathSeparator)
		ba, err := json.Marshal(store)
		if err != nil {
			return err
		}
		if err := sr.fileIO.WriteFile(fn, ba, permission); err != nil {
			return err
		}
		// TODO: add to cache.
	}
	return nil
}

func (sr *storeRepository) Get(ctx context.Context, names ...string) ([]sop.StoreInfo, error) {
	return sr.GetWithTTL(ctx, false, 0, names...)
}

func (sr *storeRepository) getAll(ctx context.Context) (map[string]byte, error) {
	sl, err := sr.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	m := make(map[string]byte, len(sl))
	for _, s := range sl {
		m[s] = 1
	}
	return m, nil
}

func (sr *storeRepository) GetAll(ctx context.Context) ([]string, error) {	
	fn := fmt.Sprintf("%s%cstorelist.txt", sr.storesBaseFolders[0], os.PathSeparator)
	if sr.replicate && !sr.isFirstFolderActive {
		fn = fmt.Sprintf("%s%cstorelist.txt", sr.storesBaseFolders[1], os.PathSeparator)
	}
	ba, err := sr.fileIO.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	var storeList []string
	err = encoding.Unmarshal(ba, &storeList)
	// No need to cache the store list. (by intent, for now)
	return storeList, err
}

func (sr *storeRepository) GetWithTTL(ctx context.Context, isCacheTTL bool, cacheDuration time.Duration, names ...string) ([]sop.StoreInfo, error) {
	// stores := make([]sop.StoreInfo, len(names))
	// for i, name := range names {
	// 	v := sr.lookup[name]
	// 	stores[i] = v
	// }
	// return stores, nil
	return nil, nil
}

func (sr *storeRepository) Remove(ctx context.Context, names ...string) error {
	for _, name := range names {
		if err := sr.manageStore.RemoveStore(ctx, name); err != nil {
			return err
		}
		// TODO: remove from cache.
	}
	return nil
}
