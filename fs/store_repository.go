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
	lockStoreListKey = "sr_infs"
	lockDuration     = 5 * time.Minute
	storeListFilename = "storelist.txt"
	storeInfoFilename = "storeinfo.txt"
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
	// 1. Lock Store List.
	lk := sr.cache.CreateLockKeys(lockStoreListKey)
	if err := sr.cache.Lock(ctx, lockDuration, lk...); err != nil {
		return err
	}
	defer sr.cache.Unlock(ctx, lk...)

	// 2. Get Store List.
	storesLookup, err := sr.getAll(ctx)
	if err != nil {
		return err
	}

	// 3. Merge added items to Store List. Only allow add of store with unique name.
	for _, store := range stores {
		if _, ok := storesLookup[store.Name]; ok {
			return fmt.Errorf("can't add store %s, an existing item with such name exists", store.Name)
		}
		storesLookup[store.Name] = 1
	}

	// 4. Write Store List to tmp file.
	storeWriter := newFileWriterWithReplication(sr.replicate, sr.cache, sr.manageStore)
	storeList := make([]string, len(storesLookup))
	i := 0
	for k := range storesLookup {
		storeList[i] = k
		i++
	}
	ba, _ := encoding.Marshal(storeList)
	storeWriter.writeToTemp(ba, sr.storesBaseFolders, storeListFilename)

	// 5-6. Create folders and write store info to its tmp file, for each added item.
	// Finalize added items' tmp files. Ensure to delete items' tmp files.
	for _, store := range stores {
		// Create the store sub-folder.
		// sifn1 := fmt.Sprintf("%s%c%s", sr.storesBaseFolders[0], os.PathSeparator, store.Name)
		// sr.manageStore.CreateStore(ctx, sifn1)
		// sifn2 := ""
		if err := storeWriter.createStore(ctx, sr.storesBaseFolders, store.Name); err != nil {
			return err
		}

		// Persist store info into a JSON text file.
		ba, err := json.Marshal(store)
		if err != nil {
			return err
		}

		if err := storeWriter.writeToTemp(ba, sr.storesBaseFolders, fmt.Sprintf("%c%s%c%s", os.PathSeparator, store.Name, os.PathSeparator, storeInfoFilename)); err != nil {
			return err
		}
	}
	
	// 7. Finalize added items' tmp files. Ensure to delete items' tmp files.
	return storeWriter.finalize()

	// 8. Unlock Store List. The defer statement will unlock store list.
}


// TODO: (next up)
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
