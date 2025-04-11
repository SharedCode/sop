package fs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/SharedCode/sop"
)

// storeRepository is a simple in-memory implementation of store repository to demonstrate
// or mockup the structure composition, so we can define it in preparation of v2.
type storeRepositoryWithReplication struct {
	cache       sop.Cache
	fileIO FileIO
	manageStore sop.ManageStore
	// Replication across two folders.
	storesBaseFolders []string
}

// NewStoreRepository manages the StoreInfo in a File System.
func NewStoreRepositoryWithReplication(storesBaseFolders []string, manageStore sop.ManageStore, cache sop.Cache) sop.StoreRepository {
	return &storeRepositoryWithReplication{
		cache:       cache,
		manageStore: manageStore,
		fileIO: NewDefaultFileIO(DefaultToFilePath),
		storesBaseFolders : storesBaseFolders,
	}
}

func (sr *storeRepositoryWithReplication) Add(ctx context.Context, stores ...sop.StoreInfo) error {
	for _, store := range stores {
		if err := sr.manageStore.CreateStore(ctx, store.BlobTable); err != nil {
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

func (sr *storeRepositoryWithReplication) Update(ctx context.Context, stores ...sop.StoreInfo) error {
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

func (sr *storeRepositoryWithReplication) Get(ctx context.Context, names ...string) ([]sop.StoreInfo, error) {
	return sr.GetWithTTL(ctx, false, 0, names...)
}

func (sr *storeRepositoryWithReplication) GetAll(ctx context.Context) ([]string, error) {
	// storeNames := make([]string, len(sr.lookup))
	// var i = 0
	// for k := range sr.lookup {
	// 	storeNames[i] = k
	// 	i++
	// }
	// return storeNames, nil
	return nil,nil
}

func (sr *storeRepositoryWithReplication) GetWithTTL(ctx context.Context, isCacheTTL bool, cacheDuration time.Duration, names ...string) ([]sop.StoreInfo, error) {
	// stores := make([]sop.StoreInfo, len(names))
	// for i, name := range names {
	// 	v := sr.lookup[name]
	// 	stores[i] = v
	// }
	// return stores, nil
	return nil,nil
}

func (sr *storeRepositoryWithReplication) Remove(ctx context.Context, names ...string) error {
	for _, name := range names {
		if err := sr.manageStore.RemoveStore(ctx, name); err != nil {
			return err
		}
		// TODO: remove from cache.
	}
	return nil
}
