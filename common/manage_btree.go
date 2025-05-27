// Package in_red_ck contains SOP implementations that uses Redis for caching & Cassandra for backend data storage.
package common

import (
	"context"
	"fmt"
	log "log/slog"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
)

// OpenBtree will open an existing B-Tree instance & prepare it for use in a transaction.
func OpenBtree[TK btree.Ordered, TV any](ctx context.Context, name string, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if t == nil {
		return nil, fmt.Errorf("transaction 't' parameter can't be nil")
	}
	if !t.HasBegun() {
		return nil, fmt.Errorf("transaction 't' parameter has not started")
	}

	var t2 interface{} = t.GetPhasedTransaction()
	trans := t2.(*Transaction)
	stores, err := trans.storeRepository.Get(ctx, name)
	if len(stores) == 0 || stores[0].IsEmpty() || err != nil {
		if err == nil {
			trans.Rollback(ctx)
			return nil, fmt.Errorf("B-Tree '%s' does not exist, please use NewBtree to create an instance of it", name)
		}
		trans.Rollback(ctx)
		return nil, err
	}
	return newBtree[TK, TV](ctx, &stores[0], trans, comparer)
}

// NewBtree will create a new B-Tree instance with data persisted to backend storage upon commit.
// If B-Tree(name) is not found in the backend, a new one will be created. Otherwise, the existing one will be opened
// and the parameters checked if matching. If you know that it exists, then it is more convenient and more readable to call
// the OpenBtree function.
func NewBtree[TK btree.Ordered, TV any](ctx context.Context, si sop.StoreOptions, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if t == nil {
		return nil, fmt.Errorf("transaction 't' parameter can't be nil")
	}
	if !t.HasBegun() {
		return nil, fmt.Errorf("transaction 't' parameter has not started")
	}

	var t2 any = t.GetPhasedTransaction()
	trans := t2.(*Transaction)

	var stores []sop.StoreInfo
	var err error
	if si.CacheConfig != nil {
		stores, err = trans.storeRepository.GetWithTTL(ctx, si.CacheConfig.IsStoreInfoCacheTTL, si.CacheConfig.StoreInfoCacheDuration, si.Name)
	} else {
		stores, err = trans.storeRepository.Get(ctx, si.Name)
	}
	if err != nil {
		trans.Rollback(ctx)
		return nil, err
	}
	ns := sop.NewStoreInfoExt(si.Name, si.SlotLength, si.IsUnique, si.IsValueDataInNodeSegment, si.IsValueDataActivelyPersisted, si.IsValueDataGloballyCached, si.LeafLoadBalancing, si.Description, si.BlobStoreBaseFolderPath, si.CacheConfig)

	// Allow caller to use the same name for blob store and the store name.
	if si.DisableBlobStoreFormatting {
		ns.BlobTable = ns.Name
	}
	// Allow caller to use the same name for registry store and the store name.
	if si.DisableRegistryStoreFormatting {
		ns.RegistryTable = ns.Name
	}

	if len(stores) == 0 || stores[0].IsEmpty() {
		// Add to store repository if store not found.
		if ns.RootNodeID.IsNil() {
			// Pre-assign root node ID so B-Trees can merge newly created root nodes on commit.
			ns.RootNodeID = sop.NewUUID()
			ns.Timestamp = sop.Now().UnixMilli()
		}
		if err := trans.storeRepository.Add(ctx, *ns); err != nil {
			// Cleanup the store if there was anything added in backend.
			trans.storeRepository.Remove(ctx, ns.Name)
			trans.Rollback(ctx)
			return nil, err
		}
		return newBtree[TK, TV](ctx, ns, trans, comparer)
	}
	// Check if store retrieved is empty or of non-compatible specification.
	if !ns.IsCompatible(stores[0]) {
		trans.Rollback(ctx)
		// Recommend to use the OpenBtree function to open it.
		return nil, fmt.Errorf("B-Tree '%s' exists, please use OpenBtree to open & create an instance of it", si.Name)
	}
	ns = &stores[0]
	return newBtree[TK, TV](ctx, ns, trans, comparer)
}

func newBtree[TK btree.Ordered, TV any](ctx context.Context, s *sop.StoreInfo, trans *Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	si := StoreInterface[TK, TV]{}

	// Assign the item action tracker frontend and backend bits.
	iat := newItemActionTracker[TK, TV](s, trans.l2Cache, trans.blobStore, trans.logger)
	si.ItemActionTracker = iat

	// Assign the node repository frontend and backend bits.
	nrw := newNodeRepository[TK, TV](trans, s)
	si.NodeRepository = nrw
	si.backendNodeRepository = nrw.backendNodeRepository

	// Wire up the B-tree & the backend bits required by the transaction.
	b3, err := btree.New(s, &si.StoreInterface, comparer)
	if err != nil {
		trans.Rollback(ctx)
		return nil, err
	}

	// B-Tree backend processing(of commit & rollback) required objects.
	b3b := btreeBackend{
		// Node blob repository.
		nodeRepository: nrw.backendNodeRepository,
		// Needed for auto-merging of Node contents.
		refetchAndMerge: refetchAndMergeClosure(&si, b3, trans.storeRepository),
		// Needed when applying the "delta" to the Store Count field.
		getStoreInfo: func() *sop.StoreInfo { return b3.StoreInfo },

		// Needed for tracked items' lock & "value data" in separate segments management.
		commitTrackedItemsValues:         iat.commitTrackedItemsValues,
		getForRollbackTrackedItemsValues: iat.getForRollbackTrackedItemsValues,
		getObsoleteTrackedItemsValues:    iat.getObsoleteTrackedItemsValues,

		hasTrackedItems:    iat.hasTrackedItems,
		checkTrackedItems:  iat.checkTrackedItems,
		lockTrackedItems:   iat.lock,
		unlockTrackedItems: iat.unlock,
	}
	trans.btreesBackend = append(trans.btreesBackend, b3b)

	return btree.NewBtreeWithTransaction(trans, b3), nil
}

// Use tracked Items to refetch their Nodes(using B-Tree) and merge the changes in, if there is no conflict.
func refetchAndMergeClosure[TK btree.Ordered, TV any](si *StoreInterface[TK, TV], b3 *btree.Btree[TK, TV], sr sop.StoreRepository) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		b3ModifiedItems := si.ItemActionTracker.(*itemActionTracker[TK, TV]).items
		// Clear the backend "cache" so we can force B-Tree to re-fetch from Redis(or BlobStore).
		si.ItemActionTracker.(*itemActionTracker[TK, TV]).items = make(map[sop.UUID]cacheItem[TK, TV])
		si.backendNodeRepository.localCache = make(map[sop.UUID]cachedNode)
		si.backendNodeRepository.readNodesCache.Clear()
		// Reset StoreInfo of B-Tree in prep to replay the "actions".
		storeInfo, err := sr.GetWithTTL(ctx, b3.StoreInfo.CacheConfig.IsStoreInfoCacheTTL, b3.StoreInfo.CacheConfig.StoreInfoCacheDuration, b3.StoreInfo.Name)
		if err != nil {
			return err
		}

		// Reset the internal variables with value from backend Store DB.
		b3.StoreInfo.Count = storeInfo[0].Count
		si.backendNodeRepository.count = storeInfo[0].Count
		b3.StoreInfo.RootNodeID = storeInfo[0].RootNodeID

		for uuid, ci := range b3ModifiedItems {
			log.Debug(fmt.Sprintf("inside refetchAndMergeClosure, tid: %v", si.backendNodeRepository.transaction.GetID()))
			if ci.Action == addAction {
				if !b3.StoreInfo.IsValueDataInNodeSegment {
					if ok, err := b3.AddItem(ctx, ci.item); !ok || err != nil {
						if err != nil {
							return err
						}
						return fmt.Errorf("refetchAndMergeModifications failed to merge add item with key %v", ci.item.Key)
					}
					ci.persisted = true
					si.ItemActionTracker.(*itemActionTracker[TK, TV]).items[ci.item.ID] = ci
					continue
				}
				if ok, err := b3.Add(ctx, ci.item.Key, *ci.item.Value); !ok || err != nil {
					if err != nil {
						return err
					}
					return fmt.Errorf("refetchAndMergeModifications failed to merge add item with key %v", ci.item.Key)
				}
				continue
			}
			if ok, err := b3.FindOneWithID(ctx, ci.item.Key, uuid); !ok || err != nil {
				if err != nil {
					return err
				}
				return fmt.Errorf("refetchAndMergeModifications failed to find item with key %v", ci.item.Key)
			}

			// Check if the item read from backend has been updated since the time we read it.
			item, err := b3.GetCurrentItem(ctx)
			if err != nil || item.Version != ci.versionInDB {
				if err != nil {
					return err
				}
				return fmt.Errorf("refetchAndMergeModifications detected a newer version of item with key %v", ci.item.Key)
			}

			if ci.Action == getAction {
				// GetCurrentItem call above already "marked" the "get" (or fetch) done.
				continue
			}
			if ci.Action == removeAction {
				if ok, err := b3.RemoveCurrentItem(ctx); !ok || err != nil {
					if err != nil {
						return err
					}
					return fmt.Errorf("refetchAndMergeModifications failed to merge remove item with key %v", ci.item.Key)
				}
				continue
			}
			if ci.Action == updateAction {
				if !b3.StoreInfo.IsValueDataInNodeSegment {
					// Merge the inflight Item ID with target.
					si.ItemActionTracker.(*itemActionTracker[TK, TV]).forDeletionItems = append(
						si.ItemActionTracker.(*itemActionTracker[TK, TV]).forDeletionItems, item.ID)
					delete(si.ItemActionTracker.(*itemActionTracker[TK, TV]).items, item.ID)
					ci.persisted = true
					si.ItemActionTracker.(*itemActionTracker[TK, TV]).items[ci.item.ID] = ci

					// Ensure Btree will do everything else needed to update current Item, except merge change(above).
					if ok, err := b3.UpdateCurrentNodeItem(ctx, ci.item); !ok || err != nil {
						if err != nil {
							return err
						}
						return fmt.Errorf("refetchAndMergeModifications failed to merge update item with key %v", ci.item.Key)
					}
					continue
				}
				if ok, err := b3.UpdateCurrentItem(ctx, *ci.item.Value); !ok || err != nil {
					if err != nil {
						return err
					}
					return fmt.Errorf("refetchAndMergeModifications failed to merge update item with key %v", ci.item.Key)
				}
			}
		}
		return nil
	}
}
