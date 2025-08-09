// Package common contains shared transaction and B-tree management helpers used by SOP.
package common

import (
	"context"
	"fmt"
	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
)

// OpenBtree will open an existing B-Tree instance & prepare it for use in a transaction.
// Requires an active transaction. Returns an error if the store does not exist.
func OpenBtree[TK btree.Ordered, TV any](ctx context.Context, name string, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if t == nil {
		return nil, fmt.Errorf("transaction 't' parameter can't be nil")
	}
	if !t.HasBegun() {
		return nil, fmt.Errorf("transaction 't' parameter has not started")
	}
	if name == "" {
		return nil, fmt.Errorf("b-tree name can't be empty string")
	}

	var t2 interface{} = t.GetPhasedTransaction()
	trans := t2.(*Transaction)
	stores, err := trans.StoreRepository.Get(ctx, name)
	if len(stores) == 0 || stores[0].IsEmpty() || err != nil {
		if err == nil {
			trans.Rollback(ctx, nil)
			return nil, fmt.Errorf("b-tree '%s' does not exist, please use NewBtree to create an instance of it", name)
		}
		trans.Rollback(ctx, err)
		return nil, err
	}
	return newBtree[TK, TV](ctx, &stores[0], trans, comparer)
}

// NewBtree will create a new B-Tree instance with data persisted to backend storage upon commit.
// If the store exists, it opens it and validates compatibility with the provided options.
// When creating a new store, a root node ID is preassigned for commit-time merging.
func NewBtree[TK btree.Ordered, TV any](ctx context.Context, si sop.StoreOptions, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if t == nil {
		return nil, fmt.Errorf("transaction 't' parameter can't be nil")
	}
	if !t.HasBegun() {
		return nil, fmt.Errorf("transaction 't' parameter has not started")
	}
	if si.Name == "" {
		return nil, fmt.Errorf("b-tree name can't be empty string")
	}

	var t2 any = t.GetPhasedTransaction()
	trans := t2.(*Transaction)

	var stores []sop.StoreInfo
	var err error
	if si.CacheConfig != nil {
		stores, err = trans.StoreRepository.GetWithTTL(ctx, si.CacheConfig.IsStoreInfoCacheTTL, si.CacheConfig.StoreInfoCacheDuration, si.Name)
	} else {
		stores, err = trans.StoreRepository.Get(ctx, si.Name)
	}
	if err != nil {
		trans.Rollback(ctx, err)
		return nil, err
	}
	ns := sop.NewStoreInfo(si)
	if len(stores) == 0 || stores[0].IsEmpty() {
		// Add to store repository if store not found.
		if ns.RootNodeID.IsNil() {
			// Pre-assign root node ID so B-Trees can merge newly created root nodes on commit.
			ns.RootNodeID = sop.NewUUID()
			ns.Timestamp = sop.Now().UnixMilli()
		}
		if err := trans.StoreRepository.Add(ctx, *ns); err != nil {
			// Cleanup the store if there was anything added in backend.
			trans.StoreRepository.Remove(ctx, ns.Name)
			trans.Rollback(ctx, err)
			return nil, err
		}
		return newBtree[TK, TV](ctx, ns, trans, comparer)
	}
	// Check if store retrieved is empty or of non-compatible specification.
	if !ns.IsCompatible(stores[0]) {
		trans.Rollback(ctx, nil)
		// Recommend to use the OpenBtree function to open it.
		return nil, fmt.Errorf("b-tree '%s' exists & has different configuration, please use OpenBtree to open & create an instance of it", si.Name)
	}
	ns = &stores[0]
	return newBtree[TK, TV](ctx, ns, trans, comparer)
}

func newBtree[TK btree.Ordered, TV any](ctx context.Context, s *sop.StoreInfo, trans *Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	// Fail if b-tree with a given name is already in the transaction's list.
	for i := range trans.btreesBackend {
		if s.Name == trans.btreesBackend[i].getStoreInfo().Name {
			err := fmt.Errorf("b-tree '%s' is already in the transaction's b-tree instances list", s.Name)
			trans.Rollback(ctx, err)
			return nil, err
		}
	}

	si := StoreInterface[TK, TV]{}

	// Assign the item action tracker frontend and backend bits.
	iat := newItemActionTracker[TK, TV](s, trans.l2Cache, trans.blobStore, trans.logger)
	si.ItemActionTracker = iat

	// Assign the node repository frontend and backend bits.
	nrw := newNodeRepository[TK, TV](trans, s)
	si.NodeRepository = nrw
	si.backendNodeRepository = nrw.nodeRepositoryBackend

	// Wire up the B-tree & the backend bits required by the transaction.
	b3, err := btree.New(s, &si.StoreInterface, comparer)
	if err != nil {
		trans.Rollback(ctx, err)
		return nil, err
	}

	// B-Tree backend processing(of commit & rollback) required objects.
	b3b := btreeBackend{
		// Node blob repository.
		nodeRepository: nrw.nodeRepositoryBackend,
		// Needed for auto-merging of Node contents.
		refetchAndMerge: refetchAndMergeClosure(&si, b3, trans.StoreRepository),
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
// This closure resets caches, reloads store info, and replays actions recorded by the item tracker,
// validating versions to prevent lost updates.
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
			if ok, err := b3.FindWithID(ctx, ci.item.Key, uuid); !ok || err != nil {
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
