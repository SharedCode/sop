package in_red_ck

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop/btree"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

// Assign the configs & open connections to different sub-systems used by this package.
// Example, connection to Cassandra, Redis.
func Initialize(cassandraConfig cas.Config, redisConfig redis.Options) error {
	if _, err := cas.OpenConnection(cassandraConfig); err != nil {
		return err
	}
	if _, err := redis.OpenConnection(redisConfig); err != nil {
		return err
	}
	return nil
}

// Returns true if components required were initialized, false otherwise.
func IsInitialized() bool {
	return cas.IsConnectionInstantiated() && redis.IsConnectionInstantiated()
}

// Shutdown or closes all connections used in this package.
func Shutdown() {
	cas.CloseConnection()
	redis.CloseConnection()
}

// Removes B-Tree from the backend storage. This involves dropping tables
// that are permanent action and thus, 'can't get rolled back.
//
// So, be careful calling this API as you will lose your data.
func RemoveBtree(ctx context.Context, name string, t Transaction) error {
	// TODO: add tests to exercise this and to illustrate usage.
	var t2 interface{} = t.GetPhasedTransaction()
	trans := t2.(*transaction)
	return trans.storeRepository.Remove(ctx, name)
}

// OpenBtree will open an existing B-Tree instance & prepare it for use in a transaction.
func OpenBtree[TK btree.Comparable, TV any](ctx context.Context, name string, t Transaction) (btree.BtreeInterface[TK, TV], error) {
	if t == nil {
		return nil, fmt.Errorf("Transaction 't' parameter can't be nil")
	}
	if !t.HasBegun() {
		return nil, fmt.Errorf("Transaction 't' parameter has not started")
	}

	var t2 interface{} = t.GetPhasedTransaction()
	trans := t2.(*transaction)
	stores, err := trans.storeRepository.Get(ctx, name)
	if len(stores) == 0 || stores[0].IsEmpty() || err != nil {
		if err == nil {
			trans.Rollback(ctx)
			return nil, fmt.Errorf("B-Tree '%s' does not exist, please use NewBtree to create an instance of it", name)
		}
		trans.Rollback(ctx)
		return nil, err
	}
	return newBtree[TK, TV](ctx, &stores[0], trans)
}

// NewBtree will create a new B-Tree instance with data persisted to backend storage upon commit.
// If B-Tree(name) is not found in the backend, a new one will be created. Otherwise, the existing one will be opened
// and the parameters checked if matching. If you know that it exists, then it is more convenient and more readable to call
// the OpenBtree function.
//
// Parameters:
// name - specifies the name of the store/b-tree.
// slotLength - specifies the number of item slots per node of a b-tree.
// isUnique - specifies whether the b-tree will enforce key uniqueness(true) or not(false).
// isValueDataInNodeSegment - specifies whether the b-tree will store the "value" data in the tree's node segment together with
//
//	the key, or store it in another (data) segment. Currently not implemented and always stores the data in the node segment.
//
// leafLoadBalancing - true means leaf load balancing feature is enabled, false otherwise.
// description - (optional) description about the store.
// t - transaction that the instance will participate in.
func NewBtree[TK btree.Comparable, TV any](ctx context.Context, name string, slotLength int, isUnique bool,
	isValueDataInNodeSegment bool, leafLoadBalancing bool, desciption string, t Transaction) (btree.BtreeInterface[TK, TV], error) {
	if t == nil {
		return nil, fmt.Errorf("Transaction 't' parameter can't be nil")
	}
	if !t.HasBegun() {
		return nil, fmt.Errorf("Transaction 't' parameter has not started")
	}

	var t2 interface{} = t.GetPhasedTransaction()
	trans := t2.(*transaction)

	stores, err := trans.storeRepository.Get(ctx, name)
	if err != nil {
		trans.Rollback(ctx)
		return nil, err
	}
	ns := btree.NewStoreInfo(name, slotLength, isUnique, true, leafLoadBalancing, desciption)
	if len(stores) == 0 || stores[0].IsEmpty() {
		// Add to store repository if store not found.
		if ns.RootNodeId.IsNil() {
			// Pre-assign root node Id so B-Trees can merge newly created root nodes on commit.
			ns.RootNodeId = btree.NewUUID()
			ns.Timestamp = Now()
		}
		if err := trans.storeRepository.Add(ctx, *ns); err != nil {
			trans.Rollback(ctx)
			return nil, err
		}
		return newBtree[TK, TV](ctx, ns, trans)
	}
	// Check if store retrieved is empty or of non-compatible specification.
	if !ns.IsCompatible(stores[0]) {
		trans.Rollback(ctx)
		// Recommend to use the OpenBtree function to open it.
		return nil, fmt.Errorf("B-Tree '%s' exists, please use OpenBtree to open & create an instance of it", name)
	}
	ns = &stores[0]
	return newBtree[TK, TV](ctx, ns, trans)
}

func newBtree[TK btree.Comparable, TV any](ctx context.Context, s *btree.StoreInfo, trans *transaction) (btree.BtreeInterface[TK, TV], error) {
	si := StoreInterface[TK, TV]{}

	// Assign the item action tracker frontend and backend bits.
	iat := newItemActionTracker[TK, TV]()
	si.ItemActionTracker = iat

	// Assign the node repository frontend and backend bits.
	nrw := newNodeRepository[TK, TV](trans, s)
	si.NodeRepository = nrw
	si.backendNodeRepository = nrw.realNodeRepository

	// Wire up the B-tree & the backend bits required by the transaction.
	b3, err := btree.New[TK, TV](s, &si.StoreInterface)
	if err != nil {
		trans.Rollback(ctx)
		return nil, err
	}
	b3b := btreeBackend{
		// Node blob repository.
		nodeRepository: nrw.realNodeRepository,
		// Needed for auto-merging of Node contents.
		refetchAndMerge: refetchAndMergeClosure[TK, TV](&si, b3, trans.storeRepository),
		// Needed when applying the "delta" to the Store Count field.
		getStoreInfo: func() *btree.StoreInfo { return b3.StoreInfo },

		// Needed for tracked items' lock management.
		hasTrackedItems:    iat.hasTrackedItems,
		lockTrackedItems:   iat.lock,
		unlockTrackedItems: iat.unlock,
	}
	trans.btreesBackend = append(trans.btreesBackend, b3b)

	return newBtreeWithTransaction[TK, TV](trans, b3), nil
}

// Use tracked Items to refetch their Nodes(using B-Tree) and merge the changes in, if there is no conflict.
func refetchAndMergeClosure[TK btree.Comparable, TV any](si *StoreInterface[TK, TV], b3 *btree.Btree[TK, TV], sr cas.StoreRepository) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		b3ModifiedItems := si.ItemActionTracker.(*itemActionTracker[TK, TV]).items
		// Clear the backend "cache" so we can force B-Tree to re-fetch from Redis(or BlobStore).
		si.ItemActionTracker.(*itemActionTracker[TK, TV]).items = make(map[btree.UUID]cacheItem[TK, TV])
		si.backendNodeRepository.nodeLocalCache = make(map[btree.UUID]cacheNode)
		// Reset StoreInfo of B-Tree in prep to replay the "actions".
		storeInfo, err := sr.Get(ctx, b3.StoreInfo.Name)
		if err != nil {
			return err
		}
		b3.StoreInfo.Count = storeInfo[0].Count
		b3.StoreInfo.RootNodeId = storeInfo[0].RootNodeId

		for itemId, ci := range b3ModifiedItems {
			if ci.Action == addAction {
				if ok, err := b3.Add(ctx, ci.item.Key, *ci.item.Value); !ok || err != nil {
					if err != nil {
						return err
					}
					return fmt.Errorf("refetchAndMergeModifications failed to merge add item with key %v", ci.item.Key)
				}
				continue
			}
			if ok, err := b3.FindOneWithId(ctx, ci.item.Key, itemId); !ok || err != nil {
				if err != nil {
					return err
				}
				return fmt.Errorf("refetchAndMergeModifications failed to find item with key %v", ci.item.Key)
			}

			// Check if the item read from backend has been updated since the time we read it.
			if item, err := b3.GetCurrentItem(ctx); err != nil || item.Version != ci.versionInDB {
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
