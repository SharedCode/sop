package in_red_ck

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_red_ck/redis"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

// Assign the configs & open connections to different sub-systems used by this package.
// Example, connection to Cassandra, Redis, etc...
func Initialize(cassandraConfig cas.Config, redisConfig redis.Options) error {
	if _, err := cas.GetConnection(cassandraConfig); err != nil {
		return err
	}
	if _, err := redis.GetConnection(redisConfig); err != nil {
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

// OpenBtree will open an existing B-Tree instance it for use in a transaction.
func OpenBtree[TK btree.Comparable, TV any](ctx context.Context, name string, t Transaction) (btree.BtreeInterface[TK, TV], error) {
	if t == nil {
		return nil, fmt.Errorf("Transaction 't' can't be nil.")
	}
	var t2 interface{} = t.GetPhasedTransaction()
	trans := t2.(*transaction)
	stores, err := trans.storeRepository.Get(ctx, name)
	if len(stores) == 0 || stores[0].IsEmpty() || err != nil {
		if err == nil {
			return nil, fmt.Errorf("B-Tree '%s' does not exist, please use NewBtree to create an instance of it.", name)
		}
		return nil, err
	}
	return newBtree[TK, TV](&stores[0], trans)
}

// NewBtree will create a new B-Tree instance with data persisted to backend storage upon commit, e.g. - AWS storage services.
// Parameters:
// name - specifies the name of the store/b-tree.
// slotLength - specifies the number of item slots per node of a b-tree.
// isUnique - specifies whether the b-tree will enforce key uniqueness(true) or not(false).
// isValueDataInNodeSegment - specifies whether the b-tree will store the "value" data in the tree's node segment together with
//	the key, or store it in another (data) segment. Currently not implemented and always stores the data in the node segment.
// leafLoadBalancing - true means leaf load balancing feature is enabled, false otherwise.
// description - (optional) description about the store.
// t - transaction that the instance will participate in.
func NewBtree[TK btree.Comparable, TV any](ctx context.Context, name string, slotLength int, isUnique bool,
	isValueDataInNodeSegment bool, leafLoadBalancing bool, desciption string, t Transaction) (btree.BtreeInterface[TK, TV], error) {
	if t == nil {
		return nil, fmt.Errorf("Transaction 't' can't be nil.")
	}

	var t2 interface{} = t.GetPhasedTransaction()
	trans := t2.(*transaction)

	stores, err := trans.storeRepository.Get(ctx, name)
	if err != nil {
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
			return nil, err
		}
		stores = []btree.StoreInfo{*ns}
	}
	// Check if store retrieved is empty or of non-compatible specification.
	if !ns.IsCompatible(stores[0]) {
		// Recommend to use the OpenBtree function to open it.
		return nil, fmt.Errorf("B-Tree '%s' exists, please use OpenBtree to open & create an instance of it.", name)
	}
	return newBtree[TK, TV](ns, trans)
}

func newBtree[TK btree.Comparable, TV any](s *btree.StoreInfo, trans *transaction) (btree.BtreeInterface[TK, TV], error) {
	si := StoreInterface[interface{}, interface{}]{}

	// Assign the item action tracker frontend and backend bits.
	iatw := newItemActionTracker()
	si.ItemActionTracker = iatw
	si.backendItemActionTracker = iatw

	// Assign the node repository frontend and backend bits.
	nrw := newNodeRepository[interface{}, interface{}](trans, s)
	si.NodeRepository = nrw
	si.backendNodeRepository = nrw.realNodeRepository

	// Wire up the B-tree & add its backend interface to the transaction.
	b3, _ := btree.New[interface{}, interface{}](s, &si.StoreInterface)
	trans.btreesBackend = append(trans.btreesBackend, si)
	trans.btrees = append(trans.btrees, b3)

	return newBtreeWithTransaction[TK, TV](trans, b3), nil
}
