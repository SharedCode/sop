package in_aws

import "github.com/SharedCode/sop/btree"

// StoreInterface contains different repositories needed/used by B-Tree to manage/access its data/objects.
type StoreInterface[TK btree.Comparable, TV any] struct {
	btree.StoreInterface[TK, TV]
	// StoreRepository is used to manage/access stores.
	StoreRepository StoreRepository
	// VirtualIdRepository is used to manage/access all objects keyed off of their virtual Ids (UUIDs).
	VirtualIdRepository VirtualIdRepository
	// RecyclerRepository is used to manage/access all deleted objects' "data blocks".
	RecyclerRepository RecyclerRepository
}

// in-memory transaction manager just relays CRUD actions to the actual in-memory NodeRepository.
type transaction_manager[TK btree.Comparable, TV any] struct {
	storeInterface *btree.StoreInterface[TK, TV]
}

// RecyclerRepository provides capability to recycle storage areas for storing data such as Node, etc...
// There are backends where this is not needed at all, e.g. Cassandra backend will not need this.
type RecyclerRepository interface {
	Get(itemCount int, objectType int) []Recyclable
	Add(recyclables []Recyclable) error
	Remove(items []Recyclable) error
}

// VirtualIdRepository interface specifies the "virtualized Id" repository, a.k.a. Id registry.
type VirtualIdRepository interface {
	Get(lid UUID) (Handle, error)
	Add(Handle) error
	Update(Handle) error
	Remove(lid UUID) error
}

type TransactionRepository interface {
	Get(transactionId UUID) ([]TransactionEntry, error)
	GetByStore(transactionId UUID, storeName string) ([]TransactionEntry, error)
	Add([]TransactionEntry) error
	MarkDone([]TransactionEntry) error
}

// StoreRepository interface specifies the store repository.
type StoreRepository interface {
	Get(name string) (Store, error)
	Add(Store) error
	Remove(name string) error
}

// Transaction Manager surfaces a StoreInterface that which, knows how to reconcile/merge
// changes in a transaction session with the actual destination storage.
// Idea here is, to let the algorithms be abstracted with the backend store. To the b-tree
// it feels like it only uses normal repositories but in actuality, these repositories
// are facade for the transaction manager, so it can reconcile/merge with the backend
// during transaction commit. A very simple model, but powerful/complete control on
// the data changes & necessary merging with backend storage.
func newTransactionManager[TK btree.Comparable, TV any]() *transaction_manager[TK, TV] {
	si := btree.StoreInterface[TK, TV]{
		NodeRepository:      newNodeRepository[TK, TV](),
		RecyclerRepository:  newRecycler(), // shared globally.
		VirtualIdRepository: newVirtualIdRepository(),
		StoreRepository:     newStoreRepository(), // shared globally.
	}
	return &transaction_manager[TK, TV]{
		storeInterface: &si,
	}
}

// NewBtree will create an in-memory B-Tree & its required data stores. You can use it to store
// and access key/value pairs similar to a map but which, sorts items & allows "range queries".
// This will return btree instance that has no wrapper, thus, methods have error in return where appropriate.
// Handy for using in-memory b-tree for writing unit tests to mock the "Enterprise" V2 version.
func NewBtreeWithNoWrapper[TK btree.Comparable, TV any](isUnique bool) btree.BtreeInterface[TK, TV] {
	transactionManager := newTransactionManager[TK, TV]()
	s := btree.NewStore("", itemsPerNode, isUnique, true)
	transactionManager.storeInterface.StoreRepository.Add(s)
	return btree.NewBtree[TK, TV](s, transactionManager.storeInterface)
}
