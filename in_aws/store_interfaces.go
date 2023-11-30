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

// RecyclerRepository provides capability to recycle storage areas for storing data such as Node, etc...
// There are backends where this is not needed at all, e.g. Cassandra backend will not need this.
type RecyclerRepository interface {
	Get(itemCount int, objectType int) []Recyclable
	Add(recyclables []Recyclable) error
	Remove(items []Recyclable) error
}

// VirtualIdRepository interface specifies the "virtualized Id" repository, a.k.a. Id registry.
type VirtualIdRepository interface {
	Get(lid btree.UUID) (btree.Handle, error)
	Add(btree.Handle) error
	Update(btree.Handle) error
	Remove(lid btree.UUID) error
}

type TransactionRepository interface {
	Get(transactionId btree.UUID) ([]TransactionEntry, error)
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

// Transaction interface defines the "enduser facing" transaction methods.
type Transaction interface {
	Begin() error
	Commit() error
	Rollback() error
	HasBegun() bool
}

// TransactionEntry contain info about each Store Item modified within a Transaction.
// NOTE: newly created Stores themselves don't get tracked within the Transaction Entry table.
// Their items do. New Stores are cached in-memory and get saved (conflict resolved)
// during Transaction Commit.
type TransactionEntry struct {
	EntityLogicalId btree.UUID
	EntityType      EntityType
	Sequence        int
	Action          TransactionActionType
	IsDeleted       bool
}

type TransactionActionType uint

const (
	Get = iota
	Add
	Update
	Remove
)

type EntityType uint

const (
	// BTreeNode is the entity type of the B-Tree Node.
	BTreeNode = iota
	// ValuePart is the entity type of the value part in the key/value pair
	// that a B-Tree supports persistence & access.
	ValuePart
)

type NodeDataBlocks struct {
	Id               Handle
	SlotDataBlock    []byte
	SlotDataBlockIds []UUID
	Children         []UUID
	count            int
	Version          int
	IsDeleted        bool
}

type SlotValueDataBlocks struct {
	Id                Handle
	Value             []byte
	ValueDataBlockIds []UUID
	IsDeleted         bool
}

type Recyclable struct {
	ObjectType int
	ObjectId   UUID
	LockDate   int64
	IsDeleted  bool
}

type SlotValue struct {
	Id        Handle
	Value     []byte
	IsDeleted bool
}
