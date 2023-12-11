package in_cas_s3

import (
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_cas_s3/redis"
)

// StoreInterface contains different repositories needed/used by B-Tree to manage/access its data/objects.
type StoreInterface[TK btree.Comparable, TV any] struct {
	btree.StoreInterface[TK, TV]
	// itemRedisCache is a global lookup table for used for tracking, conflict detection & resolution
	// across different transactions in same and/or different machines.
	itemRedisCache redis.Cache
	// StoreRepository is used to manage/access stores.
	storeRepository StoreRepository
	// VirtualIdRegistry is used to manage/access all objects keyed off of their virtual Ids (UUIDs).
	virtualIdRegistry VirtualIdRegistry
	// RecyclerRepository is used to manage/access all deleted objects' "data blocks".
	recyclerRepository RecyclerRepository
}

// TODO: uncomment and reuse anything below as needed. (initial design artifacts)

// type TransactionRepository interface {
// 	Get(transactionId btree.UUID) ([]TransactionEntry, error)
// 	GetByStore(transactionId btree.UUID, storeName string) ([]TransactionEntry, error)
// 	Add([]TransactionEntry) error
// 	MarkDone([]TransactionEntry) error
// }

// // TransactionEntry contain info about each Store Item modified within a Transaction.
// // NOTE: newly created Stores themselves don't get tracked within the Transaction Entry table.
// // Their items do. New Stores are cached in-memory and get saved (conflict resolved)
// // during Transaction Commit.
// type TransactionEntry struct {
// 	EntityLogicalId btree.UUID
// 	EntityType      EntityType
// 	Sequence        int
// 	Action          TransactionActionType
// 	IsDeleted       bool
// }

// type TransactionActionType uint
// const (
// 	Get = iota
// 	Add
// 	Update
// 	Remove
// )

// type EntityType uint
// const (
// 	// BTreeNode is the entity type of the B-Tree Node.
// 	BTreeNode = iota
// 	// ValuePart is the entity type of the value part in the key/value pair
// 	// that a B-Tree supports persistence & access.
// 	ValuePart
// )

// type NodeDataBlocks struct {
// 	Id               Handle
// 	SlotDataBlock    []byte
// 	SlotDataBlockIds []btree.UUID
// 	Children         []btree.UUID
// 	count            int
// 	Version          int
// 	IsDeleted        bool
// }

// type SlotValueDataBlocks struct {
// 	Id                Handle
// 	Value             []byte
// 	ValueDataBlockIds []btree.UUID
// 	IsDeleted         bool
// }

// type SlotValue struct {
// 	Id        Handle
// 	Value     []byte
// 	IsDeleted bool
// }
