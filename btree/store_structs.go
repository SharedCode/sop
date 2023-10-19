// persistence constructs
package btree

import "github.com/google/uuid"

type UUID uuid.UUID

type Store struct {
    Name string
	NodeSlotCount int
	IsUnique bool
	KeyInfo string
	ValueInfo string
	// RootNodeId is the root node's handle.
	RootNodeId Handle
	Count int64
	Version int
	IsDeleted bool
}

type StoreInterface[TKey Comparable, TValue any] struct{
	StoreRepository StoreRepository
	NodeRepository NodeRepository[TKey, TValue]
	VirtualIdRepository VirtualIdRepository
	RecyclerRepository RecyclerRepository
	TransactionRepository TransactionRepository
}

func NewStore(name string, nodeSlotCount int, isUnique bool) Store{
	return Store{
		Name: name,
		NodeSlotCount: nodeSlotCount,
		IsUnique: isUnique,
	}
}

type NodeBlocks struct {
	Id Handle
    SlotBlock []byte
	SlotBlockMap []UUID
	Children []UUID
	count int
	Version int
	IsDeleted bool
}

type SlotValue struct{
	Id Handle
	Value []byte
	IsDeleted bool
} 
type SlotValueBlocks struct{
	Id Handle
	Value []byte
	ValueBlockMap []UUID
	IsDeleted bool
}

type Recyclable struct{
	ObjectType int
	ObjectId UUID
	LockDate int64
	IsDeleted bool
}

// VirtualId is a structure that holds Logical Id and the underlying current Physical Id it maps to.
// It also has other members used for Transaction processing.
type VirtualId struct {
	Handle
	IsDeleted bool
}

type TransactionActionType uint
const(
	Get = iota
	Add
	Update
	Remove
)

// TransactionEntry contain info about each Store Item modified within a Transaction.
// NOTE: newly created Stores themselves don't get tracked within the Transaction Entry table.
// Their items do. New Stores are cached in-memory and get saved (conflict resolved) 
// during Transaction Commit.
type TransactionEntry struct{
	Id Handle
	Sequence UUID
	Action TransactionActionType
	IsDeleted bool
}
