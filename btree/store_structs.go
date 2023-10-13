package btree

import "github.com/google/uuid"

// persistence constructs

type UUID uuid.UUID

type versionedItem struct{
	Version int
	IsDeleted bool
}

type Store struct {
    Name string
	NodeSlotCount int
	IsUnique bool
	KeyInfo string
	ValueInfo string
	// RootNodeID is the root node's handle.
	RootNodeID Handle
	Count int64
	versionedItem
}

func NewStore(name string, nodeSlotCount int, isUnique bool, itemSerializer ItemSerializer) Store{
	return Store{
		Name: name,
		NodeSlotCount: nodeSlotCount,
		IsUnique: isUnique,
		ItemSerializer: itemSerializer,
	}
}

type Item struct{
	Key interface{}
	Value interface{}	
	Version int
}
func (item Item) IsEmpty() bool{
	return item.Key == nil && item.Value == nil
}

type Node struct {
	ID Handle

    Slots []Item
	ChildrenAddresses []UUID
	// Count of Items stored in Slots array.
	Count int
	versionedItem
	parentAddress Handle
	indexOfNode int
}

func NewNode(slotCount int) *Node{
	return &Node{
		Slots: make([]Item, slotCount),
		indexOfNode:-1,
	}
}

type NodeBlocks struct {
	ID Handle

    SlotBlock []byte
	SlotBlockMap []UUID
	Children []UUID
	count int
	versionedItem
}

type SlotValue struct{
	ID Handle
	Value []byte
	IsDeleted bool
} 
type SlotValueBlocks struct{
	ID Handle
	Value []byte
	ValueBlockMap []UUID
	IsDeleted bool
}

type Recyclable struct{
	ObjectType int
	ObjectID UUID
	LockDate int64
	IsDeleted bool
}

// VirtualID is a structure that holds Logical ID and the underlying current Physical ID it maps to.
// It also has other members used for Transaction processing.
type VirtualID struct {
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

// TransactionEntryKeys contain info about each Store Item modified within a Transaction.
// NOTE: newly created Stores themselves don't get tracked within the Transaction Entry table.
// Their items do. New Stores are cached in-memory and get saved (conflict resolved) 
// during Transaction Commit.
type TransactionEntryKeys struct{
	ID Handle
	StoreName string
	Sequence UUID
}

type TransactionEntry struct{
	TransactionEntryKeys
	Action TransactionActionType
	CurrentItem Item
	NewItem Item
	versionedItem
}
