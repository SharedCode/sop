package btree

import "time"

// persistence constructs

type UUID [16]byte

type KVType uint
const (
	// Key is string, Value is string data types. Supported first.
	KeyStringValueString = iota
	// Key is string, Value is binary. Supported next.
	KeyStringValueBinary

	// Key is string, Value is custom (serialized)
	KeyStringValueCustom
	// Key is custom, Value is string
	KeyCustomValueString
	// Key is custom, Value is custom
	KeyCustomValueCustom
)

type baseItem struct{
	IsDeleted bool
}
type versionedItem struct{
	Version int
	baseItem
}

type Store struct {
    Name string
	NodeSlotCount int
	IsUnique bool
	KVType KVType
	KeyInfo string
	ValueInfo string
	IsCustomKeyStoredAsString bool
	IsCustomValueStoredAsString bool
	ItemSerializer ItemSerializer
	// RootNodeID is the root node's handle.
	RootNodeID *Handle
	Count int64
	versionedItem
}

func NewStoreDefaultSerializer(name string, nodeSlotCount int, isUnique bool) *Store{
	var itemSer ItemSerializer
	return NewStore(name, nodeSlotCount, isUnique, itemSer)
}

func NewStore(name string, nodeSlotCount int, isUnique bool, itemSerializer ItemSerializer) *Store{
	return &Store{
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
func (item *Item) IsEmpty() bool{
	return item.Key == nil && item.Value == nil
}

type Node struct {
	ID *Handle

    Slots []Item
	Children []UUID
	Count int
	versionedItem
}
func NewNode(slotCount int) *Node{
	return &Node{
		Slots: make([]Item, slotCount),
	}
}

type NodeBlocks struct {
	ID *Handle

    SlotBlock []byte
	SlotBlockMap []UUID
	Children []UUID
	count int
	versionedItem
}

type SlotValue struct{
	ID *Handle
	Value []byte
	baseItem
} 
type SlotValueBlocks struct{
	ID *Handle
	Value []byte
	ValueBlockMap []UUID
	baseItem
}

type Recyclable struct{
	ObjectType int
	ObjectID UUID
	LockDate time.Time
	baseItem
}

// VirtualID is a structure that holds Logical ID and the underlying current Physical ID it maps to.
// It also has other members used for Transaction processing.
type VirtualID struct {
	Handle
	baseItem
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
	ID *Handle
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
