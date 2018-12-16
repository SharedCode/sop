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
	RootNodeID UUID
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

type Node struct {
	ID UUID
    Slots []Item
	Children []UUID
	Count int
	versionedItem
}

type NodeBlocks struct {
	ID UUID
    SlotBlock []byte
	SlotBlockMap []UUID
	Children []UUID
	count int
	versionedItem
}

type SlotValue struct{
    ID UUID
	Value []byte
	baseItem
} 
type SlotValueBlocks struct{
    ID UUID
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

type VirtualID struct {
	LogicalID UUID
	IsPhysicalIDB bool
	PhysicalIDA UUID
	PhysicalIDB UUID
	baseItem
}

type TransactionActionType uint
const(
	Get = iota
	Add
	Update
	Remove
)

type TransactionEntryKeys struct{
	ID UUID
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
