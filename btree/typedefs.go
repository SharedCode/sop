
package btree

import "time"

// persistence constructs & interfaces

type UUID [16]byte

type baseItem struct{
	IsDeleted bool
}
type versionedItem struct{
	Version int
	baseItem
}

type Store struct {
    Name string
    RootNodeID UUID
	NodeSlotCount int
	Count int64
	IsUnique bool
	ItemSerializer *ItemSerializer
	versionedItem
}

func NewStoreDefaultSerializer(name string, nodeSlotCount int, isUnique bool) *Store{
	return NewStore(name, nodeSlotCount, isUnique, nil)
}

func NewStore(name string, nodeSlotCount int, isUnique bool, itemSerializer *ItemSerializer) *Store{
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
type ItemSerializer struct{
	SerializeKey func(k interface{}) ([]byte, error)
	DeSerializeKey func(kData []byte) (interface{}, error)
	CompareKey func(k1 interface{}, k2 interface{}) (int, error)
	SerializeValue func(v interface{}) ([]byte, error)
	DeSerializeValue func(vData []byte) (interface{}, error)
}

func (itemSer *ItemSerializer) IsEmpty() bool {
	return itemSer == nil ||
		(itemSer.SerializeKey == nil &&
		itemSer.DeSerializeKey == nil &&
		itemSer.CompareKey == nil &&
		itemSer.SerializeValue == nil &&
		itemSer.DeSerializeValue == nil)
}

type Node struct {
	ID UUID
    Slots []Item
	Children []UUID
	count int
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
