
package btree

// persistence constructs & interfaces

type UUID [16]byte

type Store struct {
    Name string
    RootNodeID UUID
	NodeSlotCount int
	IsUnique bool
	IsDeleted bool
}

func NewStore(name string, nodeSlotCount int, isUnique bool) *Store{
	return &Store{
		Name: name,
		NodeSlotCount: nodeSlotCount,
		IsUnique: isUnique,
	}
}

type Item struct{
	Key interface{}
	Version int
	Value interface{}	
}

type Node struct {
    ID UUID
    Slots []Item
	Children []UUID
	IsDeleted bool
	count int
}

type SlotValue struct{
    ID UUID
    Value []byte
	IsDeleted bool
} 

type Recyclable struct{
	ObjectType int
	ObjectID UUID
	IsDeleted bool
}

type VirtualID struct {
	LogicalID UUID
	IsPhysicalIDB bool
	PhysicalIDA UUID
	PhysicalIDB UUID
	IsDeleted bool
}

// interfaces

type StoreRepository interface{
	Get(name string) *Store
	Add(*Store) error
	Remove(name string) error
}

type NodeRepository interface{
	Get(nodeID UUID) *Node
	Add(*Node) error
	Update(*Node) error
	Remove(nodeID UUID) error
}

type VirtualIDRepository interface{
	Get(logicalID UUID) *VirtualID
	Add(*VirtualID) error
	Update(*VirtualID) error
	Remove(logicalID UUID) error
}

type Recycler interface{
	Get(batch int, objectType int) []*Recyclable
	Add([]*Recyclable) error
	//Update([]*Recyclable) error
	Remove([]*Recyclable) error
}
