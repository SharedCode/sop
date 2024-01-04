package in_cas_s3

import (
	"github.com/SharedCode/sop/btree"
)

// StoreInterface contains different repositories needed/used by B-Tree to manage/access its data/objects.
type StoreInterface[TK btree.Comparable, TV any] struct {
	btree.StoreInterface[TK, TV]

	// Non-generics item action tracker, used in transaction commit to process modified Items.
	backendItemActionTracker *itemActionTracker
	// Non-generics node repository, used in transaction commit to process modified Nodes.
	backendNodeRepository *nodeRepository
}

// TODO: uncomment and reuse anything below as needed. (initial design artifacts)

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
