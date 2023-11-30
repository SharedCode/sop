package btree

// Store contains a given (B-Tree) store details.
type Store struct {
	// Name of this (B-Tree store).
	Name string
	// Count of items that can be stored on a given node.
	SlotLength int
	// IsUnique tells whether key/value pair (items) of this tree should be unique on key.
	IsUnique  bool
	KeyInfo   string
	ValueInfo string
	// RootNodeId is the root node's Id.
	RootNodeId UUID
	// Total count of items stored.
	Count int64
	// Version number.
	Version int
	// Is marked deleted or not.
	IsDeleted bool
	// IsValueDataInNodeSegment is true if "Value" data is stored in the B-Tree node's data segment.
	// Otherwise is false.
	IsValueDataInNodeSegment bool
}

// StoreInterface contains different repositories needed/used by B-Tree to manage/access its data/objects.
type StoreInterface[TK Comparable, TV any] struct {
	// NodeRepository is used to manage/access B-Tree nodes.
	NodeRepository NodeRepository[TK, TV]
}

// NewStore instantiates a new Store.
func NewStore(name string, slotLength int, isUnique bool, isValueDataInNodeSegment bool) Store {
	// Only even numbered slot lengths are allowed as we reduced scenarios to simplify logic.
	if slotLength%2 != 0 {
		slotLength--
	}
	// Minimum slot length is 4, you lose gains if you use less than 4.
	if slotLength < 4 {
		slotLength = 4
	}
	return Store{
		Name:                     name,
		SlotLength:               slotLength,
		IsUnique:                 isUnique,
		IsValueDataInNodeSegment: isValueDataInNodeSegment,
	}
}
