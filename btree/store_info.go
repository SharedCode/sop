package btree

// StoreInfo contains a given (B-Tree) store details.
type StoreInfo struct {
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
	// UpsertTime in milliseconds.
	UpsertTime int64
	// Is marked deleted or not.
	IsDeleted bool
	// IsValueDataInNodeSegment is true if "Value" data is stored in the B-Tree node's data segment.
	// Otherwise is false.
	IsValueDataInNodeSegment bool
}

// NewStoreInfo instantiates a new Store.
func NewStoreInfo(name string, slotLength int, isUnique bool, isValueDataInNodeSegment bool) *StoreInfo {
	// Only even numbered slot lengths are allowed as we reduced scenarios to simplify logic.
	if slotLength%2 != 0 {
		slotLength--
	}
	// Minimum slot length is 4, you lose gains if you use less than 4.
	if slotLength < 4 {
		slotLength = 4
	}
	return &StoreInfo{
		Name:                     name,
		SlotLength:               slotLength,
		IsUnique:                 isUnique,
		IsValueDataInNodeSegment: isValueDataInNodeSegment,
	}
}

// Returns true if this StoreInfo is empty, false otherwise.
// Empty StoreInfo signifies B-Tree does not exist yet.
func (s StoreInfo) IsEmpty() bool {
	var zero StoreInfo
	return s == zero
}
