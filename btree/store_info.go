package btree

import "fmt"

// StoreInfo contains a given (B-Tree) store details.
type StoreInfo struct {
	// Short name of this (B-Tree store).
	Name string
	// Count of items that can be stored on a given node.
	SlotLength int
	// IsUnique tells whether key/value pair (items) of this tree should be unique on key.
	IsUnique bool
	// (optional) Description of the Store.
	Description string
	// Virtual Id registry table name.
	RegistryTable string
	// Blob table name.
	BlobTable string
	// RootNodeId is the root node's Id.
	RootNodeId UUID
	// Total count of items stored.
	Count int64
	// Used internally by SOP. Should be ignored when persisted in the backend.
	CountDelta int64 `json:"-"`
	// Timestamp in milliseconds.
	Timestamp int64
	// Is marked deleted or not.
	IsDeleted bool
	// IsValueDataInNodeSegment is true if "Value" data is stored in the B-Tree node's data segment.
	// Otherwise is false.
	IsValueDataInNodeSegment bool
}

// NewStoreInfo instantiates a new Store.
func NewStoreInfo(name string, slotLength int, isUnique bool, isValueDataInNodeSegment bool,
	registryTableName string, blobPath string, desciption string) *StoreInfo {
	// Only even numbered slot lengths are allowed as we reduced scenarios to simplify logic.
	if slotLength%2 != 0 {
		slotLength--
	}
	// Minimum slot length is 4, you lose gains if you use less than 4.
	if slotLength < 4 {
		slotLength = 4
	}
	if registryTableName == "" {
		// If registry table name was not specified, use default name, e.g. "hello_vr" where "hello" is the store name.
		registryTableName = fmt.Sprintf("%s_vr", name)
	}
	// Maximum slot length is 1,000. It may be ridiculously huge blob if too big.
	// Even 1,000 may be too much, depending on key & value data size you'll store.
	if slotLength > 1000 {
		slotLength = 1000
	}
	return &StoreInfo{
		Name:                     name,
		SlotLength:               slotLength,
		IsUnique:                 isUnique,
		IsValueDataInNodeSegment: isValueDataInNodeSegment,
		RegistryTable:            registryTableName,
		BlobTable:                blobPath,
		Description:              desciption,
	}
}

// Returns true if this StoreInfo is empty, false otherwise.
// Empty StoreInfo signifies B-Tree does not exist yet.
func (s StoreInfo) IsEmpty() bool {
	var zero StoreInfo
	return s == zero
}

// Returns true if another store is compatible with this one spec wise.
func (s StoreInfo) IsCompatible(b StoreInfo) bool {
	return s.SlotLength == b.SlotLength &&
		s.IsUnique == b.IsUnique &&
		s.BlobTable == b.BlobTable &&
		s.RegistryTable == b.RegistryTable &&
		s.IsValueDataInNodeSegment == b.IsValueDataInNodeSegment
}
