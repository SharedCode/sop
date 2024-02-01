package btree

import (
	"fmt"

	"github.com/SharedCode/sop"
)

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
	RootNodeId sop.UUID
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
	// If true, node load will be balanced by pushing items to sibling nodes if there are vacant slots,
	// otherwise will not. This feature can be turned off if backend is impacted by the "balancing" act.
	LeafLoadBalancing bool

	// If true, each Add(..) method call will persist the item value's data to another partition, then on commit,
	// it will then be a very quick action as item(s) values' data were already saved.
	// This rquires 'IsValueDataInNodeSegment' field to be set to false to work.
	IsValueDataActivelyPersisted bool
}

// NewStoreInfo instantiates a new Store.
func NewStoreInfo(name string, slotLength int, isUnique bool, isValueDataInNodeSegment bool, leafLoadBalancing bool, desciption string) *StoreInfo {
	// Only even numbered slot lengths are allowed as we reduced scenarios to simplify logic.
	if slotLength%2 != 0 {
		slotLength--
	}
	// Minimum slot length is 2.
	if slotLength < 2 {
		slotLength = 2
	}

	// auto generate table names based off of store name.
	registryTableName := FormatRegistryTable(name)
	blobTableName := FormatBlobTable(name)

	const maxSlotLength = 10000

	// Maximum slot length is 10,000. It may be ridiculously huge blob if too big.
	// Even 10,000 may be too much, depending on key & value data size you'll store.
	if slotLength > maxSlotLength {
		slotLength = maxSlotLength
	}

	return &StoreInfo{
		Name:                     name,
		SlotLength:               slotLength,
		IsUnique:                 isUnique,
		IsValueDataInNodeSegment: isValueDataInNodeSegment,
		RegistryTable:            registryTableName,
		BlobTable:                blobTableName,
		Description:              desciption,
		LeafLoadBalancing:        leafLoadBalancing,
	}
}

func FormatBlobTable(name string) string {
	return fmt.Sprintf("%s_b", name)
}
func FormatRegistryTable(name string) string {
	return fmt.Sprintf("%s_r", name)
}

func ConvertToBlobTableName(registryTableName string) string {
	return FormatBlobTable(registryTableName[0 : len(registryTableName)-2])
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
		s.IsValueDataInNodeSegment == b.IsValueDataInNodeSegment &&
		s.LeafLoadBalancing == b.LeafLoadBalancing
}
