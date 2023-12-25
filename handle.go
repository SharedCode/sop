package sop

import (
	"github.com/SharedCode/sop/btree"
)

// Handle is a structure that holds Logical Id and the underlying Physical Id it maps to.
// It is used by SOP to provide ability to support ACID transactions and swiftly replace
// Node(s) of the trie, with minimal to zero disruption.
type Handle struct {
	// LogicalId is the "functional" Id of the entity.
	LogicalId   btree.UUID
	// PhysicalIdA is one of the two physical Ids supported.
	PhysicalIdA btree.UUID
	// PhysicalIdB is the "other" physical Id supported.
	PhysicalIdB btree.UUID
	// true if active Id is physicalIdB, otherwise false.
	IsActiveIdB bool
	// Upsert time in milliseconds, is also used for conflict resolution among (in-flight) transactions.
	UpsertTime int64
	// IsDeleted is used for "logical" deletes, useful for implementation on backends such as Cassandra, where
	// physical record deletes are expensive. If true then Handle is treated like it is deleted.
	IsDeleted bool
}

// GetActiveId returns the currently active (if there is) UUID of a given Handle.
func (h Handle) GetActiveId() btree.UUID {
	if h.PhysicalIdA.IsNil() && h.PhysicalIdB.IsNil() {
		return h.LogicalId
	}
	if h.IsActiveIdB {
		return h.PhysicalIdB
	}
	return h.PhysicalIdA
}

// NewHandle creates a new Handle.
func NewHandle(id btree.UUID) Handle {
	return Handle{
		LogicalId:   id,
	}
}
