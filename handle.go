package sop

import (
	"time"

	"github.com/SharedCode/sop/btree"
)

// Handle is a structure that holds Logical Id and the underlying Physical Id it maps to.
// It is used by SOP to provide ability to support ACID transactions and swiftly replace
// Node(s) of the trie.
type Handle struct {
	// LogicalId is the "functional" Id of the entity.
	LogicalId   btree.UUID
	// PhysicalIdA is one of the two physical Ids supported.
	PhysicalIdA btree.UUID
	// PhysicalIdB is the "other" physical Id supported.
	PhysicalIdB btree.UUID
	// true if active Id is physicalIdB, otherwise false.
	IsActiveIdB bool
	// Upsert time of the active Id, in milliseconds.
	UpsertTime int64
	// Upsert time of the inactive Id, in milliseconds.
	InactiveUpsertTime int64
	// IsDeleted is used for "logical" deletes.
	IsDeleted bool
}

// NewHandle creates a new Handle given a logical Id.
func NewHandle(id btree.UUID) Handle {
	return Handle{
		LogicalId:   id,
		PhysicalIdA: id,
	}
}

// GetActiveId returns the currently active (if there is) UUID of a given Handle.
func (h Handle) GetActiveId() btree.UUID {
	if h.IsActiveIdB {
		return h.PhysicalIdB
	}
	return h.PhysicalIdA
}
// Returns the inactive phys. Id.
func (h Handle) GetInActiveId() btree.UUID {
	if h.IsActiveIdB {
		return h.PhysicalIdA
	}
	return h.PhysicalIdB
}

// Returns true if physical A and B are both in use across transactions, false otherwise.
func (h Handle) IsAandBinUse() bool {
	return !h.PhysicalIdA.IsNil() && !h.PhysicalIdB.IsNil()
}

// AllocateId will create a new UUID and auto-assign it to the available phys. A or B slot.
// Will return nil UUID if there is no slot left.
func (h *Handle) AllocateId() btree.UUID {
	if h.IsAandBinUse() {
		return btree.NilUUID
	}
	id := btree.NewUUID()
	h.InactiveUpsertTime = time.Now().UnixMilli()
	if h.IsActiveIdB {
		h.PhysicalIdA = id
		return id
	}
	h.PhysicalIdB = id
	return id
}

// Returns true if inactive Id is expired, false otherwise.
func (h *Handle) IsExpiredInactive() bool {
	const maxDuration = 2
	return h.InactiveUpsertTime > 0 &&
		(time.Now().UnixMilli() - h.InactiveUpsertTime) > int64(time.Duration(maxDuration) * time.Hour)
}

// Returns true if id is either physical Id A or B, false otherwise.
func (h *Handle) HasId(id btree.UUID) bool {
	return h.PhysicalIdA == id || h.PhysicalIdB == id
}

// Make inactive physical Id as active.
func (h *Handle) FlipActiveId() {
	if h.IsActiveIdB {
		h.PhysicalIdB = btree.NilUUID
	} else {
		h.PhysicalIdA = btree.NilUUID
	}
	h.IsActiveIdB = !h.IsActiveIdB
}

// Reset to nil the inactive phys. Id.
func (h *Handle) ClearInactiveId() {
	if h.IsActiveIdB {
		h.PhysicalIdA = btree.NilUUID
	} else {
		h.PhysicalIdB = btree.NilUUID
	}
	h.InactiveUpsertTime = 0
}
