package dynamic

import (
	"time"

	"github.com/sharedcode/sop"
)

// Now returns the current time. It is a var to allow tests to override time.Now for determinism.
var Now = time.Now

// Handle holds a logical ID and its two physical IDs (A and B) used to implement ACID-safe swaps.
// SOP uses Handle to quickly switch between node versions and to support logical deletes.
type Handle struct {
	// LogicalID is the stable identifier of the entity.
	LogicalID sop.UUID
	// PhysicalIDA is one of the two physical IDs supported.
	PhysicalIDA sop.UUID
	// PhysicalIDB is the second physical ID supported.
	PhysicalIDB sop.UUID
	// IsActiveIDB indicates whether PhysicalIDB is currently the active ID.
	IsActiveIDB bool
}

// NewHandle creates a new Handle with the provided logical ID. PhysicalIDA is initialized to the same value.
func NewHandle(id sop.UUID) Handle {
	return Handle{
		LogicalID:   id,
		PhysicalIDA: id,
	}
}

// GetActiveID returns the currently active UUID (either PhysicalIDA or PhysicalIDB).
func (h Handle) GetActiveID() sop.UUID {
	if h.IsActiveIDB {
		return h.PhysicalIDB
	}
	return h.PhysicalIDA
}

// GetInActiveID returns the currently inactive physical UUID.
func (h Handle) GetInActiveID() sop.UUID {
	if h.IsActiveIDB {
		return h.PhysicalIDA
	}
	return h.PhysicalIDB
}

// IsAandBinUse reports whether both physical IDs A and B are populated.
func (h Handle) IsAandBinUse() bool {
	return !h.PhysicalIDA.IsNil() && !h.PhysicalIDB.IsNil()
}

// HasID reports whether the provided UUID matches either physical ID A or B.
func (h *Handle) HasID(id sop.UUID) bool {
	return h.PhysicalIDA == id || h.PhysicalIDB == id
}

// FlipActiveID switches the active physical ID from A to B or B to A.
func (h *Handle) FlipActiveID() {
	h.IsActiveIDB = !h.IsActiveIDB
}

// ClearInactiveID resets the inactive physical ID to NilUUID and clears the WIP timestamp.
func (h *Handle) ClearInactiveID() {
	if h.IsActiveIDB {
		h.PhysicalIDA = sop.NilUUID
	} else {
		h.PhysicalIDB = sop.NilUUID
	}
}

// IsEmpty reports whether all Handle fields are zero values (no IDs, not deleted, zero version and timestamps).
func (x *Handle) IsEmpty() bool {
	return x.LogicalID == sop.NilUUID &&
		x.PhysicalIDA == sop.NilUUID &&
		x.PhysicalIDB == sop.NilUUID
}

// IsEqual reports whether two Handle instances are equal ignoring the Version field.
func (x *Handle) IsEqual(y *Handle) bool {
	return x.LogicalID == y.LogicalID &&
		x.PhysicalIDA == y.PhysicalIDA &&
		x.PhysicalIDB == y.PhysicalIDB
}
