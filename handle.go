package sop

import (
	"time"
)

// Handle is a structure that holds Logical ID and the underlying Physical ID it maps to.
// It is used by SOP to provide ability to support ACID transactions and swiftly replace
// Node(s) of the trie.
type Handle struct {
	// LogicalID is the "functional" ID of the entity.
	LogicalID UUID
	// PhysicalIDA is one of the two physical IDs supported.
	PhysicalIDA UUID
	// PhysicalIDB is the "other" physical ID supported.
	PhysicalIDB UUID
	// true if active ID is physicalIDB, otherwise false.
	IsActiveIDB bool
	// Current state(active ID, final deleted state) version.
	Version int
	// Work in progress(inactive ID, non final deleted state) timestamp in milliseconds.
	WorkInProgressTimestamp int64
	// IsDeleted is used for "logical" deletes.
	IsDeleted bool
}

// NewHandle creates a new Handle given a logical ID.
func NewHandle(id UUID) Handle {
	return Handle{
		LogicalID:   id,
		PhysicalIDA: id,
	}
}

// GetActiveID returns the currently active (if there is) UUID of a given Handle.
func (h Handle) GetActiveID() UUID {
	if h.IsActiveIDB {
		return h.PhysicalIDB
	}
	return h.PhysicalIDA
}

// Returns the inactive phys. ID.
func (h Handle) GetInActiveID() UUID {
	if h.IsActiveIDB {
		return h.PhysicalIDA
	}
	return h.PhysicalIDB
}

// Returns true if physical A and B are both in use across transactions, false otherwise.
func (h Handle) IsAandBinUse() bool {
	return !h.PhysicalIDA.IsNil() && !h.PhysicalIDB.IsNil()
}

// AllocateID will create a new UUID and auto-assign it to the available phys. A or B slot.
// Will return nil UUID if there is no slot left.
func (h *Handle) AllocateID() UUID {
	if h.IsAandBinUse() {
		return NilUUID
	}
	id := NewUUID()
	h.WorkInProgressTimestamp = time.Now().UnixMilli()
	if h.IsActiveIDB {
		h.PhysicalIDA = id
		return id
	}
	h.PhysicalIDB = id
	return id
}

// Returns true if inactive ID is expired, false otherwise.
func (h *Handle) IsExpiredInactive() bool {
	// TODO: Do we want maxDuration to be configurable? For now, hardcode to 2 hours.
	// Transactions are encouraged to be around 15 mins max, thus, 2 hr expiration of failed
	// node update ID(inactive ID) seems good.
	const maxDuration = 2
	expiryTime := time.Now().Add(time.Duration(-maxDuration) * time.Hour).UnixMilli()
	return h.WorkInProgressTimestamp > 0 && h.WorkInProgressTimestamp < expiryTime
}

// Returns true if id is either physical ID A or B, false otherwise.
func (h *Handle) HasID(id UUID) bool {
	return h.PhysicalIDA == id || h.PhysicalIDB == id
}

// Make inactive physical ID as active.
func (h *Handle) FlipActiveID() {
	h.IsActiveIDB = !h.IsActiveIDB
}

// Reset to nil the inactive phys. ID.
func (h *Handle) ClearInactiveID() {
	if h.IsActiveIDB {
		h.PhysicalIDA = NilUUID
	} else {
		h.PhysicalIDB = NilUUID
	}
	h.WorkInProgressTimestamp = 0
}

// Checks if this Handle instance has the same attributes' values as another Handle, except version #.
func (x *Handle) IsEqual(y *Handle) bool {
	return x.LogicalID == y.LogicalID &&
		x.IsDeleted == y.IsDeleted &&
		x.PhysicalIDA == y.PhysicalIDA &&
		x.PhysicalIDB == y.PhysicalIDB
}
