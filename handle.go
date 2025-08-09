package sop

import (
	"time"
)

// Now returns the current time. It is a var to allow tests to override time.Now for determinism.
var Now = time.Now

// Handle holds a logical ID and its two physical IDs (A and B) used to implement ACID-safe swaps.
// SOP uses Handle to quickly switch between node versions and to support logical deletes.
type Handle struct {
	// LogicalID is the stable identifier of the entity.
	LogicalID UUID
	// PhysicalIDA is one of the two physical IDs supported.
	PhysicalIDA UUID
	// PhysicalIDB is the second physical ID supported.
	PhysicalIDB UUID
	// IsActiveIDB indicates whether PhysicalIDB is currently the active ID.
	IsActiveIDB bool
	// Version is the current state version (active ID, final deleted state).
	Version int32
	// WorkInProgressTimestamp stores the millisecond timestamp of the inactive ID (or non-final deleted state).
	WorkInProgressTimestamp int64
	// IsDeleted marks a logical delete.
	IsDeleted bool
}

const (
	// HandleSizeInBytes is the size, in bytes, of a Handle structure when encoded.
	HandleSizeInBytes = 62
)

// NewHandle creates a new Handle with the provided logical ID. PhysicalIDA is initialized to the same value.
func NewHandle(id UUID) Handle {
	return Handle{
		LogicalID:   id,
		PhysicalIDA: id,
	}
}

// GetActiveID returns the currently active UUID (either PhysicalIDA or PhysicalIDB).
func (h Handle) GetActiveID() UUID {
	if h.IsActiveIDB {
		return h.PhysicalIDB
	}
	return h.PhysicalIDA
}

// GetInActiveID returns the currently inactive physical UUID.
func (h Handle) GetInActiveID() UUID {
	if h.IsActiveIDB {
		return h.PhysicalIDA
	}
	return h.PhysicalIDB
}

// IsAandBinUse reports whether both physical IDs A and B are populated.
func (h Handle) IsAandBinUse() bool {
	return !h.PhysicalIDA.IsNil() && !h.PhysicalIDB.IsNil()
}

// AllocateID generates a new UUID and assigns it to the available physical slot.
// If both A and B are already in use, NilUUID is returned.
func (h *Handle) AllocateID() UUID {
	if h.IsAandBinUse() {
		return NilUUID
	}
	id := NewUUID()
	h.WorkInProgressTimestamp = Now().UnixMilli()
	if h.IsActiveIDB {
		h.PhysicalIDA = id
		return id
	}
	h.PhysicalIDB = id
	return id
}

// IsExpiredInactive reports whether the inactive ID has expired based on a fixed window.
func (h *Handle) IsExpiredInactive() bool {
	// Transaction commit is encouraged to be 15 mins max, thus, 1 hr expiration of failed
	// node update ID(inactive or marked deleted ID) seems good.
	const maxDuration = 1
	expiryTime := Now().Add(time.Duration(-maxDuration) * time.Hour).UnixMilli()
	return h.WorkInProgressTimestamp > 0 && h.WorkInProgressTimestamp < expiryTime
}

// HasID reports whether the provided UUID matches either physical ID A or B.
func (h *Handle) HasID(id UUID) bool {
	return h.PhysicalIDA == id || h.PhysicalIDB == id
}

// FlipActiveID switches the active physical ID from A to B or B to A.
func (h *Handle) FlipActiveID() {
	h.IsActiveIDB = !h.IsActiveIDB
}

// ClearInactiveID resets the inactive physical ID to NilUUID and clears the WIP timestamp.
func (h *Handle) ClearInactiveID() {
	if h.IsActiveIDB {
		h.PhysicalIDA = NilUUID
	} else {
		h.PhysicalIDB = NilUUID
	}
	h.WorkInProgressTimestamp = 0
}

// IsEmpty reports whether all Handle fields are zero values (no IDs, not deleted, zero version and timestamps).
func (x *Handle) IsEmpty() bool {
	return x.LogicalID == NilUUID &&
		!x.IsDeleted &&
		x.PhysicalIDA == NilUUID &&
		x.PhysicalIDB == NilUUID &&
		x.Version == 0 &&
		x.WorkInProgressTimestamp == 0
}

// IsEqual reports whether two Handle instances are equal ignoring the Version field.
func (x *Handle) IsEqual(y *Handle) bool {
	return x.LogicalID == y.LogicalID &&
		x.IsDeleted == y.IsDeleted &&
		x.PhysicalIDA == y.PhysicalIDA &&
		x.PhysicalIDB == y.PhysicalIDB
}

// ExtractLogicalIDs converts a slice of RegistryPayload[Handle] to RegistryPayload[UUID] by mapping LogicalID.
func ExtractLogicalIDs(storeHandles []RegistryPayload[Handle]) []RegistryPayload[UUID] {
	r := make([]RegistryPayload[UUID], len(storeHandles))
	for i := range storeHandles {
		r[i].RegistryTable = storeHandles[i].RegistryTable
		r[i].CacheDuration = storeHandles[i].CacheDuration
		r[i].IDs = make([]UUID, len(storeHandles[i].IDs))
		for ii := range storeHandles[i].IDs {
			r[i].IDs[ii] = storeHandles[i].IDs[ii].LogicalID
		}
	}
	return r
}
