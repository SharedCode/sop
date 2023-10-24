package btree;

import (
	"bytes"
	"github.com/google/uuid"
)

// UUID type.
type UUID uuid.UUID

// Handle contains Id to SOP data, e.g. - Node, Slot Value, etc...
// It is called Handle
type Handle struct{
	LogicalId UUID
	PhysicalIdA UUID
	PhysicalIdB UUID
	IsActiveIdB bool
	Version int
}

func (id UUID) ToString() string{
	return string(id[:])
}
func ToUUID(id string) UUID{
	var bid = []byte(id)
	var nid UUID
	copy(nid[:], bid)
	return nid
}

// NewHandle creates a new Handle.
func NewHandle() Handle{
	return Handle{
		LogicalId: NewUUID(),
		PhysicalIdA: NewUUID(),
	}
}

// NewUUID returns a new UUID.
func NewUUID() UUID {
	return UUID(uuid.New())
}

// NillUUID is an empty UUID.
var NilUUID UUID

func (id UUID) IsNil() bool{
	return bytes.Compare(id[:], NilUUID[:]) == 0
}

// ToHandle converts logical & physical UUIDs to a handle, a.k.a. - virtual Id.
func ToHandle(lid UUID, physIdA UUID) Handle {
	return Handle{
		LogicalId: lid,
		PhysicalIdA: physIdA,
	}
}

func (id Handle) IsEmpty() bool{
	return id.GetActiveId().IsNil()
}

// GetActiveId returns the currently active (if there is) UUID of a given Handle.
func (id Handle) GetActiveId() UUID{
	if id.IsActiveIdB {return id.PhysicalIdB}
	return id.PhysicalIdA
}

// ToString method of Handle returns the Handle's currently Active Id's string value.
func (id Handle) ToString() string{
	return id.GetActiveId().ToString()
}
