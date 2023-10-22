package btree;

import (
	"bytes"
	"github.com/google/uuid"
)

// UUID type
type UUID uuid.UUID

// Handle to SOP data, e.g. - Node, Slot Value, etc...
type Handle struct{
	LogicalId UUID
	IsPhysicalIdB bool
	PhysicalIdA UUID
	PhysicalIdB UUID
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

func (id UUID) ToHandle() Handle{
	return NewHandle(id)
}

// NewHandle creates a new Handle with Logical Id set to the 'id' parameter.
func NewHandle(id UUID) Handle{
	var h = Handle{
		LogicalId: id,
	}
	return h
}

// NillUUID is an empty UUID.
var NilUUID UUID

func (id UUID) IsNil() bool{
	return bytes.Compare(id[:], NilUUID[:]) == 0
}

// IsEmpty checkds id whether it is empty or has a value.
func (id Handle) IsEmpty() bool{
	return id.LogicalId.IsNil()
}

// GetPhysicalId returns the currently active (if there is) UUID of a given Handle.
func (id Handle) GetPhysicalId() UUID{
	if id.IsPhysicalIdB {return id.PhysicalIdB}
	return id.PhysicalIdA
}

// ToString method of Handle returns the Handle's Logical Id's string value.
func (id Handle) ToString() string{
	return id.LogicalId.ToString()
}
