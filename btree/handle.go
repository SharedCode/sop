package btree;

import (
	"bytes"
)

// Handle to SOP data, e.g. - Node, Slot Value, etc...
type Handle struct{
	LogicalID UUID
	IsPhysicalIDB bool
	PhysicalIDA UUID
	PhysicalIDB UUID
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

// NewHandle creates a new Handle with Logical ID set to the 'id' parameter.
func NewHandle(id UUID) Handle{
	var h = Handle{
		LogicalID: id,
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
	return id.LogicalID.IsNil()
}

// GetPhysicalID returns the currently active (if there is) UUID of a given Handle.
func (id Handle) GetPhysicalID() UUID{
	if id.IsPhysicalIDB {return id.PhysicalIDB}
	return id.PhysicalIDA
}

// ToString method of Handle returns the Handle's Logical ID's string value.
func (id Handle) ToString() string{
	return id.LogicalID.ToString()
}
