package btree

import (
	"bytes"
	"github.com/google/uuid"
)

// UUID type.
type UUID uuid.UUID

// Handle is a structure that holds Logical Id and the underlying current Physical Id it maps to.
// E.g. - Node, Slot Value, etc...
// It also contains other fields useful for allowing transaction manager to effectively manage & allow seamless
// switching of data "pages", e.g. a modified Node or Value Data in a transaction can get switched to be the
// "active" one upon commit, and thus, start to get seen by succeeding SOP I/O.
//
// Yes, it is part of the SOP solution for "two phase" commit, ACID type of transaction support.
type Handle struct {
	LogicalId   UUID
	PhysicalIdA UUID
	PhysicalIdB UUID
	IsActiveIdB bool
	Version     int
	IsDeleted bool
}

func (id UUID) ToString() string {
	return string(id[:])
}
func ToUUID(id string) UUID {
	var bid = []byte(id)
	var nid UUID
	copy(nid[:], bid)
	return nid
}

// NewHandle creates a new Handle.
func NewHandle() Handle {
	return Handle{
		LogicalId:   NewUUID(),
		PhysicalIdA: NewUUID(),
	}
}

// NewUUID returns a new UUID.
func NewUUID() UUID {
	return UUID(uuid.New())
}

// NillUUID is an empty UUID.
var NilUUID UUID

func (id UUID) IsNil() bool {
	return bytes.Compare(id[:], NilUUID[:]) == 0
}

// ToHandle converts logical & physical UUIDs to a handle, a.k.a. - virtual Id.
func ToHandle(lid UUID, physIdA UUID) Handle {
	return Handle{
		LogicalId:   lid,
		PhysicalIdA: physIdA,
	}
}

func (id Handle) IsEmpty() bool {
	return id.GetActiveId().IsNil()
}

// GetActiveId returns the currently active (if there is) UUID of a given Handle.
func (id Handle) GetActiveId() UUID {
	if id.IsActiveIdB {
		return id.PhysicalIdB
	}
	return id.PhysicalIdA
}

// ToString method of Handle returns the Handle's currently Active Id's string value.
func (id Handle) ToString() string {
	return id.GetActiveId().ToString()
}
