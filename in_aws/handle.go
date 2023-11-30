package in_aws

import (
	"bytes"

	"github.com/google/uuid"
    "github.com/SharedCode/sop/btree"
)



// Handle is a structure that holds Logical Id and the underlying current Physical Id it maps to.
// E.g. - Node, Slot Value, etc...
// It also contains other fields useful for allowing transaction manager to effectively manage & allow seamless
// switching of data "objects", e.g. a modified Node or Value Data in a transaction can get switched to be the
// "active" one upon commit, and thus, start to get seen by succeeding SOP I/O.
type Handle struct {
	LogicalId   btree.UUID
	PhysicalIdA btree.UUID
	PhysicalIdB btree.UUID
	IsActiveIdB bool
	Version     int
	IsDeleted   bool
}

func (id btree.UUID) ToString() string {
	return string(id[:])
}
func ToUUID(id string) btree.UUID {
	var bid = []byte(id)
	var nid btree.UUID
	copy(nid[:], bid)
	return nid
}

// NewHandle creates a new Handle.
func NewHandle() Handle {
	return Handle{
		LogicalId:   btree.NewUUID(),
		PhysicalIdA: btree.NewUUID(),
	}
}

// ToHandle converts logical & physical UUIDs to a handle, a.k.a. - virtual Id.
func ToHandle(lid btree.UUID, physIdA btree.UUID) Handle {
	return Handle{
		LogicalId:   lid,
		PhysicalIdA: physIdA,
	}
}

func (id Handle) IsEmpty() bool {
	return id.GetActiveId().IsNil()
}

// GetActiveId returns the currently active (if there is) UUID of a given Handle.
func (id Handle) GetActiveId() btree.UUID {
	if id.IsActiveIdB {
		return id.PhysicalIdB
	}
	return id.PhysicalIdA
}

// // ToString method of Handle returns the Handle's currently Active Id's string value.
// func (id Handle) ToString() string {
// 	return id.GetActiveId().ToString()
// }
