package btree

import (
	"bytes"
	"github.com/google/uuid"
)

// UUID type.
type UUID uuid.UUID

// NewUUID returns a new UUID.
func NewUUID() UUID {
	return UUID(uuid.New())
}

// NillUUID is an empty UUID.
var NilUUID UUID

func (id UUID) IsNil() bool {
	return bytes.Compare(id[:], NilUUID[:]) == 0
}

// ToString converts UUID to its string representation.
func (id UUID) ToString() string {
	return string(id[:])
}
