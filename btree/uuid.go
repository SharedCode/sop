package btree

import (
	"bytes"
	"time"

	"github.com/google/uuid"
)

// UUID type.
type UUID uuid.UUID

// NewUUID returns a new UUID. Will retry after sleep of 1 milli if an error occurs.
// Guaranteed no panic by sleeping/retry.
func NewUUID() UUID {
	// In the case of generating new UUID errored, we just need to retry because
	// generating UUID is a must.
	for {
		id, err := uuid.NewRandom()
		if err == nil {
			return UUID(id)
		}
		// Sleep 1 millisecond then retry to generate new UUID.
		time.Sleep(time.Duration(1 * time.Millisecond))
	}
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

// ToUUID converts UUID string to its UUID representation.
func ToUUID(id string) UUID {
	var bid = []byte(id)
	var nid UUID
	copy(nid[:], bid)
	return nid
}
