package sop

import (
	"bytes"
	"time"

	"github.com/google/uuid"
)

// UUID is a thin wrapper over github.com/google/uuid.UUID to keep SOP decoupled from the external package.
type UUID uuid.UUID

// ParseUUID converts a string to a UUID. It returns an error if the input is not a valid UUID.
func ParseUUID(id string) (UUID, error) {
	u, err := uuid.Parse(id)
	return UUID(u), err
}

// NewUUID returns a new randomly generated UUID. It retries on error with a 1ms backoff up to 10 times
// and panics only if all attempts fail (which should never happen under normal conditions).
func NewUUID() UUID {
	// In the case of generating new UUID errored, we just need to retry because
	// generating UUID is a must.
	var err error
	for i := 0; i < 10; i++ {
		var id uuid.UUID
		id, err = uuid.NewRandom()
		if err == nil {
			return UUID(id)
		}
		// Sleep 1 millisecond then retry to generate new UUID.
		time.Sleep(time.Duration(1 * time.Millisecond))
	}
	// Panic if still can't generate UUID after 10 retries. Should never happen but in case.
	panic(err)
}

// NilUUID is the zero-value UUID.
var NilUUID UUID

// IsNil reports whether the UUID equals the zero-value UUID.
func (id UUID) IsNil() bool {
	return bytes.Equal(id[:], NilUUID[:])
}

// String returns the canonical string representation of the UUID.
func (id UUID) String() string {
	return uuid.UUID(id).String()
}

// Split returns the high and low 64-bit parts of the UUID.
func (id UUID) Split() (uint64, uint64) {
	// Split UUID into high & low int64 parts.
	bytes := id[:]

	var high uint64
	for i := 0; i < 8; i++ {
		high = high<<8 | uint64(bytes[i])
	}

	var low uint64
	for i := 8; i < 16; i++ {
		low = low<<8 | uint64(bytes[i])
	}
	return high, low
}

// Compare compares two UUIDs and returns -1 if x < y, 1 if x > y, and 0 if they are equal.
func (x UUID) Compare(y UUID) int {
	return bytes.Compare(x[:], y[:])
}
