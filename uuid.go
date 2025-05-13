package sop

import (
	"bytes"
	"time"

	"github.com/google/uuid"
)

// UUID type.
type UUID uuid.UUID

// Parse a string into a UUID.
func ParseUUID(id string) (UUID, error) {
	u, err := uuid.Parse(id)
	return UUID(u), err
}

// NewUUID returns a new UUID. Will retry after sleep of 1 milli if an error occurs.
// Guaranteed no panic by sleeping/retry, and only does after exhausting 10 attempts.
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

// NillUUID is an empty UUID.
var NilUUID UUID

func (id UUID) IsNil() bool {
	return bytes.Equal(id[:], NilUUID[:])
}

// String converts UUID to its string representation.
func (id UUID) String() string {
	return uuid.UUID(id).String()
}

// Split a UUID into its high & low unsigned int64 bit parts.
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

// Compare two UUIDs. If x < y, return -1, if x > y, 1, otherwise 0.
func (x UUID) Compare(y UUID) int {
	xHigh, xLow := x.Split()
	yHigh, yLow := y.Split()
	if xHigh < yHigh {
		return -1
	}
	if yHigh < xHigh {
		return 1
	}
	if xLow < yLow {
		return -1
	}
	if yLow < xLow {
		return 1
	}
	return 0
}