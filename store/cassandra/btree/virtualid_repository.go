package btree;

import (
	"time"
	"github.com/gocql/gocql"
	cass ".."
	"../../../btree"
)

type CC cass.Connection

// NewUUID generates a new globally unique and time based UUID.
func (conn *CC) NewUUID() btree.UUID{
	return btree.UUID(gocql.UUIDFromTime(time.Now()))
}

func (conn *CC) Add(vid *btree.VirtualID) error {
	return nil;
}

func (conn *CC) Update(vid *btree.VirtualID) error {
	return nil;
}
func (conn *CC) Get(logicalID btree.UUID) (*btree.VirtualID, error) {
	return &btree.VirtualID{}, nil;
}
func (conn *CC) Remove(logicalID btree.UUID) error {
	return nil;
}
