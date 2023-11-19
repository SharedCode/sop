package btree

import (
	"time"
	"github.com/gocql/gocql"

	"github.com/SharedCode/sop/btree"
	cass "github.com/SharedCode/sop/store/cassandra"
)

type CC cass.Connection

// NewUUID generates a new globally unique and time based UUID.
func (conn *CC) NewUUID() btree.UUID {
	return btree.UUID(gocql.UUIDFromTime(time.Now()))
}

func (conn *CC) Add(vid btree.Handle) error {
	return nil
}

func (conn *CC) Update(vid btree.Handle) error {
	return nil
}
func (conn *CC) Get(logicalID btree.UUID) (btree.Handle, error) {
	return btree.Handle{}, nil
}
func (conn *CC) Remove(logicalID btree.UUID) error {
	return nil
}

/*
Logical ID to Physical ID story:
A. Node ID handling
- Btree reader will always use Logical ID so it can read the "active" Node
- During a Transaction phase 1 commit:
	 - Updated Nodes will actually be "new" Nodes that are copies of the currently "active" Node.
	 - New Nodes will be persisted with (final) Logical ID to Physical ID map.
- During phase 2 commit:
	 - Updated Nodes' Physical ID will be made the current "active" Node in the Virual Registry.

B. Value ID handling
- Logical ID handling does not apply for Values stored on Node itself as there is no separate entry for it.
- Values that are stored in separate Value table (e.g. - slot_value) will be handled similar
to Node Update described above.

NOTE: Based on above story, Logical ID handling will be the default ID known to Btree. There is a
special override action, that is:
- Updated Nodes will "know" it is "new" and has Logical ID entry persisted for use during phase 2 commit.
During phase 2 commit, handler will use this Logical ID to make it the "active" Node.
- Other objects like Value stored in separate table, will be handled similar to updated Node.

*/
