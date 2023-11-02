package in_memory

import (
	"github.com/SharedCode/sop/btree"
)

type vid_repository struct{}

func newVirtualIdRepository() btree.VirtualIdRepository {
	return &vid_repository{}
}

func (conn *vid_repository) Add(h btree.Handle) error {
	return nil
}

func (conn *vid_repository) Update(h btree.Handle) error {
	return nil
}
func (conn *vid_repository) Get(logicalID btree.UUID) (btree.Handle, error) {
	return btree.Handle{
		LogicalId: logicalID,
		PhysicalIdA: logicalID,
	}, nil
}
func (conn *vid_repository) Remove(logicalID btree.UUID) error {
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
