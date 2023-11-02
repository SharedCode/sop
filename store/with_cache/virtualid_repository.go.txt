package with_cache

import "github.com/SharedCode/sop/btree"
import cassB3 "github.com/SharedCode/sop/store/cassandra/btree"

type vc Connection

// NewUUID generates a new globally unique and
// time based UUID by delegating call to respective Cassandra implementation.
func (conn *vc) NewUUID() btree.UUID {
	var cc = cassB3.CC(*conn.CassandraConnection)
	return cc.NewUUID()
}

func (conn *vc) Add(vid btree.VirtualID) error {
	return nil
}

func (conn *vc) Update(vid btree.VirtualID) error {
	return nil
}
func (conn *vc) Get(logicalID btree.UUID) (btree.VirtualID, error) {
	return btree.VirtualID{}, nil
}
func (conn *vc) Remove(logicalID btree.UUID) error {
	return nil
}
