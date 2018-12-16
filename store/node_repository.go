package store;

import "../btree"

type nc Connection

func (conn *nc) Add(n *btree.Node) error {
	conn.CacheConnection.SetStruct(n.ID.ToString(), n, conn.CacheConnection.Options.DefaultDuration)
	// todo: Backend Store Add
	return nil;
}

func (conn *nc) Update(n *btree.Node) error {
	conn.CacheConnection.SetStruct(n.ID.ToString(), n, conn.CacheConnection.Options.DefaultDuration)
	// todo: Backend Store Update
	return nil;
}
func (conn *nc) Get(nodeID btree.UUID) (*btree.Node, error) {
	n := btree.Node{}
	_,e := conn.CacheConnection.GetStruct(n.ID.ToString(), &n)
	// todo: Backend Store Get if not found in Cache
	return &n, e;
}
func (conn *nc) Remove(nodeID btree.UUID) error {
	// todo: Backend Store Remove after remove from Cache
	return conn.CacheConnection.DeleteStruct(nodeID.ToString())
}
