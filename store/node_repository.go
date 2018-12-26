package store;

import "../btree"

type nc Connection

// todo: add a local caching feature. For now, only oop (redis) caching.

func (conn *nc) Add(n *btree.Node) error {
	conn.CacheConnection.SetStruct(n.ID.String(), n, 
	conn.CacheConnection.Options.GetDefaultDuration())
	// todo: Backend Store Add
	return nil;
}

func (conn *nc) Update(n *btree.Node) error {
	conn.CacheConnection.SetStruct(n.ID.String(), n, 
	conn.CacheConnection.Options.GetDefaultDuration())
	// todo: Backend Store Update
	return nil;
}
func (conn *nc) Get(nodeID *btree.Handle) (*btree.Node, error) {
	n := btree.Node{}
	_,e := conn.CacheConnection.GetStruct(nodeID.String(), &n)
	// todo: Backend Store Get if not found in Cache
	return &n, e;
}
func (conn *nc) Remove(nodeID *btree.Handle) error {
	// todo: Backend Store Remove after remove from Cache
	return conn.CacheConnection.DeleteStruct(nodeID.String())
}
