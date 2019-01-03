package store;

import "sop/btree"

type nc Connection

// todo: add a local caching feature. For now, only oop (redis) caching.
// todo: implement backend Store IOps

func (conn *nc) Add(n *btree.Node) error {
	e := conn.CacheConnection.SetStruct(format(nodeRepositoryPrefix, n.ID.ToString()), n, 
		conn.CacheConnection.Options.GetDefaultDuration())
	// todo: Backend Store Add
	return e;
}

func (conn *nc) Update(n *btree.Node) error {
	e := conn.CacheConnection.SetStruct(format(nodeRepositoryPrefix, n.ID.ToString()), n, 
		conn.CacheConnection.Options.GetDefaultDuration())
	// todo: Backend Store Update
	return e;
}
func (conn *nc) Get(nodeID *btree.Handle) (*btree.Node, error) {
	n := btree.Node{}
	n2,e := conn.CacheConnection.GetStruct(format(nodeRepositoryPrefix, nodeID.ToString()), &n)
	// todo: Backend Store Get if not found in Cache
	if n2 == nil{return nil, nil}	// not found.
	return &n, e;
}
func (conn *nc) Remove(nodeID *btree.Handle) error {
	// todo: Backend Store Remove after remove from Cache
	return conn.CacheConnection.DeleteStruct(format(nodeRepositoryPrefix, nodeID.ToString()))
}
