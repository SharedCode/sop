package with_cache

import (
	"context"
	"github.com/SharedCode/sop/btree"
)
type nc Connection

// todo: add a local caching feature. For now, only oop (redis) caching.
// todo: implement backend Store IOps

var ctx = context.Background()

func (conn *nc) Add(n *btree.Node) error {
	e := conn.CacheConnection.SetStruct(ctx, format(nodeRepositoryPrefix, n.ID.ToString()), n, -1)
	// todo: Backend Store Add
	return e;
}

func (conn *nc) Update(n *btree.Node) error {
	e := conn.CacheConnection.SetStruct(ctx, format(nodeRepositoryPrefix, n.ID.ToString()), n, -1)
	// todo: Backend Store Update
	return e;
}
func (conn *nc) Get(nodeID btree.Handle) (*btree.Node, error) {
	n := btree.Node{}
	n2,e := conn.CacheConnection.GetStruct(ctx, format(nodeRepositoryPrefix, nodeID.ToString()), &n)
	// todo: Backend Store Get if not found in Cache
	if n2 == nil{return nil, nil}	// not found.
	return &n, e;
}
func (conn *nc) Remove(nodeID btree.Handle) error {
	// todo: Backend Store Remove after remove from Cache
	return conn.CacheConnection.Delete(ctx, format(nodeRepositoryPrefix, nodeID.ToString()))
}
