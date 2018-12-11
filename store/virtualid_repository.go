// Package store contains implementations of Btree interfaces for backend storage I/O.
// This layer adds out of process caching (e.g. via redis) on top of the underlying physical 
// Store implementations such as for Cassandra, etc...
package store;

import "../btree"

func (conn *Connection) Add(vid *btree.VirtualID) error {
	return nil;
}

func (conn *Connection) Update(vid *btree.VirtualID) error {
	return nil;
}
func (conn *Connection) Get(logicalID btree.UUID) (*btree.VirtualID, error) {
	return &btree.VirtualID{}, nil;
}
func (conn *Connection) Remove(logicalID btree.UUID) error {
	return nil;
}

