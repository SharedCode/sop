// Package store contains implementations of Btree interfaces for backend storage I/O.
// This layer adds out of process caching (e.g. via redis) on top of the underlying physical 
// Store implementations such as for Cassandra, etc...
package store

import "../btree"
import "../cache"
import cass "./cassandra"

// Connection contains client connections used for caching and backend store I/O.
type Connection struct{
	// StoreType specifies which backend store this connection is to interface with.
	StoreType uint
	// CacheConnection is for Redis I/O
	CacheConnection *cache.Connection
	CassandraConnection *cass.Connection
}

func NewConnection(storeType uint, options cache.Options, cassandraClusterHosts ...string) (*Connection, error){
	var cc, err = cass.GetConnection(cassandraClusterHosts...)
	if err != nil {
		return nil, err
	}
	var c = Connection{
		StoreType: storeType,
		CacheConnection: cache.NewClient(options),
		CassandraConnection: cc,
	}
	return &c, nil
}

func (conn *Connection) GetVirtualIDRepository() btree.VirtualIDRepository{
	o := vc(*conn)
	return &o
}

func (conn *Connection) GetTransactionRepository() btree.TransactionRepository{
	o := tc(*conn)
	return &o
}


