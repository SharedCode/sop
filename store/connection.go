package store

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

func NewConnection(storeType uint, 
	options cache.Options,
	cassandraClusterHosts string,
	) *Connection{
	var cc, err = cass.GetConnection(cassandraClusterHosts)
	if err != nil {
		panic(err)
	}
	var c = Connection{
		StoreType: storeType,
		CacheConnection: cache.NewClient(options),
		CassandraConnection: cc,
	}
	return &c
}