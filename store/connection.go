// Package store contains implementations of Btree interfaces for backend storage I/O.
// This layer adds out of process caching (e.g. via redis) on top of the underlying physical 
// Store implementations such as for Cassandra, etc...
package store

import (
	"fmt"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/cache"
	cass "github.com/SharedCode/sop/store/cassandra"
)

// Type is unsigned int for enumerating supported backend stores.
type Type uint
const (
	// Cassandra backend store
	Cassandra = iota
	//AwsS3
)

// Connection contains client connections used for caching and backend store I/O.
type Connection struct{
	// StoreType specifies which backend store this connection is to interface with.
	StoreType uint
	// CacheConnection is for Redis I/O
	CacheConnection cache.Cache
	CassandraConnection *cass.Connection
}

// NewConnection initializes connections to underlying caching and backend stores like Redis and Cassandra.
func NewConnection(storeType uint, options cache.Options, cassandraConfig cass.Config) (*Connection, error){
	if storeType != Cassandra {
		return nil, fmt.Errorf("'storeType' of int value %d(Cassandra) is the only one supported currently", Cassandra)
	}
	if cassandraConfig.ClusterHosts == nil || len(cassandraConfig.ClusterHosts) == 0{
		return nil, fmt.Errorf("'cassandraClusterHosts' can't be null or empty")
	}
	var cc, err = cass.GetConnection(cassandraConfig)
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

// GetStoreInterface instantiates a Store Interface object with Repositories
// required to manage Btree backend storage.
func (conn *Connection) GetStoreInterface() btree.StoreInterface {
	return btree.StoreInterface{
		StoreType: conn.StoreType,
		StoreRepository: conn.getStoreRepository(),
		NodeRepository: conn.getNodeRepository(),
		VirtualIDRepository: conn.getVirtualIDRepository(),
		RecyclerRepository: conn.getRecyclerRepository(),
		TransactionRepository: conn.getTransactionRepository(),
	}
}

const (
	storeRepositoryPrefix = "StoreRepo"
	nodeRepositoryPrefix = "NodeRepo"
	virtualIDRepositoryPrefix = "VirtualIDRepo"
	transactionRepositoryPrefix = "TransRepo"
	recyclerRepositoryPrefix = "RecyclerRepo"
)

func format(repoPrefix string, repoID string) string{
	return fmt.Sprintf("sop.%s.%s", repoPrefix, repoID)
}

func (conn *Connection) getStoreRepository() btree.StoreRepository{
	o := sc(*conn)
	return &o
}

func (conn *Connection) getVirtualIDRepository() btree.VirtualIDRepository{
	o := vc(*conn)
	return &o
}

func (conn *Connection) getTransactionRepository() btree.TransactionRepository{
	o := tc(*conn)
	return &o
}

func (conn *Connection) getRecyclerRepository() btree.RecyclerRepository{
	o := rc(*conn)
	return &o
}

func (conn *Connection) getNodeRepository() btree.NodeRepository{
	o := nc(*conn)
	return &o
}
