package cassandra

import (
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// Config contains configuration for connecting to a Cassandra cluster and SOP keyspace.
type Config struct {
	// ClusterHosts lists contact points for the Cassandra cluster.
	ClusterHosts []string
	// Keyspace is the keyspace used for SOP tables.
	Keyspace string
	// Consistency is the default consistency level for queries.
	Consistency gocql.Consistency
	// ConnectionTimeout is the session connection timeout.
	ConnectionTimeout time.Duration
	// Authenticator is used when the cluster requires authentication.
	Authenticator gocql.Authenticator
	// ReplicationClause defines the keyspace replication (e.g., SimpleStrategy).
	ReplicationClause string

	// ConsistencyBook allows overriding per-API consistency levels.
	ConsistencyBook ConsistencyBook
}

// ConsistencyBook enumerates per-API consistency levels used by this package.
type ConsistencyBook struct {
	RegistryAdd    gocql.Consistency
	RegistryUpdate gocql.Consistency
	RegistryGet    gocql.Consistency
	RegistryRemove gocql.Consistency
	StoreAdd       gocql.Consistency
	StoreUpdate    gocql.Consistency
	StoreGet       gocql.Consistency
	StoreRemove    gocql.Consistency

	// Blob store consistency levels are only used when the blob backend is Cassandra.
	BlobStoreAdd    gocql.Consistency
	BlobStoreGet    gocql.Consistency
	BlobStoreUpdate gocql.Consistency
	BlobStoreRemove gocql.Consistency
}

// Connection wraps a Cassandra session and its configuration.
type Connection struct {
	Session *gocql.Session
	Config
}

var connection *Connection
var mux sync.Mutex

// IsConnectionInstantiated reports whether a global Connection has been created.
func IsConnectionInstantiated() bool {
	return connection != nil
}

// OpenConnection returns the existing global Connection or opens a new one using the provided config.
func OpenConnection(config Config) (*Connection, error) {
	if connection != nil {
		return connection, nil
	}
	mux.Lock()
	defer mux.Unlock()

	if connection != nil {
		return connection, nil
	}
	if config.Keyspace == "" {
		// default keyspace
		config.Keyspace = "btree"
	}
	if config.Consistency == gocql.Any {
		// Defaults to LocalQuorum consistency. You should set it to an appropriate level.
		config.Consistency = gocql.LocalQuorum
	}
	cluster := gocql.NewCluster(config.ClusterHosts...)
	cluster.Consistency = config.Consistency
	if config.ReplicationClause == "" {
		// Specify an appropriate replication feature.
		config.ReplicationClause = "{'class':'SimpleStrategy', 'replication_factor':1}"
	}
	if config.ConnectionTimeout > 0 {
		cluster.ConnectTimeout = config.ConnectionTimeout
	}
	if config.Authenticator != nil {
		cluster.Authenticator = config.Authenticator
		// Clear the authenticator just to be safer, we don't need to keep it hanging around.
		config.Authenticator = nil
	}
	var c = Connection{
		Config: config,
	}
	s, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	if err := s.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s;", config.Keyspace, config.ReplicationClause)).Exec(); err != nil {
		return nil, err
	}
	// Auto create the "store" table if not yet.
	if err := s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.store (name text PRIMARY KEY, root_id UUID, slot_count int, count bigint, unique boolean, des text, reg_tbl text, blob_tbl text, ts bigint, vdins boolean, vdap boolean, vdgc boolean, llb boolean, rcd bigint, rc_ttl boolean, ncd bigint, nc_ttl boolean, vdcd bigint, vdc_ttl boolean, scd bigint, sc_ttl boolean);", config.Keyspace)).Exec(); err != nil {
		return nil, err
	}
	if err := s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.t_log (id UUID, c_f int, c_f_p blob, PRIMARY KEY(id, c_f));", config.Keyspace)).Exec(); err != nil {
		return nil, err
	}

	c.Session = s
	connection = &c
	return connection, nil
}

// CloseConnection closes and clears the global connection, if it exists.
func CloseConnection() {
	if connection != nil {
		mux.Lock()
		defer mux.Unlock()
		if connection == nil {
			return
		}
		connection.Session.Close()
		connection = nil
	}
}
