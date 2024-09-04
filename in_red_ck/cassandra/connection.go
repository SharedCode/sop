package cassandra

import (
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// Config contains the this Cassandra package configuration or configurable variables.
type Config struct {
	// Cassandra hosts cluster.
	ClusterHosts []string
	// Keyspace to be used when doing I/O to cassandra.
	Keyspace string
	// Default Consistency level.
	Consistency gocql.Consistency
	// Connection Timeout.
	ConnectionTimeout time.Duration
	// Authenticator.
	Authenticator gocql.Authenticator
	// Defaults to "simple strategy & replication factor of 1".
	ReplicationClause string

	// ConsistencyBook should be used to specify consistency level to use for a given
	// API, e.g. one for RegistryAdd, another for StoreAdd, etc... if you so choose to.
	//
	// You can leave it default and the API will use the default Consistency level
	// for the cluster (defaults to local quorum).
	ConsistencyBook ConsistencyBook
}

// Lists all the available API's consistency level that are settable in this package.
type ConsistencyBook struct {
	RegistryAdd     gocql.Consistency
	RegistryUpdate  gocql.Consistency
	RegistryGet     gocql.Consistency
	RegistryRemove  gocql.Consistency
	StoreAdd        gocql.Consistency
	StoreUpdate     gocql.Consistency
	StoreGet        gocql.Consistency
	StoreRemove     gocql.Consistency

	// Blob Store Consistency configs are only used if Blob Store backend is Cassandra,
	// not used in others like File System.
	BlobStoreAdd    gocql.Consistency
	BlobStoreGet    gocql.Consistency
	BlobStoreUpdate gocql.Consistency
	BlobStoreRemove gocql.Consistency
}

// Connection has the Session and the config used to open/create a session.
type Connection struct {
	Session *gocql.Session
	Config
}

var connection *Connection
var mux sync.Mutex

// Returns true if connection instance is valid.
func IsConnectionInstantiated() bool {
	return connection != nil
}

// OpenConnection will create(& return) a new Connection to Cassandra if there is not one yet,
// otherwise, will just return existing singleton connection.
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
	if err := s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.store (name text PRIMARY KEY, root_id UUID, slot_count int, count bigint, unique boolean, des text, reg_tbl text, blob_tbl text, ts bigint, vdins boolean, vdap boolean, vdgc boolean, llb boolean);", config.Keyspace)).Exec(); err != nil {
		return nil, err
	}
	if err := s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.t_log (id UUID, c_f int, c_f_p blob, PRIMARY KEY(id, c_f));", config.Keyspace)).Exec(); err != nil {
		return nil, err
	}

	c.Session = s
	connection = &c
	return connection, nil
}

// Close the singleton connection if open.
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
