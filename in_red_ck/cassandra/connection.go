package cassandra

import (
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

type Config struct {
	ClusterHosts []string
	// Keyspace to be used when doing I/O to cassandra.
	Keyspace string
	// Consistency
	Consistency gocql.Consistency
	ConnectionTimeout time.Duration
	Authenticator gocql.Authenticator
	// Defaults to simple strategy & replication factor of 1.
	ReplicationClause string
}

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

// GetConnection will create(& return) a new Connection to Cassandra if there is not one yet,
// otherwise, will just return existing singleton connection.
func GetConnection(config Config) (*Connection, error) {
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
		// Defaults to One consistency. You should set it to an appropriate level.
		config.Consistency = gocql.One
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
	if err := s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.store (name text PRIMARY KEY, root_id UUID, slot_count int, count bigint, unique boolean, des text, reg_tbl text, blob_tbl text, ts bigint, vdins boolean, llb boolean, is_del boolean);", config.Keyspace)).Exec(); err != nil {
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
