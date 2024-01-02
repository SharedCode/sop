package cassandra

import "sync"
import "github.com/gocql/gocql"

type Config struct {
	ClusterHosts []string
	// Keyspace to be used when doing I/O to cassandra.
	Keyspace string
	// Consistency
	Consistency gocql.Consistency
}

type Connection struct {
	Session *gocql.Session
	Config
}

var connection *Connection
var mux sync.Mutex

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
	cluster := gocql.NewCluster(config.ClusterHosts...)
	cluster.Keyspace = config.Keyspace
	cluster.Consistency = config.Consistency
	var c = Connection{
		Config: config,
	}
	s, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	c.Session = s
	connection = &c
	return connection, nil
}
