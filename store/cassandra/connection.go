
package cassandra;

import "sync"
import "github.com/gocql/gocql"

type Connection struct{
	Session *gocql.Session
	ClusterHosts string
}

// CassandraKeyspace is the keyspace to be used when doing I/O to cassandra.
var CassandraKeyspace = "btree"

var connection *Connection
var mux sync.Mutex

// GetConnection will create(& return) a new Connection to Cassandra if there is not one yet,
// otherwise, will just return existing singleton connection.
func GetConnection(clusterHosts string) (*Connection, error){
	if connection != nil {
		return connection, nil
	}
	mux.Lock()
	defer mux.Unlock()

	if connection != nil {
		return connection, nil
	}
	cluster := gocql.NewCluster(clusterHosts)
	cluster.Keyspace = CassandraKeyspace
	var c = Connection{
		ClusterHosts: clusterHosts,
	}
	s, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	c.Session = s
	connection = &c
	return connection, nil
}
