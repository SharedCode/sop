package main

import (
	"context"

	"github.com/gocql/gocql"

	cas "sop/in_cas_s3/cassandra"
)

var Session *gocql.Session
var ctx = context.Background()

func main() {
	var err error

	c := cas.Config{
		ClusterHosts: []string{"172.17.0.2"},
		Keyspace:     "btree",
		// Consistency = gocql.Quorum
	}
	_, err = cas.GetConnection(c)
	if err != nil {
		panic(err)
	}
	// conn.Session.ExecuteBatch()
}
