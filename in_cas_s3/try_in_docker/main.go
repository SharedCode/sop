package main

import (
	"github.com/gocql/gocql"
)

var Session *gocql.Session

func main() {
	var err error

	cluster := gocql.NewCluster("172.17.0.2")
	cluster.Keyspace = "btree"
	cluster.Consistency = gocql.Quorum
	Session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
}
