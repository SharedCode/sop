
package cassandra;

import "testing";
import "github.com/gocql/gocql"

var Session *gocql.Session

func TestConnection(t *testing.T) {
	var err error
 
	cluster := gocql.NewCluster("172.17.0.2")
	cluster.Keyspace = "btree"
	Session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
}

func TestInterfaces(t *testing.T){

	// if err := 
	// 	Session.Query("INSERT INTO store(name,root_id,slot_count,is_del) VALUES(?, ?, ?, ?)",
	// 		emp.id, emp.firstName, emp.lastName, emp.age).Exec(); err != nil {
	// }
}
