
package cassandra;

import "./btree"

import "testing";
import "github.com/gocql/gocql"

var Session *gocql.Session
func init() {
	var err error
 
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "btree"
	Session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
}

func TestInterfaces(t *testing.T){
	var store = btree.NewStoreRepository()

	store.Add(nil)
	store.Get("")
	store.Remove("")

	var recycler = btree.NewRecycler()
	recycler.Add(nil)

	var nodeRepo = btree.NewNodeRepository()
	nodeRepo.Add(nil)

	// if err := 
	// 	Session.Query("INSERT INTO store(name,root_id,slot_count,is_del) VALUES(?, ?, ?, ?)",
	// 		emp.id, emp.firstName, emp.lastName, emp.age).Exec(); err != nil {
	// }
}
