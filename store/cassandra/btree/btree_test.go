
package btree;

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

func TestBtreeInterfaces(t *testing.T){
	var store = NewStoreRepository();

	store.Add(nil);
	store.Get("");
	store.Remove("");

	var recycler = NewRecycler();
	recycler.Add(nil);

	var nodeRepo = NewNodeRepository();

	// if err := 
	// 	Session.Query("INSERT INTO store(name,root_id,slot_count,is_del) VALUES(?, ?, ?, ?)",
	// 		emp.id, emp.firstName, emp.lastName, emp.age).Exec(); err != nil {
	// }
}
