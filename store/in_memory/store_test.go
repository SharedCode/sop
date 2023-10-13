
package in_memory;

import ""github.com/SharedCode/sop/store/in_memory/btree"

import "testing";

func TestInterfaces(t *testing.T){
	var store = btree.NewStoreRepository()

	store.Add(nil)
	store.Get("")
	store.Remove("")

	var recycler = btree.NewRecycler()
	recycler.Add(nil)

	var nodeRepo = btree.NewNodeRepository()
	nodeRepo.Add(nil)
}
