package sop

import (
	"testing"
	"encoding/json"
	"github.com/SharedCode/sop/store/in_memory"
)

func TestBtreeBasic(t *testing.T) {
	btree, _ := in_memory.NewStore[string, string]()
	btree.Add("foo", "bar")
	if ok, _ := btree.Find("foo", true); !ok || btree.CurrentItem().Value != "bar"{
		ba,_ := json.Marshal(btree)
		t.Errorf("btree: %s", string(ba))
		t.Errorf("Did not find foo's bar.")
	}
}
