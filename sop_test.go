package sop

import (
	"testing"
	"encoding/json"
	"github.com/SharedCode/sop/store/in_memory"
)

func TestBtreeBasic(t *testing.T) {
	btree, _ := in_memory.NewStore[string, string]()
	btree.Add("foo", "bar")
	if ok, _ := btree.FindOne("foo", true); !ok || btree.GetCurrentValue() != "bar" {
		ba,_ := json.Marshal(btree)
		t.Errorf("btree: %s", string(ba))
		t.Errorf("Did not find foo's bar.")
	}
}
