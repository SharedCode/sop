package sop

import (
	"testing"

	"github.com/SharedCode/sop/store/in_memory"
)

func TestBtreeBasic(t *testing.T) {
	btree, _ := in_memory.NewStore[string, string]()
	btree.Add("foo", "bar")
	if ok, _ := btree.Find("foo", true); !ok || btree.CurrentValue() != "bar" {
		t.Errorf("Did not find foo's bar.")
	}
}
