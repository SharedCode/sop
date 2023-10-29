package sop

import (
	"testing"

	"github.com/SharedCode/sop/store/in_memory"
)

func TestBtreeBasic(t *testing.T) {
	btree, _ := in_memory.NewStore[string, string]()
	btree.Add("foo", "bar")

	c, _ := btree.Find("foo", )
	if c != "bar" {
		t.Errorf("Did not find foo's bar.")
	}
}
