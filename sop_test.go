package sop

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/SharedCode/sop/store/in_memory"
)

func TestBtreeBasic(t *testing.T) {
	btree, _ := in_memory.NewStore[string, string]()
	btree.Add("foo", "bar")
	ba, _ := json.Marshal(btree)
	if ok, _ := btree.FindOne("foo", true); ok && btree.GetCurrentValue() == "bar" {
		fmt.Printf("btree: %s", string(ba))
	} else {
		t.Errorf("btree: %s", string(ba))
		t.Errorf("Did not find foo's bar.")
	}
}

// TODO: support node breakup! this test fails until such feature is implemented.
func TestBtreeAddLoop(t *testing.T) {
	btree, _ := in_memory.NewStore[string, string]()
	const n = 5
	for i := 0; i < n; i++ {
		btree.Add(fmt.Sprintf("foo%d", i), fmt.Sprintf("bar%d", i))
	}
	ba, _ := json.Marshal(btree)
	if ok, _ := btree.FindOne("foo1", true); ok && btree.GetCurrentValue() == "bar1" {
		fmt.Printf("btree: %s", string(ba))
	} else {
		t.Errorf("btree: %s", string(ba))
		t.Errorf("Did not find foo's bar.")
	}
}
