package sop

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/SharedCode/sop/store/in_memory"
)

func TestBtreeBasic(t *testing.T) {
	// testBtreeAddLoop(t, 1)
	// testBtreeAddLoop(t, 2)
	// testBtreeAddLoop(t, 3)
	// testBtreeAddLoop(t, 4)
	testBtreeAddLoop(t, 5)
}

// TODO: support node breakup! this test fails until such feature is implemented.
func testBtreeAddLoop(t *testing.T, n int) {
	fmt.Printf("btree %d loop test\n\n", n)
	btree, _ := in_memory.NewBtree[string, string]()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("foo%d", i)
		v := fmt.Sprintf("bar%d", i)
		btree.Add(k, v)
		ba, _ := json.Marshal(btree)
		if ok, _ := btree.FindOne(k, true); ok && btree.GetCurrentValue() == v {
			fmt.Printf("btree: %s\n", string(ba))
		} else {
			t.Errorf("btree: %s\n", string(ba))
			t.Errorf("Did not find %s's %s.\n", k, v)
		}
	}
	fmt.Printf("btree %d loop test end.\n\n", n)
}
