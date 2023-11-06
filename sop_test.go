package sop

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/SharedCode/sop/store/in_memory"
)

func TestBtreeBasic(t *testing.T) {
	testBtreeAddLoop(t, 200)
}

func TestBtreePromoteAndDistributeStability(t *testing.T) {
	n := 100000
	fmt.Printf("Promote & Distribute(P&D) %d loop test\n\n", n)
	b3, _ := in_memory.NewBtree[string, string]()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("foo%d", i)
		v := fmt.Sprintf("bar%d", i)
		b3.Add(k, v)
		if ok, _ := b3.FindOne(k, true); !ok || b3.GetCurrentValue() != v {
			fmt.Printf("Not found key:%s, value: %s\n", k, v)
		}
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("foo%d", i)
		v := fmt.Sprintf("bar%d", i)
		if ok, _ := b3.FindOne(k, true); !ok || b3.GetCurrentValue() != v {
			fmt.Printf("Not found key:%s, value: %s\n", k, v)
		}
	}
	fmt.Printf("P&D %d loop test end.\n\n", n)
}

func testBtreeAddLoop(t *testing.T, n int) {
	fmt.Printf("btree %d loop test\n\n", n)
	b3, _ := in_memory.NewBtree[string, string]()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("foo%d", i)
		v := fmt.Sprintf("bar%d", i)
		b3.Add(k, v)
		ba, _ := json.Marshal(b3)
		if ok, _ := b3.FindOne(k, true); ok && b3.GetCurrentValue() == v {
			//fmt.Printf("btree: %s\n", string(ba))
		} else {
			t.Errorf("btree: %s\n", string(ba))
			t.Errorf("Did not find %s's %s.\n", k, v)
		}
	}
	fmt.Printf("btree %d loop test end.\n\n", n)
}
