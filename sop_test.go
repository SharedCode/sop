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

func TestBtreeNilChild(t *testing.T) {
	max := 100000
	fmt.Printf("Nil child %d loop test\n\n", max)
	b3, _ := in_memory.NewBtree[string, string](false)

	tests := []struct {
		name string
		startRange int
		endRange int
		action int
	}{
		{
			name: "populate",
			startRange: 0,
			endRange: max,
			action: 1,	// add
		},
		{
			name: "find1",
			startRange: 0,
			endRange: max,
			action: 2,	// find
		},
		{
			name: "remove1",
			startRange: 450,
			endRange: 800,
			action: 3, // remove
		},
		{
			name: "readd deleted items",
			startRange: 450,
			endRange: 800,
			action: 1,
		},
		{
			name: "findAll1",
			startRange: 0,
			endRange: max,
			action: 2,
		},
		{
			name: "remove2",
			startRange: 5000,
			endRange: 10000,
			action: 3,
		},
		{
			name: "readd deleted items2",
			startRange: 5000,
			endRange: 10000,
			action: 1,
		},
		{
			name: "findAll2",
			startRange: 0,
			endRange: max,
			action: 2,	// find
		},
	}

	for _,test := range tests {
		t.Logf("Test %s started.", test.name)
		for i := test.startRange; i < test.endRange; i++ {
			k := fmt.Sprintf("foo%d", i)
			v := fmt.Sprintf("bar%d", i)

			switch test.action {
			case 1:
				if ok,_ := b3.Add(k,v); !ok {
					t.Errorf("Failed Add item with key %s.\n", k)
				}
			case 2:
				if ok,_ := b3.FindOne(k, true); !ok {
					t.Errorf("Failed FindOne item with key %s.\n", k)
				}
			case 3:
				if ok,err := b3.Remove(k); !ok {
					t.Errorf("Failed Delete item with key %s, error: %v\n", k, err)
				}
			}
		}
		t.Logf("Test %s ended.", test.name)
	}
	fmt.Printf("Nil child %d loop test ended.\n\n", max)
}

func TestBtreePromoteAndDistributeStability(t *testing.T) {
	n := 100000
	fmt.Printf("Promote & Distribute(P&D) %d loop test\n\n", n)
	b3, _ := in_memory.NewBtree[string, string](false)
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
	b3, _ := in_memory.NewBtree[string, string](false)
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
