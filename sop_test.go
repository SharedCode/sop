package sop

import (
	"fmt"
	"testing"

	"github.com/SharedCode/sop/store/in_memory"
)

func TestBtree(t *testing.T) {
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
