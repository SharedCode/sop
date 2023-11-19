package sop

import (
	"fmt"
	"testing"

	sop "github.com/SharedCode/sop/in_memory"
)

func TestBtree_HelloWorld(t *testing.T) {
	fmt.Printf("Btree hello world.\n")
	b3, _ := sop.NewBtree[int, string](false)

	b3.Add(5000, "I am the value with 5000 key.")
	b3.Add(5001, "I am the value with 5001 key.")
	b3.Add(5000, "I am also a value with 5000 key.")

	if ok,_ := b3.FindOne(5000, true); !ok || b3.GetCurrentKey() != 5000 {
		t.Errorf("FindOne(5000, true) failed, got = %v, want = 5000", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())

	if ok,_ := b3.MoveToNext(); !ok || b3.GetCurrentKey() != 5000 {
		t.Errorf("MoveToNext() failed, got = %v, want = 5000", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())

	if ok,_ :=b3.MoveToNext(); !ok || b3.GetCurrentKey() != 5001 {
		t.Errorf("MoveToNext() failed, got = %v, want = 5001", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())

	fmt.Printf("Btree hello world ended.\n\n")
	b3 = nil
}

func TestBtree_ComplexDataMgmtCases(t *testing.T) {
	max := 100000
	fmt.Printf("Btree complex data mgmt tests started(%d items).\n\n", max)
	b3, _ := sop.NewBtree[int, string](false)

	tests := []struct {
		name       string
		startRange int
		endRange   int
		action     int
		wantFound  int
	}{
		{
			name:       "populate",
			startRange: 0,
			endRange:   max,
			action:     1, // add
		},
		{
			name:       "find1",
			startRange: 0,
			endRange:   max,
			action:     2, // find
		},
		{
			name:       "remove1",
			startRange: 450,
			endRange:   542,
			action:     3, // remove
		},
		{
			name:       "remove2",
			startRange: 543,
			endRange:   600,
			action:     3, // remove
		},
		{
			name:       "Find with missing items 1",
			startRange: 445,
			endRange:   607,
			action:     4,  // FindOne + track not found items
			wantFound:  12,
		},
		{
			name:       "readd deleted items",
			startRange: 450,
			endRange:   600,
			action:     1,
		},
		{
			name:       "findAll1",
			startRange: 0,
			endRange:   max,
			action:     2,
		},
		{
			name:       "remove3",
			startRange: 60000,
			endRange:   90000,
			action:     3,
		},
		{
			name:       "remove4",
			startRange: 91000,
			endRange:   99000,
			action:     3,
		},
		{
			name:       "findAll1",
			startRange: 0,
			endRange:   max,
			action:     4,
			wantFound:  61999,
		},
		{
			name:       "readd 2",
			startRange: 60000,
			endRange:   90000,
			action:     1,
		},
		{
			name:       "readd 3",
			startRange: 91000,
			endRange:   99000,
			action:     1,
		},
		{
			name:       "remove all",
			startRange: 0,
			endRange:   max,
			action:     3,
		},
		{
			name:       "readd all",
			startRange: 0,
			endRange:   max,
			action:     1,
		},
		{
			name:       "Range Query all",
			startRange: 0,
			endRange:   max,
			action:     5,   // FindOne + MoveNext()
			wantFound:  max+1,
		},
	}

	var k int
	for _, test := range tests {
		t.Logf("Test %s started.", test.name)
		if test.action == 4 {
			itemsFoundCount := 0
			for i := test.startRange; i <= test.endRange; i++ {
				k = i
				if ok, _ := b3.FindOne(k, true); ok {
					itemsFoundCount++
				}
			}
			if itemsFoundCount != test.wantFound {
				t.Errorf("got %d items, want %d", itemsFoundCount, test.wantFound)
				t.Logf("Test %s ended.", test.name)
				t.FailNow()
			}
			continue
		}
		if test.action == 5 {
			itemsFoundCount := 0
			k = test.startRange
			if ok, _ := b3.FindOne(k, true); ok {
				itemsFoundCount++
			}
			for i := test.startRange+1; i <= test.endRange; i++ {
				k = i
				if ok, _ := b3.MoveToNext(); ok {
					if b3.GetCurrentKey() == k {
						itemsFoundCount++
						continue
					}
					t.Errorf("got %d key, want %d key", b3.GetCurrentKey(), k)
				}
			}
			if itemsFoundCount != test.wantFound {
				t.Errorf("got %d items, want %d", itemsFoundCount, test.wantFound)
				t.Logf("Test %s ended.", test.name)
				t.FailNow()
			}
			continue
		}
		for i := test.startRange; i <= test.endRange; i++ {
			k = i
			v := fmt.Sprintf("bar%d", i)

			switch test.action {
			case 1:
				if ok, _ := b3.Add(k, v); !ok {
					t.Errorf("Failed Add item with key %d.\n", k)
				}
			case 2:
				if ok, _ := b3.FindOne(k, true); !ok {
					t.Errorf("Failed FindOne item with key %d.\n", k)
				}
			case 3:
				if test.name == "remove all" && k == 99999 {
					i := 90
					i++
				}
				if ok, err := b3.Remove(k); !ok {
					t.Errorf("Failed Remove item with key %d, error: %v\n", k, err)
				}
			}
		}
		t.Logf("Test %s ended.", test.name)
	}
	fmt.Printf("Btree complex data mgmt tests ended.\n\n")
}

func TestBtree_SimpleDataMgmtCases(t *testing.T) {
	max := 100000
	fmt.Printf("Btree simple data mgmt tests started(%d items)\n\n", max)
	b3, _ := sop.NewBtree[string, string](false)

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
	fmt.Printf("Btree simple data mgmt tests ended.\n\n")
}
