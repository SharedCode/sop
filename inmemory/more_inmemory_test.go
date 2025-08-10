package inmemory

import (
	"fmt"
	"testing"
)

// Table-driven coverage extension tests for the in-memory wrapper.
func TestInMemory_API_EdgeCases_TableDriven(t *testing.T) {
	type step struct {
		name string
		run  func(t *testing.T, b BtreeInterface[int, string])
	}

	tests := []struct {
		name   string
		unique bool
		steps  []step
	}{
		{
			name:   "empty-first-last-find-remove",
			unique: false,
			steps: []step{
				{"First on empty is false", func(t *testing.T, b BtreeInterface[int, string]) {
					if b.First() {
						t.Fatalf("First() on empty got true")
					}
				}},
				{"Last on empty is false", func(t *testing.T, b BtreeInterface[int, string]) {
					if b.Last() {
						t.Fatalf("Last() on empty got true")
					}
				}},
				{"Find on empty is false", func(t *testing.T, b BtreeInterface[int, string]) {
					if b.Find(42, false) {
						t.Fatalf("Find on empty got true")
					}
				}},
				{"Remove missing returns false", func(t *testing.T, b BtreeInterface[int, string]) {
					if b.Remove(123) {
						t.Fatalf("Remove on missing returned true")
					}
				}},
			},
		},
		{
			name:   "nearest-neighbor-and-bounds",
			unique: false,
			steps: []step{
				{"Populate", func(t *testing.T, b BtreeInterface[int, string]) {
					for _, k := range []int{10, 20, 30, 40} {
						if !b.Add(k, fmt.Sprintf("v%d", k)) {
							t.Fatalf("add %d", k)
						}
					}
				}},
				{"Find nearest below picks next greater", func(t *testing.T, b BtreeInterface[int, string]) {
					if b.Find(25, true) {
						t.Fatalf("Find should return false for missing key")
					}
					if b.GetCurrentKey() != 30 {
						t.Fatalf("want current=30 got %d", b.GetCurrentKey())
					}
				}},
				{"Move Previous to get 20", func(t *testing.T, b BtreeInterface[int, string]) {
					if !b.Previous() {
						t.Fatalf("Previous from 30 -> 20 failed")
					}
					if b.GetCurrentKey() != 20 {
						t.Fatalf("want 20 got %d", b.GetCurrentKey())
					}
				}},
				{"Previous at BOF is false", func(t *testing.T, b BtreeInterface[int, string]) {
					// Move to first then go one more Previous to ensure false
					if !b.First() {
						t.Fatalf("First failed")
					}
					if b.GetCurrentKey() != 10 {
						t.Fatalf("first key != 10")
					}
					if b.Previous() {
						t.Fatalf("Previous at BOF should be false")
					}
				}},
				{"Next past EOF is false", func(t *testing.T, b BtreeInterface[int, string]) {
					if !b.Last() {
						t.Fatalf("Last failed")
					}
					if b.GetCurrentKey() != 40 {
						t.Fatalf("last key != 40")
					}
					if b.Next() {
						t.Fatalf("Next at EOF should be false")
					}
				}},
			},
		},
		{
			name:   "AddIfNotExist basic true/false",
			unique: false,
			steps: []step{
				{"AddIfNotExist new", func(t *testing.T, b BtreeInterface[int, string]) {
					if !b.AddIfNotExist(7, "x") {
						t.Fatalf("AddIfNotExist new should be true")
					}
				}},
				{"Add duplicate", func(t *testing.T, b BtreeInterface[int, string]) {
					if !b.Add(7, "y") {
						t.Fatalf("Add duplicate allowed in non-unique")
					}
				}},
				{"AddIfNotExist duplicate returns false", func(t *testing.T, b BtreeInterface[int, string]) {
					if b.AddIfNotExist(7, "z") {
						t.Fatalf("AddIfNotExist duplicate should be false")
					}
				}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBtree[int, string](tc.unique)
			for _, s := range tc.steps {
				t.Run(s.name, func(t *testing.T) { s.run(t, b) })
			}
		})
	}
}
