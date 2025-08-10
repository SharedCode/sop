package inmemory

import (
	"testing"
)

// Covers: Btree.FindWithID with duplicate keys, ensuring we can locate a specific item by ID.
func TestFindWithID_Duplicates(t *testing.T) {
	b3 := NewBtree[int, string](false) // allow duplicates

	if !b3.Add(100, "a") || !b3.Add(100, "b") || !b3.Add(100, "c") {
		t.Fatalf("failed to add duplicates")
	}

	// Position at the first 100 and capture IDs as we iterate.
	if ok := b3.Find(100, true); !ok {
		t.Fatalf("expected to find key=100")
	}
	first := b3.Btree.GetCurrentKey() // includes ID

	if ok := b3.Next(); !ok {
		t.Fatalf("expected to move to second duplicate")
	}
	second := b3.Btree.GetCurrentKey()

	if ok := b3.Next(); !ok {
		t.Fatalf("expected to move to third duplicate")
	}
	third := b3.Btree.GetCurrentKey()

	// Now use FindWithID to target the middle duplicate specifically.
	if ok, err := b3.Btree.FindWithID(nil, 100, second.ID); err != nil || !ok {
		t.Fatalf("FindWithID failed: ok=%v err=%v", ok, err)
	}
	got := b3.Btree.GetCurrentKey()
	if got.ID != second.ID {
		t.Fatalf("FindWithID positioned wrong item: gotID=%v wantID=%v", got.ID, second.ID)
	}

	// Sanity: ensure the other IDs are distinct and still reachable.
	if first.ID == second.ID || second.ID == third.ID || first.ID == third.ID {
		t.Fatalf("expected distinct IDs for duplicates")
	}
}
