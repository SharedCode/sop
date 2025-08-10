package inmemory

import (
	"fmt"
	"testing"
)

// Build a small tree, empty out the left child to force a nil child in the parent,
// then delete the parent's separator to exercise removeItemOnNodeWithNilChild and root collapse.
func TestRemove_InternalNode_WithNilChild(t *testing.T) {
	b3 := NewBtree[int, string](true) // unique keys to simplify

	// Insert enough keys to force a root split: with 8 slots per node, adding 9 should split.
	for i := 1; i <= 9; i++ {
		if !b3.Add(i, fmt.Sprintf("v%d", i)) {
			t.Fatalf("add %d failed", i)
		}
	}

	// Remove all items expected to reside in the left child (lower half keys).
	for i := 1; i <= 4; i++ {
		if !b3.Remove(i) {
			t.Fatalf("remove %d failed", i)
		}
	}

	// Now remove the root separator (middle key around the split boundary).
	// From the split logic, the promoted middle is the 5th smallest key.
	if !b3.Remove(5) {
		t.Fatalf("remove root separator failed")
	}

	// Tree should still contain keys 6..9 in a valid structure.
	for i := 6; i <= 9; i++ {
		if !b3.Find(i, false) || b3.GetCurrentKey() != i {
			t.Fatalf("expected to find key %d after structural deletes", i)
		}
	}

	// And removed keys should be gone.
	for i := 1; i <= 5; i++ {
		if b3.Find(i, false) {
			t.Fatalf("did not expect to find deleted key %d", i)
		}
	}
}

// Simple boundary check for Previous at the beginning of the tree.
func TestPrevious_AtBeginning(t *testing.T) {
	b3 := NewBtree[int, string](true)
	for i := 10; i <= 12; i++ {
		if !b3.Add(i, fmt.Sprintf("v%d", i)) {
			t.Fatalf("add %d failed", i)
		}
	}
	if !b3.First() {
		t.Fatalf("First failed")
	}
	if b3.Previous() {
		t.Fatalf("Previous at beginning should be false")
	}
}
