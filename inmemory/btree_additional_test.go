package inmemory

import (
    "fmt"
    "testing"
)

// Exercises First/Last/Previous/Next navigation on a moderately sized tree.
func TestTraversalFirstLastPrevious(t *testing.T) {
    b3 := NewBtree[int, string](true)
    for i := 0; i < 64; i++ {
        if !b3.Add(i, fmt.Sprintf("v%03d", i)) {
            t.Fatalf("Add(%d) failed", i)
        }
    }
    // Forward traversal using First/Next
    if !b3.First() {
        t.Fatalf("First() failed")
    }
    if b3.GetCurrentKey() != 0 {
        t.Fatalf("First key got %d want 0", b3.GetCurrentKey())
    }
    count := 1
    for expect := 1; expect < 64; expect++ {
        if !b3.Next() {
            t.Fatalf("Next() failed at expect=%d", expect)
        }
        if b3.GetCurrentKey() != expect {
            t.Fatalf("Next key got %d want %d", b3.GetCurrentKey(), expect)
        }
        count++
    }
    if count != 64 {
        t.Fatalf("forward traversal count=%d want 64", count)
    }

    // Backward traversal using Last/Previous
    if !b3.Last() {
        t.Fatalf("Last() failed")
    }
    if b3.GetCurrentKey() != 63 {
        t.Fatalf("Last key got %d want 63", b3.GetCurrentKey())
    }
    count = 1
    for expect := 62; expect >= 0; expect-- {
        if expect == -1 {
            break
        }
        if !b3.Previous() {
            t.Fatalf("Previous() failed at expect=%d", expect)
        }
        if b3.GetCurrentKey() != expect {
            t.Fatalf("Previous key got %d want %d", b3.GetCurrentKey(), expect)
        }
        count++
        if expect == 0 {
            break
        }
    }
    if count != 64 {
        t.Fatalf("backward traversal count=%d want 64", count)
    }
}

// Verifies behavior when non-unique keys are used and duplicates exist.
func TestDuplicateKeysNonUniqueIteration(t *testing.T) {
    b3 := NewBtree[int, string](false)

    key := 42
    values := []string{"a", "b", "c", "d", "e"}
    for _, v := range values {
        if !b3.Add(key, v) {
            t.Fatalf("Add duplicate for key=%d failed", key)
        }
    }

    if !b3.Find(key, true) {
        t.Fatalf("Find(%d,true) failed", key)
    }
    // Iterate all items that share the same key using Next
    seen := 1
    for b3.Next() && b3.GetCurrentKey() == key {
        seen++
    }
    if seen != len(values) {
        t.Fatalf("duplicate count got %d want %d", seen, len(values))
    }

    // AddIfNotExist should fail for an existing key
    if b3.AddIfNotExist(key, "z") {
        t.Fatalf("AddIfNotExist unexpectedly succeeded for existing key")
    }
}

// Inserts and removes enough items to force splits and merges.
func TestSplitMergeBasic(t *testing.T) {
    // Unique keys to simplify expectations
    b3 := NewBtree[int, string](true)

    // Insert enough keys to cause multiple node splits (itemsPerNode is 8)
    const n = 128
    for i := 0; i < n; i++ {
        if !b3.Add(i, fmt.Sprintf("v%03d", i)) {
            t.Fatalf("Add(%d) failed", i)
        }
    }
    if b3.Count() != n {
        t.Fatalf("Count() got %d want %d", b3.Count(), n)
    }

    // Remove a middle band to trigger rebalancing/merges
    for i := 16; i < 96; i++ {
        if !b3.Remove(i) {
            t.Fatalf("Remove(%d) failed", i)
        }
    }

    // Verify survivors and absences
    for i := 0; i < n; i++ {
        found := b3.Find(i, true)
        if i < 16 || i >= 96 {
            if !found {
                t.Fatalf("expected key %d to exist", i)
            }
        } else {
            if found {
                t.Fatalf("expected key %d to be deleted", i)
            }
        }
    }

    // Reinsert removed band to trigger further rebalancing
    for i := 16; i < 96; i++ {
        if !b3.Add(i, fmt.Sprintf("v%03d", i)) {
            t.Fatalf("Re-Add(%d) failed", i)
        }
    }

    if b3.Count() != n {
        t.Fatalf("final Count() got %d want %d", b3.Count(), n)
    }

    // Sanity traversal across boundaries
    if !b3.First() || b3.GetCurrentKey() != 0 {
        t.Fatalf("First traversal start failed")
    }
    steps := 1
    for b3.Next() {
        steps++
    }
    if steps != n {
        t.Fatalf("traversal steps got %d want %d", steps, n)
    }
}

// Exercises update paths and cursor edge cases on a small tree to tick branches.
func TestUpdateAndCursorEdgeCases(t *testing.T) {
    b3 := NewBtree[int, string](false)

    // Empty navigations
    if b3.First() {
        t.Fatalf("First() on empty should be false")
    }
    if b3.Last() {
        t.Fatalf("Last() on empty should be false")
    }

    // Populate
    for i := 0; i < 10; i++ {
        if !b3.Add(i, "v") {
            t.Fatalf("Add(%d) failed", i)
        }
    }

    // Previous at first should be false
    if !b3.First() {
        t.Fatalf("First() failed")
    }
    if b3.Previous() {
        t.Fatalf("Previous() at first should be false")
    }

    // Next at last should be false
    if !b3.Last() {
        t.Fatalf("Last() failed")
    }
    if b3.Next() {
        t.Fatalf("Next() at last should be false")
    }

    // Update existing key via Update
    if !b3.Update(5, "v5") {
        t.Fatalf("Update(5) failed")
    }
    if !b3.Find(5, true) || b3.GetCurrentValue() != "v5" {
        t.Fatalf("Find after Update failed: got %q", b3.GetCurrentValue())
    }

    // UpdateCurrentItem
    if !b3.UpdateCurrentItem("v5b") {
        t.Fatalf("UpdateCurrentItem failed")
    }
    if !b3.Find(5, true) || b3.GetCurrentValue() != "v5b" {
        t.Fatalf("Verify UpdateCurrentItem failed got %q", b3.GetCurrentValue())
    }

    // RemoveCurrentItem and verify cursor advances
    if !b3.RemoveCurrentItem() {
        t.Fatalf("RemoveCurrentItem failed")
    }
    // After removal, the next key should be 6 (if any)
    if b3.Find(6, true) == false {
        t.Fatalf("Expected key 6 to exist after removing 5")
    }
}
