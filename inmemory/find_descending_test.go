package inmemory

import (
	"fmt"
	"testing"
)

func TestFindInDescendingOrder(t *testing.T) {
	// Setup
	b := NewBtree[string, string](false)
	b.Add("1", "val1")
	b.Add("3", "val3")
	b.Add("5", "val5")

	// Case 1: Exact Match "3"
	if !b.FindInDescendingOrder("3") {
		t.Errorf("FindInDescendingOrder('3') returned false")
	}
	if k := b.GetCurrentKey(); k != "3" {
		t.Errorf("FindInDescendingOrder('3') positioned at '%s', expected '3'", k)
	}

	// Case 2: Search "4" (Between 3 and 5)
	// Expectation: Position at "5" (Smallest > 4)
	if b.FindInDescendingOrder("4") {
		t.Errorf("FindInDescendingOrder('4') returned true, expected false")
	}
	k := b.GetCurrentKey()
	if k != "5" {
		t.Errorf("FindInDescendingOrder('4') positioned at '%s', expected '5'", k)
	}

	// Case 3: Search "6" (Greater than all)
	// Expectation: Position at "5" (Largest item)
	if b.FindInDescendingOrder("6") {
		t.Errorf("FindInDescendingOrder('6') returned true, expected false")
	}
	k = b.GetCurrentKey()
	if k != "5" {
		t.Errorf("FindInDescendingOrder('6') positioned at '%s', expected '5'", k)
	}

	// Case 4: Search "0" (Less than all)
	// Expectation: Position at "1" (Smallest item)
	if b.FindInDescendingOrder("0") {
		t.Errorf("FindInDescendingOrder('0') returned true, expected false")
	}
	k = b.GetCurrentKey()
	if k != "1" {
		t.Errorf("FindInDescendingOrder('0') positioned at '%s', expected '1'", k)
	}
}

func TestFindInDescendingOrder_ManyItems(t *testing.T) {
	// Setup with enough items to cause splits (itemsPerNode = 8)
	b := NewBtree[int, string](false)
	for i := 10; i <= 100; i += 10 {
		b.Add(i, fmt.Sprintf("val%d", i))
	}
	// Items: 10, 20, ..., 100

	// Case 1: Search 55 (Between 50 and 60)
	// Expectation: Position at 60 (Smallest > 55)
	if b.FindInDescendingOrder(55) {
		t.Errorf("FindInDescendingOrder(55) returned true")
	}
	if k := b.GetCurrentKey(); k != 60 {
		t.Errorf("FindInDescendingOrder(55) positioned at %d, expected 60", k)
	}
	// Verify Previous() goes to 50
	if !b.Previous() {
		t.Errorf("Previous() failed after FindInDescendingOrder(55)")
	}
	if k := b.GetCurrentKey(); k != 50 {
		t.Errorf("Previous() positioned at %d, expected 50", k)
	}

	// Case 2: Search 105 (Greater than all)
	// Expectation: Position at 100 (Largest)
	if b.FindInDescendingOrder(105) {
		t.Errorf("FindInDescendingOrder(105) returned true")
	}
	if k := b.GetCurrentKey(); k != 100 {
		t.Errorf("FindInDescendingOrder(105) positioned at %d, expected 100", k)
	}
	// Verify Previous() goes to 90
	if !b.Previous() {
		t.Errorf("Previous() failed after FindInDescendingOrder(105)")
	}
	if k := b.GetCurrentKey(); k != 90 {
		t.Errorf("Previous() positioned at %d, expected 90", k)
	}

	// Case 3: Search 5 (Less than all)
	// Expectation: Position at 10 (Smallest)
	if b.FindInDescendingOrder(5) {
		t.Errorf("FindInDescendingOrder(5) returned true")
	}
	if k := b.GetCurrentKey(); k != 10 {
		t.Errorf("FindInDescendingOrder(5) positioned at %d, expected 10", k)
	}
	// Verify Previous() returns false
	if b.Previous() {
		t.Errorf("Previous() returned true after FindInDescendingOrder(5), expected false")
	}
}
