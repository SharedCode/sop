package inmemory

import (
	"fmt"
	"testing"
)

func newSeeded(t *testing.T, keys []int) BtreeInterface[int, string] {
	t.Helper()
	b3 := NewBtree[int, string](true)
	for _, k := range keys {
		if !b3.Add(k, fmt.Sprintf("v%d", k)) {
			t.Fatalf("Add(%d) failed", k)
		}
	}
	return b3
}

func TestAllYieldsEverythingInOrder(t *testing.T) {
	b3 := newSeeded(t, []int{50, 10, 40, 20, 30})
	var got []int
	for k, v := range b3.All() {
		if v != fmt.Sprintf("v%d", k) {
			t.Fatalf("key %d got value %q", k, v)
		}
		got = append(got, k)
	}
	want := []int{10, 20, 30, 40, 50}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestAllEarlyBreak(t *testing.T) {
	b3 := newSeeded(t, []int{1, 2, 3, 4, 5})
	n := 0
	for range b3.All() {
		n++
		if n == 2 {
			break
		}
	}
	if n != 2 {
		t.Fatalf("iterated %d times, want 2", n)
	}
}

func TestAllOnEmptyTree(t *testing.T) {
	b3 := NewBtree[int, string](true)
	for k, v := range b3.All() {
		t.Fatalf("unexpected item %d=%q on empty tree", k, v)
	}
}

func TestRangeInclusiveBounds(t *testing.T) {
	b3 := newSeeded(t, []int{10, 20, 30, 40, 50})
	var got []int
	for k := range b3.Range(20, 40) {
		got = append(got, k)
	}
	want := []int{20, 30, 40}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestRangeMissingBounds(t *testing.T) {
	b3 := newSeeded(t, []int{10, 20, 30, 40, 50})
	var got []int
	for k := range b3.Range(15, 45) {
		got = append(got, k)
	}
	want := []int{20, 30, 40}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestRangePastLastKey(t *testing.T) {
	b3 := newSeeded(t, []int{10, 20, 30})
	for k := range b3.Range(35, 99) {
		t.Fatalf("unexpected key %d past last key", k)
	}
}

func TestRangeOnEmptyTree(t *testing.T) {
	b3 := NewBtree[int, string](true)
	for k := range b3.Range(1, 100) {
		t.Fatalf("unexpected key %d on empty tree", k)
	}
}

func TestRangeWholeTreeAndStrings(t *testing.T) {
	b3 := NewBtree[string, int](true)
	for i, k := range []string{"cherry", "apple", "banana"} {
		b3.Add(k, i)
	}
	var got []string
	for k := range b3.Range("a", "z") {
		got = append(got, k)
	}
	want := "[apple banana cherry]"
	if fmt.Sprint(got) != want {
		t.Fatalf("got %v want %s", got, want)
	}
}
