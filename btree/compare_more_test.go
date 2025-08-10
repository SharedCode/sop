package btree

import (
    "testing"
)

type customCmp int

func (customCmp) Compare(other interface{}) int {
    return -1 // arbitrary
}

// Verify Btree.compare uses the provided comparer over coerced comparer.
func TestBtreeCompare_CustomComparer(t *testing.T) {
    b, _ := newTestBtree[string]()
    // overwrite comparer
    b.comparer = func(a, b int) int { if a < b { return -1 } else if a > b { return 1 } else { return 0 } }
    // quick sanity: 1 < 2
    if got := b.compare(1, 2); got != -1 { t.Fatalf("custom comparer not used, got %d", got) }
}
