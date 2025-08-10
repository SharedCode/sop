package btree

import (
    "testing"
)

// Cover find() stopping on left-nil child (nearest neighbor positioning)
func TestFind_StopsOnNilLeftChild_PositionsNearest(t *testing.T) {
    b, _ := newTestBtree[string]()
    // Insert a few items; tree may remain a single node but logic still applies
    for _, k := range []int{10, 20, 30} { ok, err := b.Add(nil, k, "v"); if err != nil || !ok { t.Fatalf("add %d", k) } }
    // Search for key < 10 to hit nearest-neighbor logic
    ok, err := b.Find(nil, 5, true)
    if err != nil || ok { t.Fatalf("expected false,nil for not-found with nearest positioning, got ok=%v err=%v", ok, err) }
    // After not found, current should be the next-greater (10)
    if it, _ := b.GetCurrentItem(nil); it.Key != 10 { t.Fatalf("expected current=10, got %d", it.Key) }
}
