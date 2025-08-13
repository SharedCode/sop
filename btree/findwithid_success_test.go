package btree

import (
	"github.com/sharedcode/sop"
	"testing"
)

// Ensure FindWithID returns true when duplicates exist and one matches later.
func TestFindWithID_Duplicates_Success(t *testing.T) {
	b, _ := newTestBtree[string]()
	// Allow duplicates for this test
	b.StoreInfo.IsUnique = false
	v := "v"
	vv := v
	a := sop.NewUUID()
	b1 := sop.NewUUID()
	c := sop.NewUUID()
	// Insert three duplicates; ensure the match is the middle to force iteration
	if _, err := b.AddItem(nil, &Item[int, string]{Key: 10, Value: &vv, ID: a}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AddItem(nil, &Item[int, string]{Key: 10, Value: &vv, ID: b1}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.AddItem(nil, &Item[int, string]{Key: 10, Value: &vv, ID: c}); err != nil {
		t.Fatal(err)
	}

	if ok, err := b.FindWithID(nil, 10, b1); !ok || err != nil {
		t.Fatalf("expected true,nil matching middle duplicate, got ok=%v err=%v", ok, err)
	}
}
