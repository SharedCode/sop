package btree

import (
	"github.com/sharedcode/sop"
	"testing"
)

// Duplicates exist but the requested ID is never matched; ensure FindWithID returns false.
func TestFindWithID_Duplicates_NotFound(t *testing.T) {
	b, _ := newTestBtree[string]()
	// Insert duplicate keys with different IDs using AddItem to bypass uniqueness check
	v := "v"
	vv := v
	for i := 0; i < 3; i++ {
		_, err := b.AddItem(nil, &Item[int, string]{Key: 10, Value: &vv, ID: sop.NewUUID()})
		if err != nil {
			t.Fatalf("add dup %d: %v", i, err)
		}
	}
	// Pick a random non-existent ID
	missing := sop.NewUUID()
	if ok, err := b.FindWithID(nil, 10, missing); err != nil || ok {
		t.Fatalf("expected false,nil when ID not found among duplicates")
	}
}
