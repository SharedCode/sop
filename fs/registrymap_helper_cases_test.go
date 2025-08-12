package fs

import (
	"testing"

	"github.com/sharedcode/sop"
)

// TestGetIDsHelper covers the small helper that extracts logical IDs from handles.
func TestGetIDsHelper(t *testing.T) {
	hs := []sop.Handle{{LogicalID: sop.NewUUID()}, {LogicalID: sop.NewUUID()}, {LogicalID: sop.NewUUID()}}
	ids := getIDs(hs)
	if len(ids) != len(hs) {
		t.Fatalf("len mismatch")
	}
	for i := range hs {
		if ids[i] != hs[i].LogicalID {
			t.Fatalf("id mismatch at %d", i)
		}
	}
}
