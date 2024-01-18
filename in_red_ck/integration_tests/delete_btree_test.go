package in_red_ck

import (
	"testing"

	"github.com/SharedCode/sop/in_red_ck"
)

// Add Test_ prefix if you want to run this test.
// It drops the blob & registry tables of the B-Tree, thus, the test was removed from the set.
func DeleteBTree(t *testing.T) {
	if err := in_red_ck.RemoveBtree(ctx, "fooStore"); err != nil {
		t.Error(err)
	}
	if err := in_red_ck.RemoveBtree(ctx, "persondb"); err != nil {
		t.Error(err)
	}
}
