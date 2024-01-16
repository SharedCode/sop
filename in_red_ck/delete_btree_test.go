package in_red_ck

import (
	"testing"
)

// Add Test_ prefix if you want to run this test.
// It drops the blob & registry tables of the B-Tree, thus, the test was removed from the set.
func DeleteBTree(t *testing.T) {
	if err := RemoveBtree(ctx, "fooStore"); err != nil {
		t.Error(err)
	}
}
