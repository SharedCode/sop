package integration_tests

import (
	"testing"

	"github.com/SharedCode/sop/in_red_ck"
)

// Add Test_ prefix if you want to run this test.
// It drops the blob & registry tables of the B-Tree, thus, the test was removed from the set.
func TestDeleteBTree(t *testing.T) {
	if err := in_red_ck.RemoveBtree(ctx, "fooStore"); err != nil {
		t.Error(err)
	}
	if err := in_red_ck.RemoveBtree(ctx, "persondb"); err != nil {
		t.Error(err)
	}
	if err := in_red_ck.RemoveBtree(ctx, "twophase"); err != nil {
		t.Error(err)
	}
	if err := in_red_ck.RemoveBtree(ctx, "twophase2"); err != nil {
		t.Error(err)
	}
	if err := in_red_ck.RemoveBtree(ctx, "persondb7"); err != nil {
		t.Error(err)
	}
}
