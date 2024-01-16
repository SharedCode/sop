package in_red_ck

import (
	"testing"
)

// Add Test_ prefix if you want to run this test.
// It drops the blob & registry tables of the B-Tree, thus, the test was removed from the set.
func DeleteBTree(t *testing.T) {
	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	if err := RemoveBtree(ctx, "fooStore", trans); err != nil {
		t.Error(err)
	}
	// No need to call transaction commit since RemoveBtree is a permanent action.
	// It can't get rolled back. But droping tables is a very rare action, thus, it's fine
	// not being able to roll it back.
}
