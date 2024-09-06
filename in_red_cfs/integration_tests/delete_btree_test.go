package integration_tests

import (
	"testing"

	"github.com/SharedCode/sop/in_red_cfs"
)

// Add Test_ prefix if you want to run this test.
// It drops the blob & registry tables of the B-Tree, thus, the test was removed from the set.
func TestDeleteBTree(t *testing.T) {
	tableList := []string{
		"fooStore", "persondb", "twophase", "twophase2", "twophase3",
		"twophase22", "persondb7", "persondb77", "person2db", "barStore1",
		"barStore2", "tabley", "tablex2", "tablex", "ztab1", "videoStore",
		"videoStoreM", "videoStoreD", "videoStoreU", "xyz", "emptystore",
		"videoStore2", "videoStore3", "videoStore4", "videoStore5",
		"emptyStore", "barstore2", "barstore1",
	}

	for _, tn := range tableList {
		if err := in_red_cfs.RemoveBtree(ctx, tn); err != nil {
			t.Error(err)
		}
	}
}
