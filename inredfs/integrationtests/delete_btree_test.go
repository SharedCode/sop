package integrationtests

import (
	"context"
	"testing"

	"github.com/sharedcode/sop/inredfs"
)

// Add Test_ prefix if you want to run this test.
// It drops the blob & registry tables of the B-Tree, thus, the test was removed from the set.
func DeleteBTree(t *testing.T) {
	ctx := context.Background()
	tableList := []string{
		"fooStore", "fooStore1", "fooStore2", "persondb", "twophase", "twophase2", "twophase3",
		"twophase22", "persondb7", "persondb77", "person2db", "barStore1",
		"barStore2", "tabley", "tablex2", "tablex", "ztab1", "videoStore",
		"videoStoreM", "videoStoreD", "videoStoreU", "xyz", "emptystore",
		"videoStore2", "videoStore3", "videoStore4", "videoStore5", "baStore", "barstoreec",
		"emptyStore", "barstore2", "barstore1", "storecaching", "storecachingttl", "personvdb7",
		"regnotcached",
	}

	for _, tn := range tableList {
		if err := inredfs.RemoveBtree(ctx, dataPath, tn); err != nil {
			t.Error(err)
		}
	}
}
