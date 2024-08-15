package btree

import (
	"sort"
	"testing"

	"github.com/SharedCode/sop"
)

func TestSearch(t *testing.T) {
	var l = []int{1, 2, 3, 5, 7, 8}

	found := false
	var v = 4
	i := sort.Search(len(l), func(i int) bool {
		if l[i] == v {
			found = true
		}
		return l[i] >= v
	})
	if found || i != 3 {
		t.Fail()
	}
}

func TestConvertToBlobTableName(t *testing.T) {
	s := "foo_r"
	if sop.ConvertToBlobTableName(s) != "foo_b" {
		t.Errorf("ConvertToBlobTableName(..) failed, got = %s, want = %s.", sop.ConvertToBlobTableName(s), "foo_b")
	}
}
