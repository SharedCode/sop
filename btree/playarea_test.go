package btree

import (
	"sort"
	"testing"
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
	if ConvertToBlobTableName(s) != "foo_b" {
		t.Errorf("ConvertToBlobTableName(..) failed, got = %s, want = %s.", ConvertToBlobTableName(s), "foo_b")
	}
}

func TestComparer(t *testing.T) {
	if compare(int(1), int(1)) == 0 &&
		compare(int8(1), int8(1)) == 0 &&
		compare(int16(1), int16(1)) == 0 &&
		compare(int32(1), int32(1)) == 0 &&
		compare(int64(1), int64(1)) == 0 &&
		compare(uint(1), uint(1)) == 0 &&
		compare(uint8(1), uint8(1)) == 0 &&
		compare(uint16(1), uint16(1)) == 0 &&
		compare(uint32(1), uint32(1)) == 0 &&
		compare(uint64(1), uint64(1)) == 0 &&
		compare(uintptr(1), uintptr(1)) == 0 &&
		compare(float32(1), float32(1)) == 0 &&
		compare(float64(1), float64(1)) == 0 &&
		compare("foo", "foo") == 0 {
		return
	}
	t.Error("Failed comparer test.")
}
