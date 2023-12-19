package btree

import (
	"encoding/json"
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

func TestUUIDConversion(t *testing.T) {
	want := NewUUID()
	suuid := want.ToString()
	if got := ToUUID(suuid); got != want {
		t.Errorf("ToUUID(suuid) failed, got = %v, want = %v.", got, want)
	}
}

func TestItemMarshallingBetweenInterfaceAndGenerics(t *testing.T) {
	foobar := "foobar"
	vd := Item[int, string]{
		Key:   1,
		Value: &foobar,
	}
	ba, _ := json.Marshal(vd)
	var obj Item[interface{}, interface{}]
	json.Unmarshal(ba, &obj)

	obj.SetUpsertTime()
	expectedTIme := obj.GetUpsertTime()

	ba2, _ := json.Marshal(obj)

	var item2 Item[int, string]
	json.Unmarshal(ba2, &item2)

	if item2.Key != 1 || *item2.Value != foobar || item2.UpsertTime != expectedTIme {
		t.Errorf("UpsertTime Item[TK,TV] failed to marshall back and forth.")
	}
}

func TestItemAndNodeMarshallingToVersionedData(t *testing.T) {
	n := Node[int, string]{
		Count: 7,
	}
	var obj interface{} = &n
	vd := obj.(TimestampedData)
	vd.SetUpsertTime()
	upsertTime := vd.GetUpsertTime()
	t.Logf("upsertTime %d", upsertTime)

	ba, _ := json.Marshal(n)
	var n2 interface{} = &Node[interface{}, interface{}]{}
	json.Unmarshal(ba, n2)
	intf := n2.(TimestampedData)
	intf.SetUpsertTime()
	upsertTime = intf.GetUpsertTime()
	t.Logf("upsertTime n2 %d", upsertTime)
}
