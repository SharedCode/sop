package dynamic

import (
	"testing"

	"github.com/sharedcode/sop"
)

func TestHandle(t *testing.T) {
	idA := sop.NewUUID()
	idB := sop.NewUUID()
	none := sop.NilUUID

	h := NewHandle(idA)
	if h.GetActiveID() != idA {
		t.Errorf("fail")
	}
	if h.GetInActiveID() != none {
		t.Errorf("fail")
	}
	if h.IsAandBinUse() {
		t.Errorf("fail")
	}
	if !h.HasID(idA) {
		t.Errorf("fail")
	}

	h.PhysicalIDB = idB
	if !h.IsAandBinUse() {
		t.Errorf("fail")
	}

	h.FlipActiveID()
	if h.GetActiveID() != idB {
		t.Errorf("fail")
	}
	if h.GetInActiveID() != idA {
		t.Errorf("fail")
	}

	h.ClearInactiveID()
	if h.PhysicalIDA != none {
		t.Errorf("fail")
	}
	h.FlipActiveID()
	h.ClearInactiveID()
	if h.PhysicalIDB != none {
		t.Errorf("fail")
	}

	emptyHandle := Handle{}
	if !emptyHandle.IsEmpty() {
		t.Errorf("fail")
	}
	if !h.IsEqual(&h) {
		t.Errorf("fail")
	}
}
