package sop

import (
	"testing"
	"time"
)

func fixedNow(t *testing.T, at time.Time) {
	t.Helper()
	old := Now
	Now = func() time.Time { return at }
	t.Cleanup(func() { Now = old })
}

func TestNewHandle(t *testing.T) {
	id := NewUUID()
	h := NewHandle(id)
	if h.LogicalID != id || h.PhysicalIDA != id {
		t.Fatalf("NewHandle did not seed logical and A ids: %+v", h)
	}
	if !h.PhysicalIDB.IsNil() || h.IsActiveIDB || h.IsDeleted || h.Version != 0 {
		t.Fatalf("NewHandle left unexpected state: %+v", h)
	}
}

func TestHandle_ActiveAndInactiveID(t *testing.T) {
	a, b := NewUUID(), NewUUID()
	h := Handle{PhysicalIDA: a, PhysicalIDB: b}
	if h.GetActiveID() != a || h.GetInActiveID() != b {
		t.Fatal("with A active, active must be A and inactive B")
	}
	h.FlipActiveID()
	if h.GetActiveID() != b || h.GetInActiveID() != a {
		t.Fatal("after flip, active must be B and inactive A")
	}
	h.FlipActiveID()
	if h.IsActiveIDB {
		t.Fatal("double flip must return to A")
	}
}

func TestHandle_IsAandBinUse(t *testing.T) {
	a, b := NewUUID(), NewUUID()
	tests := []struct {
		name string
		h    Handle
		want bool
	}{
		{"neither", Handle{}, false},
		{"only A", Handle{PhysicalIDA: a}, false},
		{"only B", Handle{PhysicalIDB: b}, false},
		{"both", Handle{PhysicalIDA: a, PhysicalIDB: b}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.h.IsAandBinUse(); got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHandle_AllocateID(t *testing.T) {
	at := time.UnixMilli(1_700_000_000_000)
	fixedNow(t, at)

	t.Run("A active allocates into B", func(t *testing.T) {
		h := NewHandle(NewUUID())
		id := h.AllocateID()
		if id.IsNil() || h.PhysicalIDB != id {
			t.Fatalf("expected new id in B, got id=%v h=%+v", id, h)
		}
		if h.WorkInProgressTimestamp != at.UnixMilli() {
			t.Fatalf("WIP timestamp = %d, want %d", h.WorkInProgressTimestamp, at.UnixMilli())
		}
	})

	t.Run("B active allocates into A", func(t *testing.T) {
		h := Handle{PhysicalIDB: NewUUID(), IsActiveIDB: true}
		id := h.AllocateID()
		if id.IsNil() || h.PhysicalIDA != id {
			t.Fatalf("expected new id in A, got id=%v h=%+v", id, h)
		}
	})

	t.Run("both slots used returns nil and leaves handle untouched", func(t *testing.T) {
		h := Handle{PhysicalIDA: NewUUID(), PhysicalIDB: NewUUID()}
		before := h
		if id := h.AllocateID(); !id.IsNil() {
			t.Fatalf("expected NilUUID, got %v", id)
		}
		if h != before {
			t.Fatalf("handle mutated: before=%+v after=%+v", before, h)
		}
	})
}

func TestHandle_IsExpiredInactive(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	fixedNow(t, now)
	tests := []struct {
		name string
		ts   int64
		want bool
	}{
		{"unset never expires", 0, false},
		{"fresh", now.UnixMilli(), false},
		{"just inside the hour", now.Add(-59 * time.Minute).UnixMilli(), false},
		{"older than an hour", now.Add(-61 * time.Minute).UnixMilli(), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := Handle{WorkInProgressTimestamp: tt.ts}
			if got := h.IsExpiredInactive(); got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHandle_HasID(t *testing.T) {
	a, b, other := NewUUID(), NewUUID(), NewUUID()
	h := Handle{PhysicalIDA: a, PhysicalIDB: b}
	if !h.HasID(a) || !h.HasID(b) || h.HasID(other) {
		t.Fatal("HasID must match A or B only")
	}
}

func TestHandle_ClearInactiveID(t *testing.T) {
	a, b := NewUUID(), NewUUID()

	h := Handle{PhysicalIDA: a, PhysicalIDB: b, WorkInProgressTimestamp: 5}
	h.ClearInactiveID()
	if h.PhysicalIDA != a || !h.PhysicalIDB.IsNil() || h.WorkInProgressTimestamp != 0 {
		t.Fatalf("A active: expected B cleared, got %+v", h)
	}

	h = Handle{PhysicalIDA: a, PhysicalIDB: b, IsActiveIDB: true, WorkInProgressTimestamp: 5}
	h.ClearInactiveID()
	if !h.PhysicalIDA.IsNil() || h.PhysicalIDB != b || h.WorkInProgressTimestamp != 0 {
		t.Fatalf("B active: expected A cleared, got %+v", h)
	}
}

func TestHandle_IsEmpty(t *testing.T) {
	if h := (Handle{}); !h.IsEmpty() {
		t.Fatal("zero handle must be empty")
	}
	for name, h := range map[string]Handle{
		"logical":   {LogicalID: NewUUID()},
		"a":         {PhysicalIDA: NewUUID()},
		"b":         {PhysicalIDB: NewUUID()},
		"deleted":   {IsDeleted: true},
		"version":   {Version: 1},
		"timestamp": {WorkInProgressTimestamp: 1},
	} {
		if h.IsEmpty() {
			t.Errorf("%s set: handle must not be empty", name)
		}
	}
}

func TestHandle_IsEqual(t *testing.T) {
	id := NewUUID()
	x := Handle{LogicalID: id, PhysicalIDA: id, Version: 1}
	y := Handle{LogicalID: id, PhysicalIDA: id, Version: 9}
	if !x.IsEqual(&y) {
		t.Fatal("Version must be ignored")
	}
	y.IsDeleted = true
	if x.IsEqual(&y) {
		t.Fatal("IsDeleted must be compared")
	}
	y = Handle{LogicalID: id, PhysicalIDA: id, PhysicalIDB: NewUUID()}
	if x.IsEqual(&y) {
		t.Fatal("PhysicalIDB must be compared")
	}
}

func TestExtractLogicalIDs(t *testing.T) {
	l1, l2 := NewUUID(), NewUUID()
	in := []RegistryPayload[Handle]{
		{RegistryTable: "t1", CacheDuration: time.Minute, IDs: []Handle{{LogicalID: l1}, {LogicalID: l2}}},
		{RegistryTable: "t2"},
	}
	out := ExtractLogicalIDs(in)
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2", len(out))
	}
	if out[0].RegistryTable != "t1" || out[0].CacheDuration != time.Minute ||
		len(out[0].IDs) != 2 || out[0].IDs[0] != l1 || out[0].IDs[1] != l2 {
		t.Fatalf("unexpected first payload: %+v", out[0])
	}
	if out[1].RegistryTable != "t2" || len(out[1].IDs) != 0 {
		t.Fatalf("unexpected second payload: %+v", out[1])
	}
	if got := ExtractLogicalIDs(nil); len(got) != 0 {
		t.Fatalf("nil input must yield empty output, got %v", got)
	}
}
