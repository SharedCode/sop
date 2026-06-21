package sop

import (
	"reflect"
	"testing"
)

func TestNewStoreInfo_PreservesCustomData(t *testing.T) {
	customData := map[string]any{"feature": "x", "enabled": true, "count": 2}

	got := NewStoreInfo(StoreOptions{Name: "demo", SlotLength: 4, CustomData: customData})
	if got == nil {
		t.Fatal("expected store info, got nil")
	}

	if !reflect.DeepEqual(got.CustomData, customData) {
		t.Fatalf("expected custom data %v, got %v", customData, got.CustomData)
	}
}

func TestStoreInfo_CustomDataHelpers(t *testing.T) {
	si := &StoreInfo{}

	if _, ok := si.GetCustomData("missing"); ok {
		t.Fatal("expected missing custom data to be absent")
	}

	si.SetCustomData("feature", "enabled")
	val, ok := si.GetCustomData("feature")
	if !ok || val != "enabled" {
		t.Fatalf("expected custom data to be stored, got %v, ok=%v", val, ok)
	}

	if !si.DeleteCustomData("feature") {
		t.Fatal("expected delete to succeed")
	}
	if _, ok := si.GetCustomData("feature"); ok {
		t.Fatal("expected custom data to be deleted")
	}

	si.SetCustomDataMap(map[string]any{"a": 1, "b": 2})
	data := si.GetCustomDataMap()
	if len(data) != 2 || data["a"] != 1 || data["b"] != 2 {
		t.Fatalf("unexpected custom data map: %v", data)
	}
}

func TestStoreInfo_IsCompatible_CustomData(t *testing.T) {
	a := StoreInfo{}
	b := StoreInfo{}
	if !a.IsCompatible(b) {
		t.Fatal("expected empty stores to be compatible")
	}

	a.SetCustomDataMap(map[string]any{"mode": "fast"})
	if !a.IsCompatible(b) {
		t.Fatal("expected custom data to be ignored for compatibility")
	}

	b.SetCustomDataMap(map[string]any{"mode": "slow"})
	if !a.IsCompatible(b) {
		t.Fatal("expected custom data differences to be ignored for compatibility")
	}
}
