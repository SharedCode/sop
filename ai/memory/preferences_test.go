package memory

import (
	"encoding/json"
	"testing"
)

func TestNewBoolPreference_PreservesTypedVerboseValue(t *testing.T) {
	pref := NewBoolPreference(PreferenceKeyVerbose, true)

	if pref.Key != PreferenceKeyVerbose {
		t.Fatalf("expected verbose preference key, got %q", pref.Key)
	}
	value, ok := pref.Bool()
	if !ok || !value {
		t.Fatalf("expected typed bool preference to resolve true, got value=%v ok=%v", value, ok)
	}
}

func TestPreference_JSONRoundTripPreservesBoolLane(t *testing.T) {
	original := NewBoolPreference(PreferenceKeyVerbose, false)
	original.UpdatedAtUTC = 1710000000
	original.Source = "user_toggle"

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal preference: %v", err)
	}

	var decoded Preference
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal preference: %v", err)
	}

	value, ok := decoded.Bool()
	if !ok {
		t.Fatal("expected bool lane to survive JSON round-trip")
	}
	if value {
		t.Fatalf("expected verbose=false after round-trip, got true")
	}
	if decoded.Key != PreferenceKeyVerbose {
		t.Fatalf("expected verbose key after round-trip, got %q", decoded.Key)
	}
	if decoded.Source != "user_toggle" {
		t.Fatalf("expected source to survive round-trip, got %q", decoded.Source)
	}
}
