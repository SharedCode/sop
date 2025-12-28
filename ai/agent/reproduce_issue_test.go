package agent

import (
	"testing"
)

func TestMatchesKey_Reproduction(t *testing.T) {
	// Case 1: Primitive Key vs Map Filter (Mismatch)
	// Key is UUID (string), Filter is {"Region": "APAC"}
	key := "some-uuid"
	filter := map[string]any{"Region": "APAC"}
	matched, _ := matchesKey(key, filter)
	if matched {
		t.Errorf("Case 1 Failed: Expected false, got true. Key=%v, Filter=%v", key, filter)
	}

	// Case 2: Map Value vs Map Filter (Mismatch)
	// Value is {"Region": "US"}, Filter is {"Region": "APAC"}
	val := map[string]any{"Region": "US"}
	filter2 := map[string]any{"Region": "APAC"}
	matched2, _ := matchesKey(val, filter2)
	if matched2 {
		t.Errorf("Case 2 Failed: Expected false, got true. Val=%v, Filter=%v", val, filter2)
	}

	// Case 3: Map Value vs Map Filter (Match)
	val3 := map[string]any{"Region": "APAC"}
	matched3, _ := matchesKey(val3, filter2)
	if !matched3 {
		t.Errorf("Case 3 Failed: Expected true, got false. Val=%v, Filter=%v", val3, filter2)
	}
}
