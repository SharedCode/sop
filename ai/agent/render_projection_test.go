package agent

import (
	"testing"
)

func TestRenderItemExcludesValue(t *testing.T) {
	// Source item
	item := map[string]interface{}{
		"name":  "John",
		"age":   30,
		"value": "some-id", // System artifact
		"key":   "some-key",
	}

	// Case 1: Wildcard "*"
	// Test implicit flatten (fields=nil)
	renderItem(nil, item, nil)

	// Test explicit projection ["*"]
	res2 := renderItem(nil, item, []string{"*"})

	// Check res2
	if rMap, ok := res2.(*OrderedMap); ok {
		hasValue := false
		for _, k := range rMap.keys {
			if k == "value" {
				hasValue = true
			}
		}
		// Expectation: hasValue should be FALSE now
		if hasValue {
			t.Errorf("Failed: 'value' is still present in wildcard projection.")
		}
	} else {
		t.Fatalf("Expected OrderedMap, got %T", res2)
	}

	// Test explicit projection ["name", "value"] -> should keep it
	res3 := renderItem(nil, item, []string{"name", "value"})
	if rMap, ok := res3.(*OrderedMap); ok {
		hasValue := false
		for _, k := range rMap.keys {
			if k == "value" {
				hasValue = true
			}
		}
		if !hasValue {
			t.Errorf("Explicit projection of 'value' failed")
		}
	}
}

func TestRenderItemPrimitiveValue(t *testing.T) {
	// Case 1: Primitive Integer
	val := 123
	// Explicit projection ["*"]
	res := renderItem(nil, val, []string{"*"})

	if rMap, ok := res.(*OrderedMap); ok {
		// We expect {"value": 123} because it's the only data we have.
		if len(rMap.keys) == 0 {
			t.Errorf("Primitive integer 123 rendered as empty map with wildcard projection")
		} else {
			// Check content
			if v, ok := rMap.m["value"]; ok {
				if v != 123 {
					t.Errorf("Expected 123, got %v", v)
				}
			} else {
				t.Errorf("Key 'value' missing from result fields: %v", rMap.keys)
			}
		}
	} else {
		t.Fatalf("Expected *OrderedMap, got %T", res)
	}

	// Case 2: Primitive String
	valStr := "hello"
	resStr := renderItem(nil, valStr, []string{"*"})
	if rMap, ok := resStr.(*OrderedMap); ok {
		if len(rMap.keys) == 0 {
			t.Errorf("Primitive string 'hello' rendered as empty map")
		}
	}

	// Case 3: Map with "value" (The case we wanted to fix)
	// {"id": 1, "value": 1} -> Should exclude value
	itemMap := map[string]any{"id": 1, "value": 1}
	resMap := renderItem(nil, itemMap, []string{"*"})
	if rMap, ok := resMap.(*OrderedMap); ok {
		hasValue := false
		hasID := false
		for _, k := range rMap.keys {
			if k == "value" {
				hasValue = true
			}
			if k == "id" {
				hasID = true
			}
		}
		if hasValue {
			t.Errorf("Regression: Map with explicit 'value' key should have it excluded in wildcard")
		}
		if !hasID {
			t.Errorf("Map with 'id' key should have it included")
		}
	}
}
