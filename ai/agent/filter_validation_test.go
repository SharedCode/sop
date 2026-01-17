package agent

import (
	"strings"
	"testing"
)

func TestEvaluateCondition_Validation(t *testing.T) {
	engine := &ScriptEngine{}

	// Data item
	item := map[string]any{
		"first_name": "John",
		"age":        30,
	}

	// Case 1: Valid Condition
	condValid := map[string]any{
		"first_name": "John",
	}
	match, err := engine.evaluateCondition(item, condValid)
	if err != nil {
		t.Errorf("Expected no error for valid condition, got: %v", err)
	}
	if !match {
		t.Error("Expected match for valid condition")
	}

	// Case 2: Invalid Field (Hallucination)
	condInvalid := map[string]any{
		"name": "John",
	}
	_, err = engine.evaluateCondition(item, condInvalid)
	if err == nil {
		t.Error("Expected error for non-existent field 'name'")
	} else {
		// Verify error message
		t.Logf("Got expected error: %v", err)
		if !strings.Contains(err.Error(), "Did you mean 'first_name'?") {
			t.Errorf("Error message should contain suggestion. Got: %v", err)
		}
	}

	// Case 3: Completely wrong field
	condWrong := map[string]any{
		"salary": 50000,
	}
	_, err = engine.evaluateCondition(item, condWrong)
	if err == nil {
		t.Error("Expected error for non-existent field 'salary'")
	} else {
		t.Logf("Got expected error: %v", err)
		if !strings.Contains(err.Error(), "Available fields: [age first_name]") {
			t.Errorf("Error message should list available fields. Got: %v", err)
		}
	}
}
