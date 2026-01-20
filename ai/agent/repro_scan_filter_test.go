package agent

import (
	"log"
	"testing"
)

func TestReproMaryVersusJohn(t *testing.T) {
	engine := &ScriptEngine{}

	item := map[string]any{
		"first_name": "Mary",
		"id":         "2",
	}

	// Filter asking for John
	filter := map[string]any{
		"first_name": map[string]any{
			"$eq": "John",
		},
	}

	// Should NOT match
	match, err := engine.evaluateCondition(item, filter)
	if err != nil {
		t.Fatalf("Error evaluating: %v", err)
	}

	log.Printf("Item: %+v, Filter: %+v, Match: %v", item, filter, match)

	if match {
		t.Errorf("BUG FOUND: Mary matched filter for John!")
	} else {
		t.Log("Correct: Mary did NOT match filter for John")
	}
}

func TestReproMaryVersusJohn_Implicit(t *testing.T) {
	engine := &ScriptEngine{}

	item := map[string]any{
		"first_name": "Mary",
		"id":         "2",
	}

	// Filter asking for John (Implicit EQ)
	filter := map[string]any{
		"first_name": "John",
	}

	// Should NOT match
	match, err := engine.evaluateCondition(item, filter)
	if err != nil {
		t.Fatalf("Error evaluating: %v", err)
	}

	log.Printf("Item: %+v, Filter: %+v, Match: %v", item, filter, match)

	if match {
		t.Errorf("BUG FOUND: Mary matched filter for John (Implicit)!")
	} else {
		t.Log("Correct: Mary did NOT match filter for John (Implicit)")
	}
}
