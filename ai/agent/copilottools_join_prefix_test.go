package agent

import (
	"context"
	"testing"

	"github.com/sharedcode/sop/jsondb"
)

func TestValidateIndexPrefix_SkipFirst(t *testing.T) {
	// Scenario: Index is on [A, B]. User joins on [B].
	// Expectation: Lookup should be FALSE.

	jp := &JoinProcessor{
		ctx:              context.Background(),
		rightKeyFields:   []string{"b"},
		rightKeyFieldMap: map[string]string{"b": "B"},
	}

	jp.rightIndexSpec = &jsondb.IndexSpecification{
		IndexFields: []jsondb.IndexFieldSpecification{
			{FieldName: "A"}, // Index [0]
			{FieldName: "B"}, // Index [1]
		},
	}

	if jp.validateIndexPrefix() {
		t.Errorf("validateIndexPrefix() = true, expected false (Skipping First Field)")
	}
}

func TestValidateIndexPrefix_Partial(t *testing.T) {
	// Scenario: Index is on [A, B]. User joins on [A].
	// Expectation: Lookup should be TRUE.

	jp := &JoinProcessor{
		ctx:              context.Background(),
		rightKeyFields:   []string{"a"},
		rightKeyFieldMap: map[string]string{"a": "A"},
	}

	jp.rightIndexSpec = &jsondb.IndexSpecification{
		IndexFields: []jsondb.IndexFieldSpecification{
			{FieldName: "A"}, // Index [0]
			{FieldName: "B"}, // Index [1]
		},
	}

	if !jp.validateIndexPrefix() {
		t.Errorf("validateIndexPrefix() = false, expected true (Partial Prefix)")
	}
}

func TestValidateIndexPrefix_CaseInsensitive(t *testing.T) {
	// Scenario: Index is on [A]. User joins on [a]. Map {"a": "A"}.
	// Expectation: Lookup should be TRUE.

	jp := &JoinProcessor{
		ctx:              context.Background(),
		rightKeyFields:   []string{"a"},
		rightKeyFieldMap: map[string]string{"a": "A"},
	}

	jp.rightIndexSpec = &jsondb.IndexSpecification{
		IndexFields: []jsondb.IndexFieldSpecification{
			{FieldName: "A"},
		},
	}

	if !jp.validateIndexPrefix() {
		t.Errorf("validateIndexPrefix() = false, expected true (Case Insensitive Match)")
	}
}
