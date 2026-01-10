package agent

import (
	"testing"

	"github.com/sharedcode/sop/jsondb"
)

func TestRightSortOptimization_Valid(t *testing.T) {
	jp := &JoinProcessor{
		// Index: [f1 ASC, f2 DESC]
		rightIndexSpec: &jsondb.IndexSpecification{
			IndexFields: []jsondb.IndexFieldSpecification{
				{FieldName: "f1", AscendingSortOrder: true},
				{FieldName: "f2", AscendingSortOrder: false},
			},
		},
		// Join on f1
		rightKeyFields: []string{"f1"},
		rightKeyFieldMap: map[string]string{"f1": "f1"},
		// Sort by f2 DESC
		rightSortFields: []string{"f2 DESC"},
	}
	
	jp.checkRightSortOptimization()
	
	if !jp.isRightSortOptimized {
		t.Error("Expected optimization to be ENABLED")
	}
}

func TestRightSortOptimization_Valid_WithPrefix(t *testing.T) {
	jp := &JoinProcessor{
		// Index: [f1 ASC, f2 DESC]
		rightIndexSpec: &jsondb.IndexSpecification{
			IndexFields: []jsondb.IndexFieldSpecification{
				{FieldName: "f1", AscendingSortOrder: true},
				{FieldName: "f2", AscendingSortOrder: false},
			},
		},
		// Join on f1 (user alias "a")
		rightKeyFields: []string{"a"},
		rightKeyFieldMap: map[string]string{"a": "f1"},
		// Sort by "b.f2 DESC"
		rightSortFields: []string{"b.f2 DESC"},
	}
	
	jp.checkRightSortOptimization()
	
	if !jp.isRightSortOptimized {
		t.Error("Expected optimization to be ENABLED with stripped prefix")
	}
}

func TestRightSortOptimization_Invalid_Direction(t *testing.T) {
	jp := &JoinProcessor{
		rightIndexSpec: &jsondb.IndexSpecification{
			IndexFields: []jsondb.IndexFieldSpecification{
				{FieldName: "f1", AscendingSortOrder: true},
				{FieldName: "f2", AscendingSortOrder: true}, // ASC in Index
			},
		},
		rightKeyFields: []string{"f1"},
		rightKeyFieldMap: map[string]string{"f1": "f1"},
		// Sort by f2 DESC (Mismatch)
		rightSortFields: []string{"f2 DESC"},
	}
	
	jp.checkRightSortOptimization()
	
	if jp.isRightSortOptimized {
		t.Error("Expected optimization to be DISABLED (Direction Mismatch)")
	}
}

func TestRightSortOptimization_Invalid_Gap(t *testing.T) {
	jp := &JoinProcessor{
		rightIndexSpec: &jsondb.IndexSpecification{
			IndexFields: []jsondb.IndexFieldSpecification{
				{FieldName: "f1", AscendingSortOrder: true},
				{FieldName: "f2", AscendingSortOrder: true},
				{FieldName: "f3", AscendingSortOrder: true},
			},
		},
		rightKeyFields: []string{"f1"},
		rightKeyFieldMap: map[string]string{"f1": "f1"},
		// Sort by f3 (Skip f2)
		rightSortFields: []string{"f3 ASC"},
	}
	
	jp.checkRightSortOptimization()
	
	if jp.isRightSortOptimized {
		t.Error("Expected optimization to be DISABLED (Field Gap)")
	}
}
