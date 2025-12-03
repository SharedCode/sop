package jsondb

import (
	"testing"
)

func TestIndexSpecification_Comparer(t *testing.T) {
	// Define index spec: Name (Asc), Age (Desc)
	idxSpec := NewIndexSpecification([]IndexFieldSpecification{
		{FieldName: "name", AscendingSortOrder: true},
		{FieldName: "age", AscendingSortOrder: false},
	})

	tests := []struct {
		name     string
		a        map[string]any
		b        map[string]any
		expected int
	}{
		{
			name:     "Equal items",
			a:        map[string]any{"name": "alice", "age": 30},
			b:        map[string]any{"name": "alice", "age": 30},
			expected: 0,
		},
		{
			name:     "First field diff (Asc) - a < b",
			a:        map[string]any{"name": "alice", "age": 30},
			b:        map[string]any{"name": "bob", "age": 30},
			expected: -1,
		},
		{
			name:     "First field diff (Asc) - a > b",
			a:        map[string]any{"name": "charlie", "age": 30},
			b:        map[string]any{"name": "bob", "age": 30},
			expected: 1,
		},
		{
			name:     "Second field diff (Desc) - a < b (so a > b in desc)",
			a:        map[string]any{"name": "alice", "age": 25},
			b:        map[string]any{"name": "alice", "age": 30},
			expected: 1, // 25 < 30. Compare(25, 30) = -1. Descending: -1 * -1 = 1.
		},
		{
			name:     "Second field diff (Desc) - a > b (so a < b in desc)",
			a:        map[string]any{"name": "alice", "age": 35},
			b:        map[string]any{"name": "alice", "age": 30},
			expected: -1, // 35 > 30. Compare(35, 30) = 1. Descending: 1 * -1 = -1.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := idxSpec.Comparer(tt.a, tt.b)
			if res != tt.expected {
				t.Errorf("Comparer(%v, %v) = %d; want %d", tt.a, tt.b, res, tt.expected)
			}
		})
	}

	// Run again to test lazy initialization (coercedComparer should be set)
	for _, tt := range tests {
		t.Run(tt.name+"_Run2", func(t *testing.T) {
			res := idxSpec.Comparer(tt.a, tt.b)
			if res != tt.expected {
				t.Errorf("Comparer(%v, %v) = %d; want %d", tt.a, tt.b, res, tt.expected)
			}
		})
	}
}
