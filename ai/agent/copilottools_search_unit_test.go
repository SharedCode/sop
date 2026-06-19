package agent

import (
	"testing"
)

func TestExtractHitCategory_PrefersFullPath(t *testing.T) {
	testCases := []struct {
		name     string
		payload  any
		expected string
	}{
		{
			name: "Full category_path available",
			payload: map[string]any{
				"category":      "C#",
				"category_path": "Language Bindings & Tooling/C#",
			},
			expected: "Language Bindings & Tooling/C#",
		},
		{
			name: "Only category available",
			payload: map[string]any{
				"category": "C#",
			},
			expected: "C#",
		},
		{
			name: "Only path available",
			payload: map[string]any{
				"path": "Some/Path",
			},
			expected: "Some/Path",
		},
		{
			name: "Empty category_path falls back to category",
			payload: map[string]any{
				"category":      "C#",
				"category_path": "   ",
			},
			expected: "C#",
		},
		{
			name:     "No category fields",
			payload:  map[string]any{"other": "data"},
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := extractHitCategory(tc.payload)
			if result != tc.expected {
				t.Errorf("extractHitCategory() = %q, want %q", result, tc.expected)
			}
		})
	}
}
