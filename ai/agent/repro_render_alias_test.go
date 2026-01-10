package agent

import (
	"encoding/json"
	"testing"
)

func TestRenderItem_Alias_CaseSensitivity(t *testing.T) {
	// 1. Setup Data
	key := map[string]any{"id": 1}
	val := map[string]any{"name": "John", "age": 30}

	// 2. Test Cases
	tests := []struct {
		field    string
		expected string // Expected output key
	}{
		{"name AS employee", "employee"},
		{"name as employee", "employee"},
		{"name As employee", "employee"},
		{"name aS employee", "employee"},
		{"name   as   employee", "employee"},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			fields := []string{tt.field}
			res := renderItem(key, val, fields)

			// renderItem returns *OrderedMap or map
			bytes, _ := json.Marshal(res)
			var m map[string]any
			json.Unmarshal(bytes, &m)

			if _, ok := m[tt.expected]; !ok {
				t.Errorf("Field '%s' failed. Result keys: %v. Expected key: '%s'", tt.field, m, tt.expected)
			}
			// Check that original key doesn't exist (unless it's the same)
			if tt.expected != "name" {
				if _, ok := m["name"]; ok {
					t.Errorf("Original field 'name' leaked into result: %v", m)
				}
			}
		})
	}
}
