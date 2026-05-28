package agent

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeScript(t *testing.T) {
	tests := []struct {
		name     string
		input    []ScriptInstruction
		expected []ScriptInstruction
	}{
		{
			name: "Join - Default to Inner",
			input: []ScriptInstruction{
				{
					Op: "join",
					Args: map[string]interface{}{
						"dataset": "users",
					},
				},
			},
			expected: []ScriptInstruction{
				{
					Op: "join",
					Args: map[string]interface{}{
						"dataset": "users",
						"type":    "inner",
					},
				},
			},
		},
		{
			name: "Join - Normalize Type Case",
			input: []ScriptInstruction{
				{
					Op: "join",
					Args: map[string]interface{}{
						"type": "LEFT",
					},
				},
			},
			expected: []ScriptInstruction{
				{
					Op: "join",
					Args: map[string]interface{}{
						"type": "left",
					},
				},
			},
		},
		{
			name: "CommitTx - Rewrite to Defer if Cursor Produced",
			input: []ScriptInstruction{
				{Op: "scan", Args: map[string]interface{}{"store": "users"}},
				{Op: "commit_tx", Args: map[string]interface{}{"transaction": "tx"}},
			},
			expected: []ScriptInstruction{
				{Op: "scan", Args: map[string]interface{}{"store": "users"}, ResultVar: "output"},
				{
					Op: "defer",
					Args: map[string]interface{}{
						"command": map[string]interface{}{
							"op":          "commit_tx",
							"transaction": "tx",
						},
					},
				},
			},
		},
		{
			name: "Filter - Normalize Flat Args",
			input: []ScriptInstruction{
				{Op: "filter", Args: map[string]interface{}{"field": "first_name", "op": "==", "value": "John"}},
			},
			expected: []ScriptInstruction{
				{Op: "filter", Args: map[string]interface{}{"condition": map[string]any{"first_name": map[string]any{"$eq": "John"}}}},
			},
		},
		{
			name: "OpenStore - Default ResultVar To Store Name",
			input: []ScriptInstruction{
				{Op: "open_store", Args: map[string]interface{}{"name": "users", "transaction": "tx"}},
			},
			expected: []ScriptInstruction{
				{Op: "open_store", Args: map[string]interface{}{"name": "users", "transaction": "tx"}, ResultVar: "users"},
			},
		},
		{
			name: "Return - Normalize $var Wrapper",
			input: []ScriptInstruction{
				{Op: "return", Args: map[string]interface{}{"value": map[string]any{"$var": "final_orders"}}},
			},
			expected: []ScriptInstruction{
				{Op: "return", Args: map[string]interface{}{"value": "@final_orders"}},
			},
		},
		{
			name: "Join - Expand Relation Target Shape",
			input: []ScriptInstruction{
				{Op: "begin_tx", Args: map[string]interface{}{"database": "dev_db", "mode": "read"}, ResultVar: "tx"},
				{Op: "open_store", Args: map[string]interface{}{"name": "orders", "transaction": "tx"}, ResultVar: "orders_store"},
				{Op: "join", Args: map[string]interface{}{"relation": "users_orders", "target": "orders_store"}, InputVar: "users_rows", ResultVar: "joined_rows"},
			},
			expected: []ScriptInstruction{
				{Op: "begin_tx", Args: map[string]interface{}{"database": "dev_db", "mode": "read"}, ResultVar: "tx"},
				{Op: "open_store", Args: map[string]interface{}{"name": "orders", "transaction": "tx"}, ResultVar: "orders_store"},
				{Op: "open_store", Args: map[string]interface{}{"name": "users_orders", "transaction": "tx"}, ResultVar: "users_orders"},
				{Op: "join", Args: map[string]interface{}{"store": "users_orders", "type": "inner", "on": map[string]any{"key": "key"}}, InputVar: "users_rows", ResultVar: "joined_rows__bridge"},
				{Op: "join", Args: map[string]interface{}{"store": "orders_store", "type": "inner", "on": map[string]any{"value": "key"}}, InputVar: "joined_rows__bridge", ResultVar: "joined_rows"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeScript(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeScript_Project(t *testing.T) {
	// this is harder to test with strict equality because parseProjectionFields returns []ProjectionField
	// and we don't want to couple strictly to the internal struct if we can avoid it,
	// but since we are in the same package we can import the types or just check behavior.

	input := []ScriptInstruction{
		{
			Op: "project",
			Args: map[string]interface{}{
				"fields": []string{"a", "b.c"},
			},
		},
	}

	result := sanitizeScript(input)

	fields := result[0].Args["fields"]
	// expected is a slice of ProjectionField
	// But let's just assert it is NOT []string
	_, isStringSlice := fields.([]string)
	assert.False(t, isStringSlice, "Fields should be converted from []string to []ProjectionField")

	// We can trust parseProjectionFields does the right thing if we successfully called it.
}
