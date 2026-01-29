package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// TestOpReturn validates the behavior of the "return" opcode.
//
// REGRESSION GUARD (Jan 2026):
// This test is critical. It ensures that the 'return' opcode correctly UNPACKS
// variables referenced by name (e.g., "my_var" or "@my_var") into their actual values (JSON objects/lists).
//
// If this logic regresses, the API will start returning literal strings like "@my_var"
// instead of the data payload, breaking all downstream UI/AI consumers.
func TestOpReturn_VariableResolution(t *testing.T) {
	ctx := context.Background()
	agent := &DataAdminAgent{}

	tests := []struct {
		name        string
		script      []map[string]any
		expectValue string
		failIfFound string
	}{
		{
			name: "Implicit Variable Resolution (no prefix)",
			script: []map[string]any{
				{
					"op": "assign",
					"args": map[string]any{
						"value": map[string]string{"status": "ok_implicit"},
					},
					"result_var": "my_data",
				},
				{
					"op":   "return",
					"args": map[string]any{"value": "my_data"},
				},
			},
			expectValue: "\"status\": \"ok_implicit\"",
			failIfFound: "\"my_data\"",
		},
		{
			name: "Explicit Variable Resolution (@ prefix)",
			script: []map[string]any{
				{
					"op": "assign",
					"args": map[string]any{
						"value": map[string]string{"status": "ok_explicit"},
					},
					"result_var": "my_data",
				},
				{
					"op":   "return",
					"args": map[string]any{"value": "@my_data"},
				},
			},
			expectValue: "\"status\": \"ok_explicit\"",
			failIfFound: "\"@my_data\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scriptJSON, _ := json.Marshal(tt.script)
			res, err := agent.toolExecuteScript(ctx, map[string]any{"script": string(scriptJSON)})
			if err != nil {
				t.Fatalf("toolExecuteScript failed: %v", err)
			}

			if tt.failIfFound != "" && strings.Contains(res, tt.failIfFound) && !strings.Contains(res, tt.expectValue) {
				t.Errorf("Regression: Found forbidden string %s in output: %s", tt.failIfFound, res)
			}

			if !strings.Contains(res, tt.expectValue) {
				t.Errorf("Expected output to contain %s, got: %s", tt.expectValue, res)
			}
		})
	}
}

func TestOpReturn_LiteralValue(t *testing.T) {
	ctx := context.Background()
	agent := &DataAdminAgent{}

	script := []map[string]any{
		{
			"op":   "return",
			"args": map[string]any{"value": "just_a_string"},
		},
	}

	scriptJSON, _ := json.Marshal(script)
	res, err := agent.toolExecuteScript(ctx, map[string]any{"script": string(scriptJSON)})
	if err != nil {
		t.Fatalf("toolExecuteScript failed: %v", err)
	}

	if !strings.Contains(res, "just_a_string") {
		t.Errorf("Expected literal string return, got: %s", res)
	}
}
