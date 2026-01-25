package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestDataAdminAgent_Ask_SlashCommand(t *testing.T) {
	// Setup Agent with NO databases
	cfg := Config{
		EnableObfuscation: false,
	}
	agent := NewDataAdminAgent(cfg, nil, nil)
	ctx := context.Background()
	agent.Open(ctx)

	// Inject session payload which is required by Execute
	payload := &ai.SessionPayload{
		CurrentDB: "system",
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	// Case 1: Slash Command with simple args
	// /list_databases
	resp, err := agent.Ask(ctx, "/list_databases")
	if err != nil {
		t.Fatalf("Ask with /list_databases failed: %v", err)
	}

	if strings.Contains(resp, "No valid API Key found") {
		t.Errorf("Slash command triggered 'No API Key' error. Logic failed.")
	}
}

func TestParseSlashCommand(t *testing.T) {
	tests := []struct {
		input    string
		wantTool string
		wantArgs map[string]string
	}{
		{
			input:    "tool key=value",
			wantTool: "tool",
			wantArgs: map[string]string{"key": "value"},
		},
		{
			input:    "tool key=\"value with spaces\"",
			wantTool: "tool",
			wantArgs: map[string]string{"key": "value with spaces"},
		},
		{
			input:    "tool key=\"nested \\\"quote\\\"\"",
			wantTool: "tool",
			wantArgs: map[string]string{"key": "nested \"quote\""},
		},
	}

	for _, tt := range tests {
		gotTool, gotArgs, err := parseSlashCommand(tt.input)
		if err != nil {
			t.Errorf("parseSlashCommand(%q) returned error: %v", tt.input, err)
			continue
		}
		if gotTool != tt.wantTool {
			t.Errorf("parseSlashCommand(%q) tool = %q, want %q", tt.input, gotTool, tt.wantTool)
		}

		for k, v := range tt.wantArgs {
			valStr, _ := gotArgs[k].(string)
			if valStr != v {
				t.Errorf("parseSlashCommand(%q) arg[%q] = %v, want %q", tt.input, k, gotArgs[k], v)
			}
		}
	}
}
