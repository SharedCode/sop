package agent

import (
	"context"
	"testing"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

func TestDataAdminAgent_Registry(t *testing.T) {
	cfg := Config{
		EnableObfuscation: false,
	}
	agent := NewDataAdminAgent(cfg)

	// Test Registry Listing
	tools := agent.registry.List()
	if len(tools) == 0 {
		t.Fatal("Registry should not be empty")
	}

	found := false
	for _, tool := range tools {
		if tool.Name == "list_databases" {
			found = true
			break
		}
	}
	if !found {
		t.Error("list_databases tool not found in registry")
	}
}

func TestDataAdminAgent_ExecuteTool(t *testing.T) {
	cfg := Config{
		EnableObfuscation: false,
	}
	agent := NewDataAdminAgent(cfg)

	// Setup Context with Payload
	payload := &ai.SessionPayload{
		Databases: map[string]any{
			"test_db": &database.Database{},
		},
	}
	ctx := context.Background()
	ctx = context.WithValue(ctx, "session_payload", payload)

	// Test list_databases
	resp, err := agent.executeTool(ctx, "list_databases", nil)
	if err != nil {
		t.Fatalf("executeTool failed: %v", err)
	}
	if resp != "Databases: [test_db]" {
		t.Errorf("Expected 'Databases: [test_db]', got '%s'", resp)
	}
}
