package agent

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
)

func TestDataAdminAgent_Registry(t *testing.T) {
	cfg := Config{
		EnableObfuscation: false,
	}
	agent := NewDataAdminAgent(cfg, nil, nil)

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

func TestDataAdminAgent_Execute(t *testing.T) {
	cfg := Config{
		EnableObfuscation: false,
	}
	dbs := map[string]sop.DatabaseOptions{
		"test_db": {},
	}
	agent := NewDataAdminAgent(cfg, dbs, nil)

	// Setup Context with Payload
	payload := &ai.SessionPayload{
		CurrentDB: "test_db",
	}
	ctx := context.Background()
	ctx = context.WithValue(ctx, "session_payload", payload)

	// Test list_databases
	resp, err := agent.Execute(ctx, "list_databases", nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if resp != "Databases: test_db" {
		t.Errorf("Expected 'Databases: test_db', got '%s'", resp)
	}
}
