package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestListTools(t *testing.T) {
	agent := NewCopilotAgent(Config{}, nil, nil)
	ctx := context.Background()

	// Inject session payload
	payload := &ai.SessionPayload{
		CurrentDB: "system",
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	agent.registerTools(ctx)

	// Invoke valid tool
	output, err := agent.Execute(ctx, "list_tools", nil)
	if err != nil {
		t.Fatalf("list_tools failed: %v", err)
	}

	// Output format is a simple list, not a table
	if strings.Contains(output, "| Command | Arguments | Description |") {
		t.Errorf("Output should NOT contain table header (format changed to list). Got: %s", output)
	}

	expected := "- `/list_databases`: Lists all available databases."
	if !strings.Contains(output, expected) {
		t.Errorf("Output should contain list_databases row.\nExpected to contain: %s\nGot:\n%s", expected, output)
	}
}
