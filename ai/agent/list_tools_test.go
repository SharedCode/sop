package agent

import (
	"context"
	"encoding/json"
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

func TestListTools_StoresRoutingExposesStoresProtocolTools(t *testing.T) {
	agent := NewCopilotAgent(Config{}, nil, nil)
	ctx := context.Background()
	payload := &ai.SessionPayload{
		CurrentDB: "system",
		Variables: map[string]any{
			"RoutingState": &TaskContextClassification{
				Domain: StoresDomain,
				Layers: []LayerInfo{{Name: "Single-Domain", CRUD: []string{"R"}}},
			},
		},
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	agent.registerTools(ctx)
	tools, err := agent.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools failed: %v", err)
	}

	names := make(map[string]bool, len(tools))
	for _, tool := range tools {
		names[tool.Name] = true
	}
	if !names["execute_script"] || !names["list_stores"] {
		t.Fatalf("expected Stores routing to expose execute_script and list_stores, got %#v", names)
	}
	if !names["select"] || !names["explain_join"] {
		t.Fatalf("expected Stores routing to expose registered Stores read tools, got %#v", names)
	}
}

func TestListTools_SpacesRoutingHidesStoresProtocolTools(t *testing.T) {
	agent := NewCopilotAgent(Config{}, nil, nil)
	ctx := context.Background()
	payload := &ai.SessionPayload{
		CurrentDB: "system",
		Variables: map[string]any{
			"RoutingState": &TaskContextClassification{
				Domain: SpacesDomain,
				Layers: []LayerInfo{{Name: "Single-Domain", CRUD: []string{"R"}}},
			},
		},
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	agent.registerTools(ctx)
	tools, err := agent.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools failed: %v", err)
	}

	names := make(map[string]bool, len(tools))
	for _, tool := range tools {
		names[tool.Name] = true
	}
	if names["execute_script"] || names["list_stores"] || names["scan"] {
		t.Fatalf("expected Spaces routing to hide Stores protocol tools, got %#v", names)
	}
	if !names["read_space_config"] {
		t.Fatalf("expected Spaces routing to expose registered Spaces read tools, got %#v", names)
	}
}

func TestListTools_StoresReadRoutingHidesMutationTools(t *testing.T) {
	agent := NewCopilotAgent(Config{}, nil, nil)
	ctx := context.Background()
	payload := &ai.SessionPayload{
		CurrentDB: "system",
		Variables: map[string]any{
			"RoutingState": &TaskContextClassification{
				Domain: StoresDomain,
				Layers: []LayerInfo{{Name: "Single-Domain", CRUD: []string{"R"}}},
			},
		},
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	agent.registerTools(ctx)
	tools, err := agent.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools failed: %v", err)
	}

	names := make(map[string]bool, len(tools))
	for _, tool := range tools {
		names[tool.Name] = true
	}
	if names["add"] || names["update"] || names["delete"] {
		t.Fatalf("expected read-only Stores routing to hide mutation tools, got %#v", names)
	}
	if !names["manage_transaction"] {
		t.Fatalf("expected foundational Stores transaction tool to remain available, got %#v", names)
	}
}

func TestListTools_StoresUpdateRoutingExposesOnlyUpdateMutation(t *testing.T) {
	agent := NewCopilotAgent(Config{}, nil, nil)
	ctx := context.Background()
	payload := &ai.SessionPayload{
		CurrentDB: "system",
		Variables: map[string]any{
			"RoutingState": &TaskContextClassification{
				Domain: StoresDomain,
				Layers: []LayerInfo{{Name: "Single-Domain", CRUD: []string{"U"}}},
			},
		},
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	agent.registerTools(ctx)
	tools, err := agent.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools failed: %v", err)
	}

	names := make(map[string]bool, len(tools))
	for _, tool := range tools {
		names[tool.Name] = true
	}
	if !names["update"] || names["add"] || names["delete"] {
		t.Fatalf("expected update-only Stores routing to expose only update among store mutation tools, got %#v", names)
	}
	if !names["execute_script"] || !names["list_stores"] {
		t.Fatalf("expected foundational Stores protocol tools to remain available, got %#v", names)
	}
}

func TestListTools_SpacesUpdateRoutingExposesSpaceMutationTools(t *testing.T) {
	agent := NewCopilotAgent(Config{}, nil, nil)
	ctx := context.Background()
	payload := &ai.SessionPayload{
		CurrentDB: "system",
		Variables: map[string]any{
			"RoutingState": &TaskContextClassification{
				Domain: SpacesDomain,
				Layers: []LayerInfo{{Name: "Single-Domain", CRUD: []string{"U"}}},
			},
		},
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	agent.registerTools(ctx)
	tools, err := agent.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools failed: %v", err)
	}

	names := make(map[string]bool, len(tools))
	for _, tool := range tools {
		names[tool.Name] = true
	}
	if !names["mint_to_space"] || !names["update_space_config"] {
		t.Fatalf("expected Spaces update routing to expose mutation tools, got %#v", names)
	}
	if names["execute_script"] || names["list_stores"] {
		t.Fatalf("expected Spaces update routing to keep Stores protocol tools hidden, got %#v", names)
	}
}

func TestListTools_LowRiskExposedToolsUseJSONSchemas(t *testing.T) {
	agent := NewCopilotAgent(Config{}, nil, nil)
	ctx := context.Background()
	ctx = context.WithValue(ctx, "session_payload", &ai.SessionPayload{CurrentDB: "system"})

	agent.registerTools(ctx)
	tools, err := agent.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools failed: %v", err)
	}

	expectedJSON := map[string]bool{
		"list_stores":           false,
		"manage_transaction":    false,
		"mint_to_space":         false,
		"update_space_config":   false,
		"execute_local_command": false,
		"send_email":            false,
	}

	for _, tool := range tools {
		if _, ok := expectedJSON[tool.Name]; !ok {
			continue
		}
		if !strings.HasPrefix(strings.TrimSpace(tool.Schema), "{") {
			t.Fatalf("expected %s schema to be JSON, got %q", tool.Name, tool.Schema)
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(tool.Schema), &decoded); err != nil {
			t.Fatalf("expected %s schema to decode as JSON: %v\nSchema: %s", tool.Name, err, tool.Schema)
		}
		expectedJSON[tool.Name] = true
	}

	for name, found := range expectedJSON {
		if !found {
			t.Fatalf("expected tool %s to be exposed with a JSON schema", name)
		}
	}
}
