//go:build llm
// +build llm

package agent_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/generator"
)

func TestServiceIntegration_LastTool(t *testing.T) {
	// 1. Setup LLM
	apiKey := os.Getenv("LLM_API_KEY")
	if apiKey == "" {
		t.Skip("LLM_API_KEY not set")
	}

	// Use a known working model
	model := "gemini-2.5-pro"
	if envModel := os.Getenv("GEMINI_MODEL"); envModel != "" {
		model = envModel
	}

	fmt.Printf("Using model: %s\n", model)
	gen, err := generator.New("gemini", map[string]any{
		"api_key": apiKey,
		"model":   model,
	})
	if err != nil {
		t.Fatalf("Failed to create generator: %v", err)
	}

	// 2. Setup Dependencies
	// System DB
	sysDB := database.NewDatabase(sop.DatabaseOptions{
		StoresFolders: []string{"/tmp/sysdb_test_service"},
	})

	// User DB
	databases := make(map[string]sop.DatabaseOptions)
	databases["mydb"] = sop.DatabaseOptions{
		StoresFolders: []string{"/tmp/mydb_service"},
	}

	// Registry
	registry := make(map[string]ai.Agent[map[string]any])

	// 3. Create DataAdminAgent (sql_core)
	coreCfg := agent.Config{
		ID:                "sql_core",
		Name:              "SQL Core",
		Type:              "data_admin",
		EnableObfuscation: true, // Enable Obfuscation to match production
		Verbose:           true,
		StubMode:          true, // Enable Stub Mode
	}

	// We need to manually set the generator for the core agent since NewFromConfig might try to create one
	// But NewFromConfig creates a new agent.
	// Let's use NewDataAdminAgent directly to inject the generator easily.
	coreAgent := agent.NewDataAdminAgent(coreCfg, databases, sysDB)
	coreAgent.Open(ctx)
	coreAgent.SetGenerator(gen)
	registry["sql_core"] = coreAgent

	// 4. Create Service (sql_admin)
	// The Service uses a pipeline.
	pipeline := []agent.PipelineStep{
		{
			Agent: agent.PipelineAgent{ID: "sql_core"},
		},
	}

	svc := agent.NewService(
		nil, // domain
		sysDB,
		databases,
		gen,
		pipeline,
		registry,
		false, // enableObfuscation
	)

	// 5. Run Test
	ctx := context.Background()
	ctx = context.WithValue(ctx, ai.CtxKeyWriter, os.Stdout)

	// We need to set the Executor in the context so Service can delegate tool calls
	// The Service.Ask method checks for CtxKeyExecutor.
	// In main.ai.go, DefaultToolExecutor delegates to "sql_core".
	executor := &TestToolExecutor{Agents: registry}
	ctx = context.WithValue(ctx, ai.CtxKeyExecutor, executor)

	// Set Session Payload
	ctx = context.WithValue(ctx, "session_payload", &ai.SessionPayload{
		CurrentDB: "mydb",
	})

	// Query that should trigger execute_script
	// "Join users and orders" is a good candidate as per the prompt in DataAdminAgent
	query := "Join users and orders on user_id"

	fmt.Printf("Sending query: %s\n", query)
	resp, err := svc.Ask(ctx, query)
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}
	fmt.Printf("Response: %s\n", resp)

	// 6. Verify Last Tool
	lastTool := svc.GetLastToolInstructions()
	fmt.Printf("Last Tool Instructions:\n%s\n", lastTool)

	if lastTool == "" {
		t.Fatal("Last tool instructions are empty")
	}

	// Check if script is present and not empty
	var toolCall struct {
		Tool string         `json:"tool"`
		Args map[string]any `json:"args"`
	}
	if err := json.Unmarshal([]byte(lastTool), &toolCall); err != nil {
		t.Fatalf("Failed to parse last tool JSON: %v", err)
	}

	if toolCall.Tool != "execute_script" {
		t.Logf("Warning: Expected tool 'execute_script', got '%s'. This might be due to LLM choice.", toolCall.Tool)
	}

	if toolCall.Tool == "execute_script" {
		script, ok := toolCall.Args["script"]
		if !ok {
			t.Fatal("Script argument missing in execute_script call")
		}

		// Check if script is empty
		scriptList, ok := script.([]any)
		if ok && len(scriptList) == 0 {
			t.Fatal("Script argument is an empty list")
		}

		fmt.Printf("Script content: %+v\n", script)
	}
}

// TestToolExecutor implements ai.ToolExecutor
type TestToolExecutor struct {
	Agents map[string]ai.Agent[map[string]any]
}

func (e *TestToolExecutor) Execute(ctx context.Context, tool string, args map[string]any) (string, error) {
	if agentSvc, ok := e.Agents["sql_core"]; ok {
		if da, ok := agentSvc.(*agent.DataAdminAgent); ok {
			return da.Execute(ctx, tool, args)
		}
	}
	return "", fmt.Errorf("tool '%s' not found", tool)
}

func (e *TestToolExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return nil, nil
}
