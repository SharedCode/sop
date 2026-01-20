package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
	"github.com/sharedcode/sop/ai/database"
)

// SimKnowledgeMockGenerator captures prompts and returns canned responses
type SimKnowledgeMockGenerator struct {
	LastPrompt string
	Response   string
}

func (m *SimKnowledgeMockGenerator) Name() string                     { return "mock" }
func (m *SimKnowledgeMockGenerator) EstimateCost(in, out int) float64 { return 0 }
func (m *SimKnowledgeMockGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.LastPrompt = prompt
	return ai.GenOutput{Text: m.Response}, nil
}

func TestSimulation_LLM_Learning_Loop(t *testing.T) {
	// 1. Setup Environment (Temp DB)
	tempDir := t.TempDir()

	// IMPORTANT: Must normalize/validate options to ensure internal defaults are set
	// Note: We use the sop/database package function since sop package doesn't expose it directly in this version
	// Actually, based on database_test.go, it uses database.ValidateOptions but with "sop" alias.
	// Let's rely on database.NewDatabase to do the internal validation, but ensure options are robust.
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{tempDir},
	}

	// Ensure the directory exists
	// os.MkdirAll(tempDir, 0755) // TempDir ensures this

	systemDB := database.NewDatabase(dbOpts)

	// 2. Initialize Agent with Mock Brain
	mockBrain := &SimKnowledgeMockGenerator{
		Response: "I am ready.",
	}

	// Create Agent
	// We pass an empty map for databases as we only test systemDB features here
	myAgent := agent.NewDataAdminAgent(agent.Config{
		ID:           "test-agent",
		Name:         "Tester",
		SystemPrompt: "You are a tester.",
	}, map[string]sop.DatabaseOptions{}, systemDB)

	myAgent.SetGenerator(mockBrain)

	// Open the agent (starts transaction if needed, though mostly for user DBs)
	ctx := context.Background()

	// Setup Session Payload required by Agent.Execute
	sessionPayload := &ai.SessionPayload{
		CurrentDB: "system",
	}
	ctx = context.WithValue(ctx, "session_payload", sessionPayload)

	if err := myAgent.Open(ctx); err != nil {
		t.Fatalf("Failed to open agent: %v", err)
	}
	defer myAgent.Close(ctx)

	// =========================================================
	// Phase 1: Teach the Agent (The Write Path)
	// =========================================================
	// We simulate the LLM deciding to call the tool to save a new term.
	// In a real scenario, the LLM generates this tool call JSON.

	t.Log("Step 1: Simulating LLM calling manage_knowledge tool with custom namespace...")

	// Tool arguments with CUSTOM namespace "finance"
	args := map[string]any{
		"namespace": "finance",
		"key":       "EBITDA",
		"value":     "Earnings Before Interest Taxes Depreciation and Amortization",
		"action":    "upsert",
	}

	// Execute the tool directly to simulate the agent running the tool call
	msg, err := myAgent.Execute(ctx, "manage_knowledge", args)
	if err != nil {
		t.Fatalf("Agent failed to learn (tool execution failed): %v", err)
	}

	t.Logf("Tool Output: %s", msg)

	if !strings.Contains(msg, "Successfully saved") {
		t.Errorf("Unexpected tool output: %s", msg)
	}

	// =========================================================
	// Phase 2: Verify Recall (The Read Path)
	// =========================================================
	// Now we ask a question. The Agent should fetch "EBITDA" from DB
	// and inject it into the prompt BEFORE sending it to our MockBrain.

	t.Log("Step 2: Asking question to verify knowledge injection...")

	mockBrain.Response = "I know what EBITDA is."

	// Create a new context with 'finance' as the CurrentDB.
	// This simulates the user working within the 'finance' domain,
	// triggering the retrieval of knowledge categorized under 'finance'.
	recallPayload := &ai.SessionPayload{
		CurrentDB: "finance",
	}
	recallCtx := context.WithValue(context.Background(), "session_payload", recallPayload)

	// Note: We use recallCtx to trigger 'finance' category loading.
	_, err = myAgent.Ask(recallCtx, "Calculate the EBITDA.")
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	// =========================================================
	// Phase 3: Assertions
	// =========================================================
	// Check if the prompt sent to the LLM contained our knowledge

	t.Log("Step 3: Verifying Prompt Content...")

	if !strings.Contains(mockBrain.LastPrompt, "Earnings Before Interest") {
		t.Fatalf("FAILURE: LLM was not given the learned knowledge!\nPrompt Payload Partial:\n...%s...",
			mockBrain.LastPrompt[len(mockBrain.LastPrompt)/2:]) // Show end of prompt
	} else {
		t.Log("SUCCESS: LLM received the learned knowledge context.")
	}
}
