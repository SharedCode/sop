package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
	"github.com/sharedcode/sop/ai/database"
)

// MockGenerator is a placeholder.
type MockGenerator struct{}

func (m *MockGenerator) Name() string { return "mock" }

func (m *MockGenerator) Generate(ctx context.Context, prompt string, options ai.GenOptions) (ai.GenOutput, error) {
	return ai.GenOutput{Text: "Mock AI Response"}, nil
}

func (m *MockGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

// MockAgent implements ai.Agent and ToolProvider
type MockAgent struct{}

func (a *MockAgent) Open(ctx context.Context) error  { return nil }
func (a *MockAgent) Close(ctx context.Context) error { return nil }
func (a *MockAgent) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	return nil, nil
}
func (a *MockAgent) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error) {
	return "", nil
}
func (a *MockAgent) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	if toolName == "echo" {
		msg, _ := args["msg"].(string)
		return "Echo: " + msg, nil
	}
	return "", fmt.Errorf("unknown tool: %s", toolName)
}

// GenericDomain is a placeholder.
type GenericDomain struct{}

func (d *GenericDomain) ID() string              { return "generic" }
func (d *GenericDomain) Name() string            { return "generic" }
func (d *GenericDomain) Embedder() ai.Embeddings { return nil }
func (d *GenericDomain) Index(ctx context.Context, tx sop.Transaction) (ai.VectorStore[map[string]any], error) {
	return nil, nil
}
func (d *GenericDomain) TextIndex(ctx context.Context, tx sop.Transaction) (ai.TextIndex, error) {
	return nil, nil
}
func (d *GenericDomain) BeginTransaction(ctx context.Context, mode sop.TransactionMode) (sop.Transaction, error) {
	return nil, nil
}
func (d *GenericDomain) Policies() ai.PolicyEngine                               { return nil }
func (d *GenericDomain) Classifier() ai.Classifier                               { return nil }
func (d *GenericDomain) Prompt(ctx context.Context, kind string) (string, error) { return "", nil }
func (d *GenericDomain) DataPath() string                                        { return "" }

func TestHandleExecuteScript(t *testing.T) {
	// 1. Setup Agent Service
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.NoCache,
	}
	sysDB := database.NewDatabase(dbOpts)

	// Create registry with MockAgent to handle "echo" tool
	registry := map[string]ai.Agent[map[string]any]{
		"mock": &MockAgent{},
	}

	svc := agent.NewService(&GenericDomain{}, sysDB, map[string]sop.DatabaseOptions{"system": dbOpts}, &MockGenerator{}, nil, registry, false)

	// 2. Register Agent in global map
	loadedAgents = make(map[string]ai.Agent[map[string]any])
	loadedAgents["sql_admin"] = svc

	// 3. Create a Test Script
	ctx := context.Background()
	script := ai.Script{
		Name:        "test_script",
		Description: "A test script",
		Parameters:  []string{"name"},
		Steps: []ai.ScriptStep{
			{
				Type: "command",
				// We use a dummy command. The execution might fail if the command handler isn't registered,
				// but the goal is to verify the endpoint invokes the service and streams JSON.
				Command: "echo",
				Args:    map[string]any{"msg": "Hello {{.name}}"},
			},
		},
	}

	// Save script to system DB
	tx, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
	store, _ := sysDB.OpenModelStore(ctx, "scripts", tx)
	store.Save(ctx, "general", "test_script", script)
	tx.Commit(ctx)

	// 4. Create Request
	reqBody := `{"name": "test_script", "args": {"name": "World"}}`
	req, _ := http.NewRequest("POST", "/api/scripts/execute", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// 5. Execute Handler
	// We test the handler directly, bypassing the auth middleware for this basic test
	// (or we could wrap it if we wanted to test auth)
	handleExecuteScript(w, req)

	// 6. Verify
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", w.Code, w.Body.String())
	}

	// Check headers
	if w.Header().Get("Transfer-Encoding") != "chunked" {
		t.Log("Warning: Transfer-Encoding chunked not set in recorder (expected for streaming)")
	}

	// Check Body
	body := w.Body.String()
	// Verify output contains the expected result (NDJSON format or JSON Array)
	// We relax the check to allow for whitespace differences (e.g. "record": "Echo..." vs "record":"Echo...")
	if !strings.Contains(body, "Echo: Hello World") {
		t.Errorf("Expected output to contain 'Echo: Hello World', got: %s", body)
	}
}

func TestHandleExecuteScript_Auth(t *testing.T) {
	// Setup Config
	config.EnableRestAuth = true
	config.RootPassword = "secret_password"

	// Handler with Auth
	handler := withAuth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Case 1: No Header
	req, _ := http.NewRequest("POST", "/api/scripts/execute", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 for missing header, got %d", w.Code)
	}

	// Case 2: Wrong Token
	req, _ = http.NewRequest("POST", "/api/scripts/execute", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 for wrong token, got %d", w.Code)
	}

	// Case 3: Correct Token
	req, _ = http.NewRequest("POST", "/api/scripts/execute", nil)
	req.Header.Set("Authorization", "Bearer secret_password")
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 for correct token, got %d", w.Code)
	}

	// Reset Config
	config.EnableRestAuth = false
}
