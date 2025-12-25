package agent

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	core_database "github.com/sharedcode/sop/database"
)

// MockScriptedGenerator returns a sequence of responses.
type MockScriptedGenerator struct {
	Responses []string
	Index     int
}

func (m *MockScriptedGenerator) Name() string { return "mock_scripted" }
func (m *MockScriptedGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	if m.Index >= len(m.Responses) {
		return ai.GenOutput{Text: "No more mock responses"}, nil
	}
	resp := m.Responses[m.Index]
	m.Index++
	return ai.GenOutput{Text: resp}, nil
}
func (m *MockScriptedGenerator) EstimateCost(in, out int) float64 { return 0 }

// MockToolExecutor for testing
type MockToolExecutor struct{}

func (m *MockToolExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	if toolName == "select" {
		return "Found: John Doe, Bob Smith", nil
	}
	return "Tool executed", nil
}
func (m *MockToolExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return nil, nil
}

func TestMacroExecution_SelectTwice(t *testing.T) {
	// 1. Setup Temp DB
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered, // Use Clustered to support transactions properly
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory, // Use InMemory cache for speed
	}

	// Initialize SystemDB
	sysDB := database.NewDatabase(dbOpts)

	// 2. Create a Test Store with Data
	ctx := context.Background()

	// Create "employees" store
	t.Log("Creating 'employees' store...")
	tx, err := core_database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Use core_database to create the B-Tree
	b3, err := core_database.NewBtree[string, any](ctx, dbOpts, "employees", tx, nil)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add data
	b3.Add(ctx, "emp1", "John Doe")
	b3.Add(ctx, "emp2", "Jane Doe")
	b3.Add(ctx, "emp3", "Bob Smith")

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit setup transaction: %v", err)
	}

	// 3. Setup Agent & Service
	mockGen := &MockScriptedGenerator{
		Responses: []string{
			// Step 1
			`{"tool": "select", "args": {"database": "` + filepath.Base(tmpDir) + `", "store": "employees", "limit": 2}}`,
			// Step 2
			`{"tool": "select", "args": {"database": "` + filepath.Base(tmpDir) + `", "store": "employees", "limit": 3}}`,
		},
	}

	// Create DataAdminAgent
	agentCfg := Config{
		ID:          "sql_admin",
		Name:        "SQL Admin",
		Description: "SQL Admin",
	}

	adminAgent := &DataAdminAgent{
		Config:            agentCfg,
		brain:             mockGen,
		enableObfuscation: false,
	}
	// Ensure registry is initialized and tools are registered
	adminAgent.registry = NewRegistry()
	adminAgent.registerTools()

	registry := map[string]ai.Agent[map[string]any]{
		"sql_admin": adminAgent,
	}

	pipeline := []PipelineStep{
		{Agent: PipelineAgent{ID: "sql_admin"}},
	}

	svc := NewService(&MockDomain{}, sysDB, mockGen, pipeline, registry, false)

	// 4. Execute Macro Logic (Simulated via /play)
	t.Log("Starting Macro Execution Simulation via /play...")

	payload := &ai.SessionPayload{
		CurrentDB: sysDB,
		Databases: map[string]any{
			filepath.Base(tmpDir): sysDB,
		},
		Variables: map[string]any{
			"database": filepath.Base(tmpDir),
		},
	}

	ctx = context.WithValue(ctx, "session_payload", payload)
	ctx = context.WithValue(ctx, ai.CtxKeyExecutor, &MockToolExecutor{})

	// We need to save the macro first
	// We can manually insert it into the macros store or use /record
	// Let's use /record for realism

	// Start recording
	svc.Ask(ctx, "/record demo_loop")

	// Record steps
	svc.RecordStep(ai.MacroStep{
		Type:   "ask",
		Prompt: "select from employees limit 2",
	})
	svc.RecordStep(ai.MacroStep{
		Type:   "ask",
		Prompt: "select from employees limit 3",
	})

	// Stop recording (saves macro)
	resp, err := svc.Ask(ctx, "/stop")
	if err != nil {
		t.Fatalf("Failed to stop recording: %v", err)
	}
	t.Logf("Stop response: %s", resp)

	// Now Play
	// We need to reset the mock generator index because /play will trigger the same prompts
	mockGen.Index = 0

	// Execute /play
	respPlay, err := svc.Ask(ctx, "/play demo_loop")
	if err != nil {
		t.Fatalf("Play failed: %v", err)
	}
	t.Logf("Play Response: %s", respPlay)

	// Verify output contains expected strings
	// Simple contains check
	if !strings.Contains(respPlay, "John Doe") {
		t.Error("Response missing data")
	}
	if !strings.Contains(respPlay, "Bob Smith") {
		t.Error("Response missing Bob Smith")
	}
}

func TestMacroExecution_NoPipeline(t *testing.T) {
	// Setup Service with NO pipeline, just a generator
	mockGen := &MockScriptedGenerator{
		Responses: []string{
			`{"tool": "select", "args": {"store": "employees"}}`,
		},
	}

	svc := NewService(&MockDomain{}, nil, mockGen, nil, nil, false)

	// Inject MockToolExecutor
	ctx := context.Background()
	ctx = context.WithValue(ctx, ai.CtxKeyExecutor, &MockToolExecutor{})

	// Play macro
	macro := ai.Macro{
		Steps: []ai.MacroStep{
			{Type: "ask", Prompt: "select something"},
		},
	}

	var sb strings.Builder
	err := svc.executeMacro(ctx, macro.Steps, make(map[string]any), nil, &sb, nil)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	output := sb.String()
	if !strings.Contains(output, "Found: John Doe, Bob Smith") {
		t.Errorf("Expected tool output, got: %s", output)
	}
}

func TestMacroExecution_CommandStep(t *testing.T) {
	svc := NewService(&MockDomain{}, nil, nil, nil, nil, false)

	// Inject MockToolExecutor
	ctx := context.Background()
	ctx = context.WithValue(ctx, ai.CtxKeyExecutor, &MockToolExecutor{})

	// Play macro with "command" step
	macro := ai.Macro{
		Steps: []ai.MacroStep{
			{
				Type:    "command",
				Command: "select",
				Args:    map[string]any{"store": "employees"},
			},
		},
	}

	var sb strings.Builder
	err := svc.executeMacro(ctx, macro.Steps, make(map[string]any), nil, &sb, nil)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	output := sb.String()
	if !strings.Contains(output, "Found: John Doe, Bob Smith") {
		t.Errorf("Expected tool output, got: %s", output)
	}
}

func TestMacroShow(t *testing.T) {
	// Setup Temp DB
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)

	svc := NewService(&MockDomain{}, sysDB, nil, nil, nil, false)
	ctx := context.Background()

	// Create a macro manually
	macro := ai.Macro{
		Name: "test_macro",
		Steps: []ai.MacroStep{
			{Type: "ask", Prompt: "hello"},
			{Type: "command", Command: "select", Args: map[string]any{"store": "users"}},
		},
	}

	// Save macro
	tx, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
	store, _ := sysDB.OpenModelStore(ctx, "macros", tx)
	store.Save(ctx, "macros", "test_macro", macro)
	tx.Commit(ctx)

	// Test /macro show
	resp, err := svc.Ask(ctx, "/macro show test_macro")
	if err != nil {
		t.Fatalf("Show failed: %v", err)
	}
	t.Logf("Show Response:\n%s", resp)

	if !strings.Contains(resp, "test_macro") {
		t.Error("Response missing macro name")
	}
	if !strings.Contains(resp, "hello") {
		t.Error("Response missing prompt")
	}
	if !strings.Contains(resp, "select") {
		t.Error("Response missing command")
	}
}

func TestMacroSaveAs(t *testing.T) {
	// Setup Temp DB
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)

	// Mock Generator that returns a tool call
	mockGen := &MockScriptedGenerator{
		Responses: []string{
			`{"tool": "select", "args": {"store": "users"}}`,
		},
	}

	svc := NewService(&MockDomain{}, sysDB, mockGen, nil, nil, false)
	ctx := context.Background()
	// Inject MockToolExecutor
	ctx = context.WithValue(ctx, ai.CtxKeyExecutor, &MockToolExecutor{})

	// 1. Run a command (not recording)
	_, err := svc.Ask(ctx, "find users")
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	// 2. Save as macro
	resp, err := svc.Ask(ctx, "/macro save_as my_saved_macro")
	if err != nil {
		t.Fatalf("Save As failed: %v", err)
	}
	t.Logf("Save As Response: %s", resp)

	// 3. Verify macro exists and has the command step
	tx, _ := sysDB.BeginTransaction(ctx, sop.ForReading)
	store, _ := sysDB.OpenModelStore(ctx, "macros", tx)
	var macro ai.Macro
	err = store.Load(ctx, "macros", "my_saved_macro", &macro)
	tx.Commit(ctx)

	if err != nil {
		t.Fatalf("Failed to load saved macro: %v", err)
	}
	if len(macro.Steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(macro.Steps))
	}
	if macro.Steps[0].Type != "command" {
		t.Errorf("Expected step type 'command', got '%s'", macro.Steps[0].Type)
	}
	if macro.Steps[0].Command != "select" {
		t.Errorf("Expected command 'select', got '%s'", macro.Steps[0].Command)
	}
}

func TestMacroRecording_SelectTwice(t *testing.T) {
	// 1. Setup Temp DB
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}

	// Initialize SystemDB
	sysDB := database.NewDatabase(dbOpts)

	// 2. Create a Test Store with Data
	ctx := context.Background()

	t.Log("Creating 'employees' store...")
	tx, err := core_database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	b3, err := core_database.NewBtree[string, any](ctx, dbOpts, "employees", tx, nil)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	b3.Add(ctx, "emp1", "John Doe")
	b3.Add(ctx, "emp2", "Jane Doe")
	b3.Add(ctx, "emp3", "Bob Smith")

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit setup transaction: %v", err)
	}

	// 3. Setup Agent & Service
	mockGen := &MockScriptedGenerator{
		Responses: []string{
			// Step 1
			`{"tool": "select", "args": {"database": "` + filepath.Base(tmpDir) + `", "store": "employees", "limit": 2}}`,
			// Step 2
			`{"tool": "select", "args": {"database": "` + filepath.Base(tmpDir) + `", "store": "employees", "limit": 3}}`,
		},
	}

	agentCfg := Config{
		ID:          "sql_admin",
		Name:        "SQL Admin",
		Description: "SQL Admin",
	}

	adminAgent := &DataAdminAgent{
		Config:            agentCfg,
		brain:             mockGen,
		enableObfuscation: false,
		registry:          NewRegistry(),
	}
	adminAgent.registerTools()

	registry := map[string]ai.Agent[map[string]any]{
		"sql_admin": adminAgent,
	}

	pipeline := []PipelineStep{
		{Agent: PipelineAgent{ID: "sql_admin"}},
	}

	svc := NewService(&MockDomain{}, sysDB, mockGen, pipeline, registry, false)

	// 4. Execute Macro Logic (Recording Mode)
	t.Log("Starting Macro Recording Simulation...")

	payload := &ai.SessionPayload{
		CurrentDB: sysDB,
		Databases: map[string]any{
			filepath.Base(tmpDir): sysDB,
		},
		Variables: map[string]any{
			"database": filepath.Base(tmpDir),
		},
	}

	ctx = context.WithValue(ctx, "session_payload", payload)

	// Open Session (Starts Transaction)
	if err := svc.Open(ctx); err != nil {
		t.Fatalf("Failed to open service session: %v", err)
	}
	defer svc.Close(ctx)

	// Start recording
	svc.Ask(ctx, "/record demo_loop_rec")

	// Step 1
	t.Log("Executing Step 1 (Recording)...")
	resp1, err := svc.Ask(ctx, "select from employees limit 2")
	if err != nil {
		t.Fatalf("Step 1 failed: %v", err)
	}
	t.Logf("Step 1 Response: %s", resp1)

	// Step 2
	t.Log("Executing Step 2 (Recording)...")
	// We need to reset the mock generator index because we are calling Ask again
	// But wait, Ask calls Generate.
	// The mock generator has 2 responses.
	// Step 1 consumed response 0.
	// Step 2 consumes response 1.
	// So index is correct.

	resp2, err := svc.Ask(ctx, "select from employees limit 3")
	if err != nil {
		t.Fatalf("Step 2 failed: %v", err)
	}
	t.Logf("Step 2 Response: %s", resp2)

	if !strings.Contains(resp2, "Bob Smith") {
		t.Error("Step 2 response missing Bob Smith")
	}

	// Stop recording
	svc.Ask(ctx, "/stop")
}

func TestMacroRecording_OverwriteProtection(t *testing.T) {
	// 1. Setup Temp DB
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}

	// Initialize SystemDB
	sysDB := database.NewDatabase(dbOpts)

	// 2. Setup Service
	// We don't need a real generator or pipeline for this test, just the macro recording logic.
	mockGen := &MockScriptedGenerator{Responses: []string{}}

	svc := NewService(&MockDomain{}, sysDB, mockGen, nil, nil, false)

	ctx := context.Background()

	// 3. Record a macro "test_macro"
	resp, err := svc.Ask(ctx, "/record test_macro")
	if err != nil {
		t.Fatalf("Failed to start recording: %v", err)
	}
	if !strings.Contains(resp, "Recording macro 'test_macro'") {
		t.Errorf("Unexpected response: %s", resp)
	}

	// Stop recording (saves it)
	resp, err = svc.Ask(ctx, "/stop")
	if err != nil {
		t.Fatalf("Failed to stop recording: %v", err)
	}
	if strings.Contains(resp, "Error") {
		t.Fatalf("Failed to save macro: %s", resp)
	}

	// 4. Try to record "test_macro" again (should fail)
	resp, err = svc.Ask(ctx, "/record test_macro")
	if err != nil {
		t.Fatalf("Failed to ask: %v", err)
	}
	if !strings.Contains(resp, "Error: Macro 'test_macro' already exists") {
		t.Errorf("Expected overwrite error, got: %s", resp)
	}

	// 5. Try to record "test_macro" again with --force (should succeed)
	resp, err = svc.Ask(ctx, "/record test_macro --force")
	if err != nil {
		t.Fatalf("Failed to ask: %v", err)
	}
	if !strings.Contains(resp, "Recording macro 'test_macro'") {
		t.Errorf("Expected success with --force, got: %s", resp)
	}
}

func TestMacroManagement(t *testing.T) {
	// 1. Setup Temp DB
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)
	mockGen := &MockScriptedGenerator{Responses: []string{}}
	svc := NewService(&MockDomain{}, sysDB, mockGen, nil, nil, false)
	ctx := context.Background()

	// 2. Create a macro "my_macro" with 3 steps
	svc.Ask(ctx, "/record my_macro")
	svc.currentMacro.Steps = []ai.MacroStep{
		{Type: "ask", Prompt: "Step 1"},
		{Type: "ask", Prompt: "Step 2"},
		{Type: "ask", Prompt: "Step 3"},
	}
	svc.Ask(ctx, "/stop")

	// 3. Test /macro list
	resp, err := svc.Ask(ctx, "/macro list")
	if err != nil {
		t.Fatalf("Failed to list macros: %v", err)
	}
	if !strings.Contains(resp, "my_macro") {
		t.Errorf("Expected 'my_macro' in list, got: %s", resp)
	}

	// 4. Test /macro show
	resp, err = svc.Ask(ctx, "/macro show my_macro")
	if err != nil {
		t.Fatalf("Failed to show macro: %v", err)
	}
	if !strings.Contains(resp, "1. [ask] Step 1") || !strings.Contains(resp, "3. [ask] Step 3") {
		t.Errorf("Unexpected show output: %s", resp)
	}

	// 5. Test /macro step delete (delete Step 2)
	resp, err = svc.Ask(ctx, "/macro step delete my_macro 2")
	if err != nil {
		t.Fatalf("Failed to delete step: %v", err)
	}
	if !strings.Contains(resp, "Step 2 deleted") {
		t.Errorf("Unexpected delete response: %s", resp)
	}

	// Verify deletion
	resp, err = svc.Ask(ctx, "/macro show my_macro")
	if !strings.Contains(resp, "1. [ask] Step 1") || !strings.Contains(resp, "2. [ask] Step 3") {
		t.Errorf("Step 2 was not deleted correctly. Output: %s", resp)
	}

	// 6. Test /macro show --json
	resp, err = svc.Ask(ctx, "/macro show my_macro --json")
	if err != nil {
		t.Fatalf("Failed to show macro json: %v", err)
	}
	if !strings.Contains(resp, "```json") || !strings.Contains(resp, "\"name\": \"my_macro\"") {
		t.Errorf("Unexpected JSON output: %s", resp)
	}

	// 7. Test /macro delete
	resp, err = svc.Ask(ctx, "/macro delete my_macro")
	if err != nil {
		t.Fatalf("Failed to delete macro: %v", err)
	}
	if !strings.Contains(resp, "deleted") {
		t.Errorf("Unexpected delete response: %s", resp)
	}

	// Verify deletion
	resp, err = svc.Ask(ctx, "/macro list")
	if strings.Contains(resp, "my_macro") {
		t.Errorf("Macro should be deleted, but found in list: %s", resp)
	}
}

func TestMacroNestedAndUpdates(t *testing.T) {
	// 1. Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)
	mockGen := &MockScriptedGenerator{Responses: []string{
		"Response 1",
		"Response 2",
	}}
	svc := NewService(&MockDomain{}, sysDB, mockGen, nil, nil, false)
	ctx := context.Background()

	// 2. Create sub-macro
	svc.Ask(ctx, "/record sub_macro")
	svc.currentMacro.Steps = []ai.MacroStep{
		{Type: "say", Message: "Sub Step 1"},
	}
	svc.Ask(ctx, "/stop")

	// 3. Create main-macro with nested macro step
	svc.Ask(ctx, "/record main_macro")
	svc.currentMacro.Steps = []ai.MacroStep{
		{Type: "ask", Prompt: "Main Step 1"},
		{Type: "macro", MacroName: "sub_macro"},
	}
	svc.Ask(ctx, "/stop")

	// 4. Verify /macro show displays nested macro correctly
	resp, err := svc.Ask(ctx, "/macro show main_macro")
	if err != nil {
		t.Fatalf("Failed to show macro: %v", err)
	}
	if !strings.Contains(resp, "2. [macro] Run 'sub_macro'") {
		t.Errorf("Unexpected show output for nested macro: %s", resp)
	}

	// 5. Test /macro step update
	// First, run a command to set s.lastStep
	svc.Ask(ctx, "New Prompt") // This sets s.lastStep to an "ask" step with "New Prompt"

	// Update step 1 of main_macro
	resp, err = svc.Ask(ctx, "/macro step update main_macro 1")
	if err != nil {
		t.Fatalf("Failed to update step: %v", err)
	}
	if !strings.Contains(resp, "Step 1 updated") {
		t.Errorf("Unexpected update response: %s", resp)
	}

	// Verify update
	resp, err = svc.Ask(ctx, "/macro show main_macro")
	if !strings.Contains(resp, "1. [ask] New Prompt") {
		t.Errorf("Step 1 was not updated correctly. Output: %s", resp)
	}

	// 6. Test /macro step add (add to bottom)
	svc.Ask(ctx, "Added Prompt")
	resp, err = svc.Ask(ctx, "/macro step add main_macro bottom")
	if err != nil {
		t.Fatalf("Failed to add step: %v", err)
	}

	// Verify add
	resp, err = svc.Ask(ctx, "/macro show main_macro")
	if !strings.Contains(resp, "3. [ask] Added Prompt") {
		t.Errorf("Step was not added correctly. Output: %s", resp)
	}
}
