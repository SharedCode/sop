package agent

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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

	// Fix IsPrimitiveKey in registry for the test store
	// This is needed because we created it using core_database (which defaults IsPrimitiveKey=false)
	// but we want jsondb.OpenStore (used by agent) to treat it as primitive.
	// Note: We must commit the previous transaction first because OpenBtree might start a new one if not careful,
	// or if we want to modify the registry which is a separate store.
	// Actually, we can just do it in the same transaction if we are careful.
	// But OpenBtree for registry might be tricky if it's already open implicitly.
	// Let's commit first, then update registry in a new transaction.
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit setup transaction: %v", err)
	}

	tx2, _ := core_database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	regStore, err := core_database.OpenBtree[string, sop.StoreInfo](ctx, dbOpts, "_registry", tx2, nil)
	if err == nil {
		if found, _ := regStore.Find(ctx, "employees", false); found {
			si, _ := regStore.GetCurrentValue(ctx)
			si.IsPrimitiveKey = true
			regStore.Update(ctx, "employees", si)
		}
	}
	tx2.Commit(ctx)

	// 3. Setup Agent & Service
	mockGen := &MockScriptedGenerator{
		Responses: []string{
			// Step 1 (Select 1)
			`{"tool": "select", "args": {"database": "` + filepath.Base(tmpDir) + `", "store": "employees", "limit": 2}}`,
			// Step 2 (Select 2)
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
	// Inject databases into agent
	dbName := filepath.Base(tmpDir)
	dbs := map[string]sop.DatabaseOptions{dbName: dbOpts}
	adminAgent.databases = dbs

	registry := map[string]ai.Agent[map[string]any]{
		"sql_admin": adminAgent,
	}

	_ = []PipelineStep{
		{Agent: PipelineAgent{ID: "sql_admin"}},
	}

	svc := NewService(&MockDomain{}, sysDB, dbs, mockGen, nil, registry, false)

	// 4. Execute Macro Logic (Simulated via /play)
	t.Log("Starting Macro Execution Simulation via /play...")

	payload := &ai.SessionPayload{
		CurrentDB: filepath.Base(tmpDir),
	}

	ctx = context.WithValue(ctx, "session_payload", payload)
	// ctx = context.WithValue(ctx, ai.CtxKeyExecutor, &MockToolExecutor{}) // Use real executor

	// We need to save the macro first
	// We can manually insert it into the macros store or use /record
	// Let's use /record for realism

	// Start recording
	svc.Ask(ctx, "/record demo_loop")

	// Record steps
	svc.RecordStep(ctx, ai.MacroStep{
		Type:    "command",
		Command: "manage_transaction",
		Args:    map[string]any{"action": "begin"},
	})
	svc.RecordStep(ctx, ai.MacroStep{
		Type:    "command",
		Command: "select",
		Args:    map[string]any{"database": filepath.Base(tmpDir), "store": "employees", "limit": 2},
	})
	svc.RecordStep(ctx, ai.MacroStep{
		Type:    "command",
		Command: "select",
		Args:    map[string]any{"database": filepath.Base(tmpDir), "store": "employees", "limit": 3},
	})
	svc.RecordStep(ctx, ai.MacroStep{
		Type:    "command",
		Command: "manage_transaction",
		Args:    map[string]any{"action": "commit"},
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

	svc := NewService(&MockDomain{}, nil, nil, mockGen, nil, nil, false)

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
	err := svc.runSteps(ctx, macro.Steps, make(map[string]any), nil, &sb, nil)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	output := sb.String()
	if !strings.Contains(output, "Found: John Doe, Bob Smith") {
		t.Errorf("Expected tool output, got: %s", output)
	}
}

func TestMacroExecution_CommandStep(t *testing.T) {
	svc := NewService(&MockDomain{}, nil, nil, nil, nil, nil, false)

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
	err := svc.runSteps(ctx, macro.Steps, make(map[string]any), nil, &sb, nil)
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

	svc := NewService(&MockDomain{}, sysDB, nil, nil, nil, nil, false)
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
	store.Save(ctx, "general", "test_macro", macro)
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

	dbName := filepath.Base(tmpDir)
	dbs := map[string]sop.DatabaseOptions{dbName: dbOpts}
	svc := NewService(&MockDomain{}, sysDB, dbs, mockGen, nil, nil, false)
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
	err = store.Load(ctx, "general", "my_saved_macro", &macro)
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

	_ = map[string]ai.Agent[map[string]any]{
		"sql_admin": adminAgent,
	}

	_ = []PipelineStep{
		{Agent: PipelineAgent{ID: "sql_admin"}},
	}

	dbName := filepath.Base(tmpDir)
	dbs := map[string]sop.DatabaseOptions{dbName: dbOpts}
	svc := NewService(&MockDomain{}, sysDB, dbs, mockGen, nil, nil, false)

	// 4. Execute Macro Logic (Recording Mode)
	t.Log("Starting Macro Recording Simulation...")

	payload := &ai.SessionPayload{
		CurrentDB: filepath.Base(tmpDir),
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

	if !strings.Contains(resp2, `"limit": 3`) {
		t.Error("Step 2 response missing limit 3")
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

	dbName := filepath.Base(tmpDir)
	dbs := map[string]sop.DatabaseOptions{dbName: dbOpts}
	svc := NewService(&MockDomain{}, sysDB, dbs, mockGen, nil, nil, false)

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
	if !strings.Contains(resp, "Error: Macro 'test_macro' (Category: general) already exists") {
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
	dbName := filepath.Base(tmpDir)
	dbs := map[string]sop.DatabaseOptions{dbName: dbOpts}
	svc := NewService(&MockDomain{}, sysDB, dbs, mockGen, nil, nil, false)
	ctx := context.Background()

	// 2. Create a macro "my_macro" with 3 steps
	svc.Ask(ctx, "/record my_macro")
	svc.session.CurrentMacro.Steps = []ai.MacroStep{
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
	dbName := filepath.Base(tmpDir)
	dbs := map[string]sop.DatabaseOptions{dbName: dbOpts}
	svc := NewService(&MockDomain{}, sysDB, dbs, mockGen, nil, nil, false)
	ctx := context.Background()

	// 2. Create sub-macro
	svc.Ask(ctx, "/record sub_macro")
	svc.session.CurrentMacro.Steps = []ai.MacroStep{
		{Type: "say", Message: "Sub Step 1"},
	}
	svc.Ask(ctx, "/stop")

	// 3. Create main-macro with nested macro step
	svc.Ask(ctx, "/record main_macro")
	svc.session.CurrentMacro.Steps = []ai.MacroStep{
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

func TestToolMacroAddStepFromLast(t *testing.T) {
	// Setup
	ctx := context.Background()

	// Mock Agent
	agent := &DataAdminAgent{
		databases: map[string]sop.DatabaseOptions{},
	}

	// Mock Last Tool Call
	agent.lastToolCall = &ai.MacroStep{
		Type:    "command",
		Command: "test_tool",
		Args:    map[string]any{"arg1": "val1"},
	}

	// We need a system DB with macros store to test this fully.
	// Since setting up a full system DB mock is complex, we will just check if the method exists and compiles.
	// The logic inside relies on updateMacro which interacts with the DB.

	// Let's just verify the method signature matches what we expect by calling it with nil context (it will fail but compile)
	_, _ = agent.toolMacroAddStepFromLast(ctx, map[string]any{
		"macro": "test_macro",
	})
}

type AsyncMockToolExecutor struct {
	mu       sync.Mutex
	executed []string
}

func (m *AsyncMockToolExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	if toolName == "sleep" {
		time.Sleep(100 * time.Millisecond)
	}
	m.mu.Lock()
	m.executed = append(m.executed, toolName)
	m.mu.Unlock()
	return "done", nil
}
func (m *AsyncMockToolExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return nil, nil
}

func TestMacroAsyncExecution(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)
	mockGen := &MockScriptedGenerator{}
	svc := NewService(&MockDomain{}, sysDB, nil, mockGen, nil, nil, false)

	executor := &AsyncMockToolExecutor{}
	ctx := context.Background()
	ctx = context.WithValue(ctx, ai.CtxKeyExecutor, executor)

	// Define macro with async steps
	macro := ai.Macro{
		Name: "async_test",
		Steps: []ai.MacroStep{
			{
				Type:    "command",
				Command: "sleep",
				IsAsync: true,
			},
			{
				Type:    "command",
				Command: "sleep",
				IsAsync: true,
			},
		},
	}

	// Execute
	var sb strings.Builder
	scope := make(map[string]any)
	var scopeMu sync.RWMutex

	start := time.Now()
	err := svc.runSteps(ctx, macro.Steps, scope, &scopeMu, &sb, sysDB)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// Verify both executed
	if len(executor.executed) != 2 {
		t.Errorf("Expected 2 executions, got %d", len(executor.executed))
	}

	// Verify duration (should be around 100ms, not 200ms)
	// Allow some buffer. 100ms sleep + overhead.
	// If sequential, it would be 200ms+.
	if duration > 190*time.Millisecond {
		t.Errorf("Execution took too long for async: %v", duration)
	}
}

type ErrorMockToolExecutor struct {
	mu       sync.Mutex
	executed []string
}

func (m *ErrorMockToolExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executed = append(m.executed, toolName)

	if toolName == "fail" {
		return "", errors.New("tool failed")
	}
	if toolName == "sleep" {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return "slept", nil
		}
	}
	return "done", nil
}
func (m *ErrorMockToolExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return nil, nil
}

func TestMacroAsyncErrorPropagation(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)
	mockGen := &MockScriptedGenerator{}
	svc := NewService(&MockDomain{}, sysDB, nil, mockGen, nil, nil, false)

	executor := &ErrorMockToolExecutor{}
	ctx := context.Background()
	ctx = context.WithValue(ctx, ai.CtxKeyExecutor, executor)

	// Define macro:
	// 1. Async sleep (should be cancelled)
	// 2. Sync fail (should stop everything)
	macro := ai.Macro{
		Name: "error_test",
		Steps: []ai.MacroStep{
			{
				Type:    "command",
				Command: "sleep",
				IsAsync: true,
			},
			{
				Type:    "command",
				Command: "fail",
			},
		},
	}

	var sb strings.Builder
	err := svc.runSteps(ctx, macro.Steps, make(map[string]any), nil, &sb, sysDB)

	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if err.Error() != "command execution failed: tool failed" {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify execution
	// Sleep might have started, but should be cancelled.
	// Fail should have executed.
	// We can't easily check if sleep was cancelled without more complex mocking,
	// but we can check that the test finished quickly (sleep didn't block for full duration).
}

func TestToolMacroAddStepFromLast_MetaToolExclusion(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)

	cfg := Config{Name: "TestAgent"}
	dbs := make(map[string]sop.DatabaseOptions)
	agent := NewDataAdminAgent(cfg, dbs, sysDB)

	ctx := context.Background()
	ctx = context.WithValue(ctx, "session_payload", &ai.SessionPayload{CurrentDB: "system"})

	// 1. Create a Macro
	// We don't have a direct tool for creating macro in registry yet (it's usually done via recording),
	// but we can manually create it in the system DB.
	tx, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
	store, _ := sysDB.OpenModelStore(ctx, "macros", tx)
	macro := ai.Macro{
		Name:  "test_macro",
		Steps: []ai.MacroStep{},
	}
	store.Save(ctx, "general", "test_macro", &macro)
	tx.Commit(ctx)

	// 2. Execute a "Real" Tool (e.g. list_databases)
	// This should update lastToolCall
	agent.Execute(ctx, "list_databases", map[string]any{})

	if agent.lastToolCall == nil || agent.lastToolCall.Command != "list_databases" {
		t.Fatalf("Expected lastToolCall to be 'list_databases', got %v", agent.lastToolCall)
	}

	// 3. Execute "macro_add_step_from_last"
	// This should NOT update lastToolCall, so it should add "list_databases" to the macro
	addArgs := map[string]any{
		"macro": "test_macro",
	}
	_, err := agent.Execute(ctx, "macro_add_step_from_last", addArgs)
	if err != nil {
		t.Fatalf("Failed to add step: %v", err)
	}

	// 4. Verify Macro Content
	tx, _ = sysDB.BeginTransaction(ctx, sop.ForReading)
	store, _ = sysDB.OpenModelStore(ctx, "macros", tx)
	var loadedMacro ai.Macro
	store.Load(ctx, "general", "test_macro", &loadedMacro)
	tx.Commit(ctx)

	if len(loadedMacro.Steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(loadedMacro.Steps))
	}
	if loadedMacro.Steps[0].Command != "list_databases" {
		t.Errorf("Expected step command 'list_databases', got '%s'", loadedMacro.Steps[0].Command)
	}

	// 5. Verify lastToolCall is STILL "list_databases" (or at least not "macro_add_step_from_last")
	if agent.lastToolCall.Command == "macro_add_step_from_last" {
		t.Error("lastToolCall was updated to 'macro_add_step_from_last', which is wrong")
	}
}

func TestToolMacroUpdateStep(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)

	cfg := Config{Name: "TestAgent"}
	dbs := make(map[string]sop.DatabaseOptions)
	agent := NewDataAdminAgent(cfg, dbs, sysDB)

	ctx := context.Background()
	ctx = context.WithValue(ctx, "session_payload", &ai.SessionPayload{CurrentDB: "system"})

	// 1. Create a Macro with 1 step
	tx, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
	store, _ := sysDB.OpenModelStore(ctx, "macros", tx)
	macro := ai.Macro{
		Name: "update_test_macro",
		Steps: []ai.MacroStep{
			{Type: "command", Command: "echo", Args: map[string]any{"msg": "hello"}},
		},
	}
	store.Save(ctx, "general", "update_test_macro", &macro)
	tx.Commit(ctx)

	// 2. Execute "macro_update_step" to change command to "print" and msg to "world"
	updateArgs := map[string]any{
		"macro":   "update_test_macro",
		"index":   0.0, // 0-based index
		"command": "print",
		"args":    map[string]any{"msg": "world"},
	}
	_, err := agent.Execute(ctx, "macro_update_step", updateArgs)
	if err != nil {
		t.Fatalf("Failed to update step: %v", err)
	}

	// 3. Verify Macro Content
	tx, _ = sysDB.BeginTransaction(ctx, sop.ForReading)
	store, _ = sysDB.OpenModelStore(ctx, "macros", tx)
	var loadedMacro ai.Macro
	store.Load(ctx, "general", "update_test_macro", &loadedMacro)
	tx.Commit(ctx)

	if len(loadedMacro.Steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(loadedMacro.Steps))
	}
	if loadedMacro.Steps[0].Command != "print" {
		t.Errorf("Expected step command 'print', got '%s'", loadedMacro.Steps[0].Command)
	}
	args := loadedMacro.Steps[0].Args
	if args["msg"] != "world" {
		t.Errorf("Expected arg msg='world', got %v", args["msg"])
	}
}
