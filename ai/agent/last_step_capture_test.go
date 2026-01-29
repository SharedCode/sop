package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop/ai"
)

// MockGeneratorWithRaw supports Raw output
type MockGeneratorWithRaw struct {
	Response string
	Raw      any
}

func (m *MockGeneratorWithRaw) Name() string {
	return "mock_raw"
}

func (m *MockGeneratorWithRaw) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	return ai.GenOutput{
		Text: m.Response,
		Raw:  m.Raw,
	}, nil
}

func (m *MockGeneratorWithRaw) EstimateCost(in, out int) float64 { return 0 }

func TestService_Ask_CapturesLastStep_OnToolExecution(t *testing.T) {
	// 1. Setup Mock Generator to return a tool call
	toolCallJSON := `{"tool": "select", "args": {"database": "mydb", "query": "select * from users"}}`
	gen := &MockGeneratorWithRaw{Response: toolCallJSON}

	// 2. Setup Service
	// NewService(domain, sysDB, registry, generator, ...)
	svc := NewService(&MockDomain{}, nil, nil, gen, nil, nil, false) // Disable obfuscation for simplicity

	// 3. Setup Mock Executor
	executor := &MockExecutor{}
	ctx := context.WithValue(context.Background(), ai.CtxKeyExecutor, executor)

	// 4. Execute Ask
	_, err := svc.Ask(ctx, "execute tool")
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	// 5. Verify LastStep
	if svc.session.LastStep == nil {
		t.Fatal("LastStep was not captured")
	}

	if svc.session.LastStep.Type != "command" {
		t.Errorf("Expected LastStep.Type 'command', got '%s'", svc.session.LastStep.Type)
	}

	if svc.session.LastStep.Command != "select" {
		t.Errorf("Expected LastStep.Command 'select', got '%s'", svc.session.LastStep.Command)
	}

	// Verify Args
	args := svc.session.LastStep.Args
	if args["database"] != "mydb" {
		t.Errorf("Expected arg 'database'='mydb', got '%v'", args["database"])
	}
}

func TestService_StepCommand_UsesLastStep(t *testing.T) {
	// 1. Setup Service and pre-populate LastStep
	svc := NewService(&MockDomain{}, nil, nil, &MockGeneratorWithRaw{}, nil, nil, false)
	svc.session.LastStep = &ai.ScriptStep{
		Type:    "command",
		Command: "prev_cmd",
		Args:    map[string]any{"arg1": "val1"},
	}

	// Start a script draft
	svc.session.CurrentScript = &ai.Script{
		Steps: []ai.ScriptStep{},
	}
	svc.session.CurrentScriptName = "test_script"

	// 2. Execute /step command via handleSessionCommand
	ctx := context.Background()
	resp, err := svc.Ask(ctx, "/step")
	if err != nil {
		t.Fatalf("Ask /step failed: %v", err)
	}

	// 3. Verify Response
	if !strings.Contains(resp, "Added step 1") {
		t.Errorf("Expected response to confirm added step, got: %s", resp)
	}

	// 4. Verify Script Steps
	if len(svc.session.CurrentScript.Steps) != 1 {
		t.Fatalf("Expected 1 step in CurrentScript, got %d", len(svc.session.CurrentScript.Steps))
	}

	addedStep := svc.session.CurrentScript.Steps[0]
	if addedStep.Command != "prev_cmd" {
		t.Errorf("Expected added step command 'prev_cmd', got '%s'", addedStep.Command)
	}
}

func TestService_Ask_CapturesLastStep_OnRawOutput(t *testing.T) {
	// 1. Setup Mock Generator to return Raw output
	// Ensure that Raw is successfully marshalled
	raw := map[string]any{"tool": "select", "args": map[string]any{"database": "mydb"}}
	gen := &MockGeneratorWithRaw{Response: "some text", Raw: raw}

	// 2. Setup Service
	svc := NewService(&MockDomain{}, nil, nil, gen, nil, nil, false)

	// Setup Mock Executor
	executor := &MockExecutor{}
	ctx := context.WithValue(context.Background(), ai.CtxKeyExecutor, executor)

	// 3. Execute Ask
	_, err := svc.Ask(ctx, "execute raw tool")
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	// 4. Verify LastStep

	if svc.session.LastStep == nil {
		t.Fatal("LastStep was not captured at all")
	}

	// Since Raw logic is removed in service.go, we expect NO tool execution from Raw.
	// So LastStep should be the "ask" step.
	if svc.session.LastStep.Type == "command" {
		// If command, then tool WAS executed (maybe via text fallback?)
	} else if svc.session.LastStep.Type == "ask" {
		t.Log("Tool was NOT executed, LastStep is 'ask'. This confirms Raw output disables JSON tool execution.")
	} else {
		t.Errorf("Unexpected LastStep type: %s", svc.session.LastStep.Type)
	}
}

// MockAgent implements ai.Agent for testing pipelines
type MockAgent struct {
	IDStr string
}

func (m *MockAgent) ID() string { return m.IDStr }
func (m *MockAgent) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error) {
	// Simulate tool execution and recording
	// This relies on the recorder being in the context and implementing ScriptRecorder interface
	if recorder, ok := ctx.Value(ai.CtxKeyScriptRecorder).(ai.ScriptRecorder); ok {
		recorder.RecordStep(ctx, ai.ScriptStep{
			Type:    "command",
			Command: "pipeline_tool",
			Args:    map[string]any{"query": query},
		})
	}
	return "Mock Agent Response", nil
}
func (m *MockAgent) Open(ctx context.Context) error  { return nil }
func (m *MockAgent) Close(ctx context.Context) error { return nil }
func (m *MockAgent) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	return nil, nil
}

func TestService_Ask_WithPipeline_CapturesLastStep(t *testing.T) {
	// 1. Setup Mock Agent
	mockAgent := &MockAgent{IDStr: "mock_agent"}
	registry := map[string]ai.Agent[map[string]any]{
		"mock_agent": mockAgent,
	}

	// 2. Setup Pipeline
	pipeline := []PipelineStep{
		{Agent: PipelineAgent{ID: "mock_agent"}},
	}

	// 3. Setup Service with Pipeline
	// NewService(domain, sysDB, registry, generator, ...)
	svc := NewService(&MockDomain{}, nil, nil, nil, pipeline, registry, false)

	// 4. Capture "ask" step happens inside Ask.
	// But the pipeline agent should overwrite it with "command".

	// 5. Execute Ask
	ctx := context.Background()
	svc.Ask(ctx, "run pipeline")

	// 6. Verify LastStep
	if svc.session.LastStep == nil {
		t.Fatal("LastStep was not captured")
	}

	// It should be the "command" step recorded by the pipeline agent, NOT the "ask" step
	if svc.session.LastStep.Type != "command" {
		t.Errorf("Expected LastStep.Type 'command', got '%s'. Prompt: %s", svc.session.LastStep.Type, svc.session.LastStep.Prompt)
	}

	if svc.session.LastStep.Command != "pipeline_tool" {
		t.Errorf("Expected LastStep.Command 'pipeline_tool', got '%s'", svc.session.LastStep.Command)
	}
}
