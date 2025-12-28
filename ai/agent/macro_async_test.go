package agent

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

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
