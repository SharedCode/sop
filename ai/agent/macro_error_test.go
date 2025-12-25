package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

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
	svc := NewService(&MockDomain{}, sysDB, mockGen, nil, nil, false)

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
	err := svc.executeMacro(ctx, macro.Steps, make(map[string]any), nil, &sb, sysDB)

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
