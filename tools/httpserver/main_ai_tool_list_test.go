package main

import (
	"context"
	"testing"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
)

func TestDefaultToolExecutor_ListTools_DelegatesToCopilotAgent(t *testing.T) {
	executor := &DefaultToolExecutor{Agents: map[string]ai.Agent[map[string]any]{"copilot": &MockAgent{}}}

	tools, err := executor.ListTools(context.Background())
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "echo" {
		t.Fatalf("ListTools() = %#v, want one echo tool", tools)
	}
}

func TestDefaultToolExecutor_ListTools_UsesServiceRegistry(t *testing.T) {
	svc := agent.NewService(nil, nil, nil, nil, nil, map[string]ai.Agent[map[string]any]{
		"tool": &MockAgent{},
	}, false)

	executor := &DefaultToolExecutor{Agents: map[string]ai.Agent[map[string]any]{"copilot": svc}}

	tools, err := executor.ListTools(context.Background())
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "echo" {
		t.Fatalf("ListTools() = %#v, want one echo tool from Service registry", tools)
	}
}
