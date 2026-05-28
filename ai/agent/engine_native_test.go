package agent

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/sharedcode/sop/ai"
)

type loopMockGenerator struct {
	calls int
}

func (m *loopMockGenerator) Name() string { return "loop_mock" }

func (m *loopMockGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *loopMockGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.calls++
	if m.calls == 1 {
		if strings.Contains(prompt, "Tool results:") {
			return ai.GenOutput{Text: "unexpected tool results on first pass"}, nil
		}
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "execute_script",
			Args: map[string]any{"script": []any{map[string]any{"op": "scan"}}},
		}}}, nil
	}

	if !strings.Contains(prompt, "Tool results:") || !strings.Contains(prompt, "[System Tool Response]:") || !strings.Contains(prompt, "Analyze the tool response") {
		return ai.GenOutput{Text: "missing synthesis prompt context"}, nil
	}
	return ai.GenOutput{Text: "Final answer: Found John Doe in the database"}, nil
}

type loopMockExecutor struct{}

func (e *loopMockExecutor) Execute(ctx context.Context, tool string, args map[string]any) (string, error) {
	return `[{"name":"John Doe"}]`, nil
}

func (e *loopMockExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return []ai.ToolDefinition{{Name: "execute_script"}}, nil
}

func TestNativeReActEngine_SynthesizesAfterToolResult(t *testing.T) {
	engine := &NativeReActEngine{}
	gen := &loopMockGenerator{}
	resp, err := engine.Run(context.Background(), ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Show me users",
		Executor:     &loopMockExecutor{},
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "Final answer: Found John Doe in the database" {
		t.Fatalf("expected synthesized final answer, got %q", resp.FinalText)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "execute_script" {
		t.Fatalf("expected recorded tool call, got %#v", resp.ToolCalls)
	}
	if gen.calls != 2 {
		t.Fatalf("expected two generator calls, got %d", gen.calls)
	}
}

type csvLoopMockGenerator struct {
	calls int
}

func (m *csvLoopMockGenerator) Name() string { return "csv_loop_mock" }

func (m *csvLoopMockGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *csvLoopMockGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.calls++
	if m.calls == 1 {
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "execute_script",
			Args: map[string]any{"script": []any{map[string]any{"op": "scan"}}},
		}}}, nil
	}
	return ai.GenOutput{Text: "| name |\n| John Doe |"}, nil
}

type csvLoopMockExecutor struct{}

func (e *csvLoopMockExecutor) Execute(ctx context.Context, tool string, args map[string]any) (string, error) {
	return "name\nJohn Doe\n", nil
}

func (e *csvLoopMockExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return []ai.ToolDefinition{{Name: "execute_script"}}, nil
}

type csvMetaToolMockGenerator struct {
	calls int
}

func (m *csvMetaToolMockGenerator) Name() string { return "csv_meta_tool_mock" }

func (m *csvMetaToolMockGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *csvMetaToolMockGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.calls++
	if m.calls == 1 {
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "list_tools",
			Args: map[string]any{},
		}}}, nil
	}
	return ai.GenOutput{Text: "Final answer: Use execute_script for store queries."}, nil
}

type csvMetaToolMockExecutor struct{}

func (e *csvMetaToolMockExecutor) Execute(ctx context.Context, tool string, args map[string]any) (string, error) {
	return "/list_tools\n/execute_script\n", nil
}

func (e *csvMetaToolMockExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return []ai.ToolDefinition{{Name: "list_tools"}}, nil
}

func TestNativeReActEngine_PreservesCSVToolResult(t *testing.T) {
	engine := &NativeReActEngine{}
	gen := &csvLoopMockGenerator{}
	ctx := context.WithValue(context.Background(), ai.CtxKeyDefaultFormat, "csv")
	resp, err := engine.Run(ctx, ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Show me users",
		Executor:     &csvLoopMockExecutor{},
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "name\nJohn Doe\n" {
		t.Fatalf("expected raw CSV tool result, got %q", resp.FinalText)
	}
	if gen.calls != 1 {
		t.Fatalf("expected one generator call for structured output fast path, got %d", gen.calls)
	}
}

func TestNativeReActEngine_DoesNotPreserveMetaToolResultForCSV(t *testing.T) {
	engine := &NativeReActEngine{}
	gen := &csvMetaToolMockGenerator{}
	ctx := context.WithValue(context.Background(), ai.CtxKeyDefaultFormat, "csv")
	resp, err := engine.Run(ctx, ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "How should I query the users store?",
		Executor:     &csvMetaToolMockExecutor{},
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "Final answer: Use execute_script for store queries." {
		t.Fatalf("expected synthesized answer, got %q", resp.FinalText)
	}
	if gen.calls != 2 {
		t.Fatalf("expected two generator calls when meta tool output should not be preserved, got %d", gen.calls)
	}
}

func TestNativeReActEngine_EmitsVerboseProgressByDefault(t *testing.T) {
	engine := &NativeReActEngine{}
	gen := &loopMockGenerator{}
	var progress []string
	ctx := context.WithValue(context.Background(), ai.CtxKeyProgressSink, func(msg string) {
		progress = append(progress, msg)
	})

	_, err := engine.Run(ctx, ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Show me users",
		Executor:     &loopMockExecutor{},
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	want := []string{
		"Planning request with native multi-step loop.",
		"Reasoning iteration 1 of 4.",
		"Waiting for model response.",
		"Calling tool `execute_script`.",
		"Tool `execute_script` completed.",
		"Reasoning iteration 2 of 4.",
		"Waiting for model response.",
		"No further tools required; preparing final answer.",
	}
	if !reflect.DeepEqual(progress, want) {
		t.Fatalf("unexpected progress messages:\nwant=%#v\ngot=%#v", want, progress)
	}
}

func TestNativeReActEngine_RespectsVerboseFalse(t *testing.T) {
	engine := &NativeReActEngine{}
	gen := &loopMockGenerator{}
	var progress []string
	ctx := context.WithValue(context.Background(), ai.CtxKeyProgressSink, func(msg string) {
		progress = append(progress, msg)
	})
	ctx = context.WithValue(ctx, "verbose", false)

	_, err := engine.Run(ctx, ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Show me users",
		Executor:     &loopMockExecutor{},
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if len(progress) != 0 {
		t.Fatalf("expected no progress messages when verbose=false, got %#v", progress)
	}
}

func TestNativeReActEngine_StreamsStructuredToolEvents(t *testing.T) {
	engine := &NativeReActEngine{}
	gen := &loopMockGenerator{}

	type streamedEvent struct {
		eventType string
		payload   map[string]any
	}
	var events []streamedEvent

	_, err := engine.Run(context.Background(), ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Show me users",
		Executor:     &loopMockExecutor{},
		Generator:    gen,
		Streamer: func(eventType string, data any) {
			payload, _ := data.(map[string]any)
			events = append(events, streamedEvent{eventType: eventType, payload: payload})
		},
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 streamed tool lifecycle events, got %#v", events)
	}
	if events[0].eventType != "tool_call" {
		t.Fatalf("expected first event to be tool_call, got %#v", events[0])
	}
	if events[0].payload["tool"] != "execute_script" {
		t.Fatalf("expected execute_script tool call, got %#v", events[0].payload)
	}
	if events[1].eventType != "tool_result" {
		t.Fatalf("expected second event to be tool_result, got %#v", events[1])
	}
	if events[1].payload["tool"] != "execute_script" {
		t.Fatalf("expected execute_script tool result, got %#v", events[1].payload)
	}
	if events[1].payload["result"] != `[{"name":"John Doe"}]` {
		t.Fatalf("expected raw tool result in streamed payload, got %#v", events[1].payload)
	}
}

type recoverableArgErrorGenerator struct {
	calls int
}

func (m *recoverableArgErrorGenerator) Name() string { return "recoverable_arg_error_mock" }

func (m *recoverableArgErrorGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *recoverableArgErrorGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.calls++
	switch m.calls {
	case 1:
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "mint_to_space",
			Args: map[string]any{},
		}}}, nil
	case 2:
		if !strings.Contains(prompt, "Tool execution error: argument 'content' is missing or not a string") {
			return ai.GenOutput{Text: "missing repair context"}, nil
		}
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "mint_to_space",
			Args: map[string]any{
				"kb_name": "Tasks",
				"content": "Task 1: Define scope\nTask 2: Assign owners",
			},
		}}}, nil
	default:
		return ai.GenOutput{Text: "Final answer: Added sample tasks to Tasks."}, nil
	}
}

type recoverableArgErrorExecutor struct {
	callCount int
}

func (e *recoverableArgErrorExecutor) Execute(ctx context.Context, tool string, args map[string]any) (string, error) {
	e.callCount++
	content, _ := args["content"].(string)
	if strings.TrimSpace(content) == "" {
		return "", fmt.Errorf("argument 'content' is missing or not a string")
	}
	return "Successfully minted content to Knowledge Base 'Tasks'.", nil
}

func (e *recoverableArgErrorExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return []ai.ToolDefinition{{Name: "mint_to_space"}}, nil
}

func TestNativeReActEngine_RetriesRecoverableToolArgumentErrors(t *testing.T) {
	engine := &NativeReActEngine{}
	gen := &recoverableArgErrorGenerator{}
	executor := &recoverableArgErrorExecutor{}
	var progress []string
	ctx := context.WithValue(context.Background(), ai.CtxKeyProgressSink, func(msg string) {
		progress = append(progress, msg)
	})

	resp, err := engine.Run(ctx, ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Generate sample tasks and add them to my Tasks space",
		Executor:     executor,
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "Final answer: Added sample tasks to Tasks." {
		t.Fatalf("expected repaired final answer, got %q", resp.FinalText)
	}
	if gen.calls != 3 {
		t.Fatalf("expected three generator calls after recoverable tool error, got %d", gen.calls)
	}
	if executor.callCount != 2 {
		t.Fatalf("expected two tool execution attempts, got %d", executor.callCount)
	}
	if !containsProgressMessage(progress, "Tool `mint_to_space` needs corrected arguments; retrying.") {
		t.Fatalf("expected recoverable retry progress message, got %#v", progress)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "mint_to_space" {
		t.Fatalf("expected only the successful repaired tool call to be recorded, got %#v", resp.ToolCalls)
	}
}

func containsProgressMessage(progress []string, want string) bool {
	for _, message := range progress {
		if message == want {
			return true
		}
	}
	return false
}
