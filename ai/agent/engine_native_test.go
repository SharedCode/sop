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
		if !strings.Contains(prompt, "Tool: mint_to_space") || !strings.Contains(prompt, "Attempted args:") || !strings.Contains(prompt, "Retry instruction:") {
			return ai.GenOutput{Text: "missing structured retry context"}, nil
		}
		if !strings.Contains(prompt, "Repair directive: The last tool call to mint_to_space failed because its arguments were invalid.") {
			return ai.GenOutput{Text: "missing repair directive"}, nil
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

type delayedRepairGenerator struct {
	calls int
}

func (m *delayedRepairGenerator) Name() string { return "delayed_repair_mock" }

func (m *delayedRepairGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *delayedRepairGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.calls++
	switch m.calls {
	case 1:
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "mint_to_space",
			Args: map[string]any{},
		}}}, nil
	case 2:
		if !strings.Contains(prompt, "Retry instruction:") {
			return ai.GenOutput{Text: "missing initial repair guidance"}, nil
		}
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "list_tools",
			Args: map[string]any{},
		}}}, nil
	case 3:
		if !strings.Contains(prompt, "Repair required before continuing.") {
			return ai.GenOutput{Text: "missing enforcement reminder"}, nil
		}
		if !strings.Contains(prompt, "The model attempted list_tools instead.") {
			return ai.GenOutput{Text: "missing attempted tool reminder"}, nil
		}
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "mint_to_space",
			Args: map[string]any{
				"kb_name": "Tasks",
				"content": "Task 1: Define scope",
			},
		}}}, nil
	default:
		return ai.GenOutput{Text: "Final answer: Added sample task to Tasks."}, nil
	}
}

type delayedRepairExecutor struct {
	callCount int
	tools     []string
}

func (e *delayedRepairExecutor) Execute(ctx context.Context, tool string, args map[string]any) (string, error) {
	e.callCount++
	e.tools = append(e.tools, tool)
	if tool == "list_tools" {
		return "mint_to_space, list_tools", nil
	}
	content, _ := args["content"].(string)
	if strings.TrimSpace(content) == "" {
		return "", fmt.Errorf("argument 'content' is missing or not a string")
	}
	return "Successfully minted content to Knowledge Base 'Tasks'.", nil
}

func (e *delayedRepairExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return []ai.ToolDefinition{{Name: "mint_to_space"}, {Name: "list_tools"}}, nil
}

func TestNativeReActEngine_RequiresRepairBeforeSwitchingTools(t *testing.T) {
	engine := &NativeReActEngine{}
	gen := &delayedRepairGenerator{}
	executor := &delayedRepairExecutor{}
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
	if resp.FinalText != "Final answer: Added sample task to Tasks." {
		t.Fatalf("expected repaired final answer, got %q", resp.FinalText)
	}
	if gen.calls != 4 {
		t.Fatalf("expected four generator calls after enforced repair, got %d", gen.calls)
	}
	if executor.callCount != 2 {
		t.Fatalf("expected only failing and repaired tool executions, got %d", executor.callCount)
	}
	if !reflect.DeepEqual(executor.tools, []string{"mint_to_space", "mint_to_space"}) {
		t.Fatalf("expected only mint_to_space to execute, got %#v", executor.tools)
	}
	if !containsProgressMessage(progress, "Tool `mint_to_space` must be corrected before other actions.") {
		t.Fatalf("expected repair enforcement progress message, got %#v", progress)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "mint_to_space" {
		t.Fatalf("expected only the successful repaired tool call to be recorded, got %#v", resp.ToolCalls)
	}
}

func TestFormatRecoverableToolError_IncludesValidationCategoryAndExample(t *testing.T) {
	err := newExecuteScriptValidationError(
		"invalid_filter_placeholder",
		"invalid type for filter condition field \"first_name\": got boolean placeholder true; expected an operator/value predicate",
		`{"op":"filter","args":{"condition":{"first_name":{"$eq":"<value>"}}}}`,
	)

	formatted := formatRecoverableToolError("execute_script", map[string]any{
		"script": []any{map[string]any{"op": "filter"}},
	}, err)

	if !strings.Contains(formatted, "Repair category: invalid_filter_placeholder") {
		t.Fatalf("expected repair category in formatted error, got %q", formatted)
	}
	if !strings.Contains(formatted, "Suggested fix example:") {
		t.Fatalf("expected suggested fix example in formatted error, got %q", formatted)
	}
	if !strings.Contains(formatted, `"first_name":{"$eq":"<value>"}`) {
		t.Fatalf("expected example predicate in formatted error, got %q", formatted)
	}
}

type executeScriptRepairGenerator struct {
	calls int
}

func (m *executeScriptRepairGenerator) Name() string { return "execute_script_repair_mock" }

func (m *executeScriptRepairGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *executeScriptRepairGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.calls++
	switch m.calls {
	case 1:
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "execute_script",
			Args: map[string]any{
				"script": []any{
					map[string]any{
						"op": "filter",
						"args": map[string]any{
							"condition": map[string]any{"first_name": true},
						},
					},
				},
			},
		}}}, nil
	case 2:
		checks := []string{
			"Tool: execute_script",
			"Repair category: invalid_filter_placeholder",
			"Suggested fix example:",
			`"first_name":{"$eq":"<value>"}`,
			"Repair directive: The last tool call to execute_script failed because its arguments were invalid.",
		}
		for _, check := range checks {
			if !strings.Contains(prompt, check) {
				return ai.GenOutput{Text: "missing execute_script retry context: " + check}, nil
			}
		}
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "execute_script",
			Args: map[string]any{
				"script": []any{
					map[string]any{
						"op": "filter",
						"args": map[string]any{
							"condition": map[string]any{"first_name": map[string]any{"$eq": "John"}},
						},
					},
				},
			},
		}}}, nil
	default:
		return ai.GenOutput{Text: "Final answer: Found matching records for John."}, nil
	}
}

type executeScriptRepairExecutor struct {
	callCount int
}

func (e *executeScriptRepairExecutor) Execute(ctx context.Context, tool string, args map[string]any) (string, error) {
	e.callCount++
	if e.callCount == 1 {
		return "", newExecuteScriptValidationError(
			"invalid_filter_placeholder",
			"invalid type for filter condition field \"first_name\": got boolean placeholder true; expected an operator/value predicate",
			`{"op":"filter","args":{"condition":{"first_name":{"$eq":"<value>"}}}}`,
		)
	}
	return `[{"first_name":"John"}]`, nil
}

func (e *executeScriptRepairExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return []ai.ToolDefinition{{Name: "execute_script"}}, nil
}

func TestNativeReActEngine_RetriesExecuteScriptWithValidationGuidance(t *testing.T) {
	engine := &NativeReActEngine{}
	gen := &executeScriptRepairGenerator{}
	executor := &executeScriptRepairExecutor{}
	var progress []string
	ctx := context.WithValue(context.Background(), ai.CtxKeyProgressSink, func(msg string) {
		progress = append(progress, msg)
	})

	resp, err := engine.Run(ctx, ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Find users named John",
		Executor:     executor,
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "Final answer: Found matching records for John." {
		t.Fatalf("expected repaired final answer, got %q", resp.FinalText)
	}
	if gen.calls != 3 {
		t.Fatalf("expected three generator calls after execute_script repair, got %d", gen.calls)
	}
	if executor.callCount != 2 {
		t.Fatalf("expected two execute_script attempts, got %d", executor.callCount)
	}
	if !containsProgressMessage(progress, "Tool `execute_script` needs corrected arguments; retrying.") {
		t.Fatalf("expected execute_script retry progress message, got %#v", progress)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "execute_script" {
		t.Fatalf("expected only the successful repaired execute_script call to be recorded, got %#v", resp.ToolCalls)
	}
}

type executeScriptJoinRepairGenerator struct {
	calls int
}

func (m *executeScriptJoinRepairGenerator) Name() string { return "execute_script_join_repair_mock" }

func (m *executeScriptJoinRepairGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *executeScriptJoinRepairGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.calls++
	switch m.calls {
	case 1:
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "execute_script",
			Args: map[string]any{
				"script": []any{
					map[string]any{
						"op": "join",
						"args": map[string]any{
							"store": "users_orders",
							"on":    map[string]any{"users.key": true},
						},
					},
				},
			},
		}}}, nil
	case 2:
		checks := []string{
			"Tool: execute_script",
			"Repair category: invalid_join_on_placeholder",
			"Suggested fix example:",
			`"on":{"users.key":"key"}`,
			"Repair directive: The last tool call to execute_script failed because its arguments were invalid.",
		}
		for _, check := range checks {
			if !strings.Contains(prompt, check) {
				return ai.GenOutput{Text: "missing execute_script join retry context: " + check}, nil
			}
		}
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{
			Name: "execute_script",
			Args: map[string]any{
				"script": []any{
					map[string]any{
						"op": "join",
						"args": map[string]any{
							"store": "users_orders",
							"on":    map[string]any{"users.key": "key"},
						},
					},
				},
			},
		}}}, nil
	default:
		return ai.GenOutput{Text: "Final answer: Joined users with users_orders successfully."}, nil
	}
}

type executeScriptJoinRepairExecutor struct {
	callCount int
}

func (e *executeScriptJoinRepairExecutor) Execute(ctx context.Context, tool string, args map[string]any) (string, error) {
	e.callCount++
	if e.callCount == 1 {
		return "", newExecuteScriptValidationError(
			"invalid_join_on_placeholder",
			"invalid type for join.on[\"users.key\"]: got boolean placeholder true; expected a field path string such as \"key\"",
			`{"op":"join","args":{"store":"users_orders","on":{"users.key":"key"}}}`,
		)
	}
	return `[{"users.key":"u1","users_orders.value":"o1"}]`, nil
}

func (e *executeScriptJoinRepairExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return []ai.ToolDefinition{{Name: "execute_script"}}, nil
}

func TestNativeReActEngine_RetriesExecuteScriptJoinWithValidationGuidance(t *testing.T) {
	engine := &NativeReActEngine{}
	gen := &executeScriptJoinRepairGenerator{}
	executor := &executeScriptJoinRepairExecutor{}
	var progress []string
	ctx := context.WithValue(context.Background(), ai.CtxKeyProgressSink, func(msg string) {
		progress = append(progress, msg)
	})

	resp, err := engine.Run(ctx, ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Join users with users_orders",
		Executor:     executor,
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "Final answer: Joined users with users_orders successfully." {
		t.Fatalf("expected repaired final answer, got %q", resp.FinalText)
	}
	if gen.calls != 3 {
		t.Fatalf("expected three generator calls after execute_script join repair, got %d", gen.calls)
	}
	if executor.callCount != 2 {
		t.Fatalf("expected two execute_script attempts, got %d", executor.callCount)
	}
	if !containsProgressMessage(progress, "Tool `execute_script` needs corrected arguments; retrying.") {
		t.Fatalf("expected execute_script retry progress message, got %#v", progress)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "execute_script" {
		t.Fatalf("expected only the successful repaired execute_script call to be recorded, got %#v", resp.ToolCalls)
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
