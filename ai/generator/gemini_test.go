package generator

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestDescribeGeminiEmptyResponse_IncludesPromptFeedback(t *testing.T) {
	resp := geminiResponse{
		PromptFeedback: &struct {
			BlockReason string `json:"blockReason,omitempty"`
		}{BlockReason: "SAFETY"},
	}

	msg := describeGeminiEmptyResponse(resp)
	if msg != "no candidates returned from gemini; block_reason=SAFETY" {
		t.Fatalf("unexpected message: %s", msg)
	}
}

func TestDescribeGeminiEmptyResponse_IncludesFinishReason(t *testing.T) {
	resp := geminiResponse{
		Candidates: []struct {
			FinishReason string `json:"finishReason,omitempty"`
			Content      struct {
				Parts []geminiPart `json:"parts"`
			} `json:"content"`
		}{
			{FinishReason: "MAX_TOKENS"},
		},
	}

	msg := describeGeminiEmptyResponse(resp)
	if msg != "no candidates returned from gemini; finish_reason=MAX_TOKENS" {
		t.Fatalf("unexpected message: %s", msg)
	}
}

func TestBuildGeminiRequest_IncludesGenerationConfigAndTools(t *testing.T) {
	req := buildGeminiRequest("find users", ai.GenOptions{
		SystemPrompt: "system",
		MaxTokens:    321,
		Temperature:  0.15,
		TopP:         0.9,
		Tools: []ai.ToolDefinition{{
			Name:        "execute_script",
			Description: "Executes a script",
			Schema:      `{"type":"object","properties":{"script":{"type":"array"}}}`,
		}},
	})

	if req.GenerationConfig == nil {
		t.Fatal("expected generation config to be included")
	}
	if req.GenerationConfig.Temperature != 0.15 {
		t.Fatalf("expected temperature 0.15, got %v", req.GenerationConfig.Temperature)
	}
	if req.GenerationConfig.TopP != 0.9 {
		t.Fatalf("expected topP 0.9, got %v", req.GenerationConfig.TopP)
	}
	if req.GenerationConfig.MaxOutputTokens != 321 {
		t.Fatalf("expected max output tokens 321, got %d", req.GenerationConfig.MaxOutputTokens)
	}
	if req.SystemInstruction == nil || len(req.SystemInstruction.Parts) != 1 || req.SystemInstruction.Parts[0].Text != "system" {
		t.Fatalf("expected system instruction to be preserved, got %#v", req.SystemInstruction)
	}
	if len(req.Tools) != 1 || len(req.Tools[0].FunctionDeclarations) != 1 {
		t.Fatalf("expected one function declaration, got %#v", req.Tools)
	}
	if req.Tools[0].FunctionDeclarations[0].Name != "execute_script" {
		t.Fatalf("expected execute_script declaration, got %#v", req.Tools[0].FunctionDeclarations[0])
	}
	if req.ToolConfig == nil || req.ToolConfig.FunctionCallingConfig == nil {
		t.Fatalf("expected Gemini tool config to be included, got %#v", req.ToolConfig)
	}
	if req.ToolConfig.FunctionCallingConfig.Mode != "VALIDATED" {
		t.Fatalf("expected VALIDATED function-calling mode, got %#v", req.ToolConfig.FunctionCallingConfig)
	}
}

func TestBuildGeminiRequest_OmitsToolConfigWithoutTools(t *testing.T) {
	req := buildGeminiRequest("find users", ai.GenOptions{SystemPrompt: "system"})

	if req.ToolConfig != nil {
		t.Fatalf("expected no Gemini tool config when no tools are present, got %#v", req.ToolConfig)
	}
	if len(req.Tools) != 0 {
		t.Fatalf("expected no Gemini tools when none were provided, got %#v", req.Tools)
	}
}

func TestBuildGeminiRequest_IncludesToolCallContinuation(t *testing.T) {
	req := buildGeminiRequest("continue after tool", ai.GenOptions{
		ToolCallContinuations: []ai.ToolCallContinuation{{
			ToolCall: ai.ToolCall{
				Name:     "list_stores",
				Args:     map[string]any{"store_names": []any{"users"}},
				NativeID: "call_abc123",
				TransportMeta: map[string]any{
					"provider":          "gemini",
					"function_call_id":  "call_abc123",
					"thought_signature": "signature_abc123",
				},
			},
			Response: map[string]any{"stores": []any{map[string]any{"name": "users"}}},
		}},
	})

	if len(req.Contents) != 3 {
		t.Fatalf("expected prompt plus function call/response continuation, got %#v", req.Contents)
	}
	if req.Contents[0].Role != "model" || req.Contents[0].Parts[0].FunctionCall == nil {
		t.Fatalf("expected first content to carry the model functionCall continuation, got %#v", req.Contents[0])
	}
	if req.Contents[0].Parts[0].FunctionCall.ID != "call_abc123" {
		t.Fatalf("expected functionCall id to round-trip, got %#v", req.Contents[0].Parts[0].FunctionCall)
	}
	if req.Contents[0].Parts[0].ThoughtSignature != "signature_abc123" {
		t.Fatalf("expected thought signature to round-trip, got %#v", req.Contents[0].Parts[0])
	}
	if req.Contents[1].Role != "user" || req.Contents[1].Parts[0].FunctionResponse == nil {
		t.Fatalf("expected second content to carry the user functionResponse continuation, got %#v", req.Contents[1])
	}
	if req.Contents[1].Parts[0].FunctionResponse.ID != "call_abc123" {
		t.Fatalf("expected functionResponse id to match functionCall id, got %#v", req.Contents[1].Parts[0].FunctionResponse)
	}
	if req.Contents[1].Parts[0].FunctionResponse.Name != "list_stores" {
		t.Fatalf("expected functionResponse name to match tool name, got %#v", req.Contents[1].Parts[0].FunctionResponse)
	}
	if req.Contents[2].Role != "user" || req.Contents[2].Parts[0].Text != "continue after tool" {
		t.Fatalf("expected final content to carry the resumed prompt, got %#v", req.Contents[2])
	}
}

func TestGeminiReActTurnStrategy_UsesContinuationFirstPrompt(t *testing.T) {
	g := &gemini{}
	provider, ok := any(g).(ai.ReActTurnStrategyProvider)
	if !ok {
		t.Fatalf("expected gemini generator to provide ReAct turn strategy")
	}
	turn := provider.ReActTurnStrategy().PrepareTurn(context.Background(), ai.ReActTurn{
		Iteration:       2,
		UserQuery:       "Find orders for John",
		Prompt:          "full synthetic retry frame",
		RepairDirective: "Repair directive: Retry execute_script with corrected arguments.",
		Options: ai.GenOptions{
			ToolCallContinuations: []ai.ToolCallContinuation{{
				ToolCall: ai.ToolCall{Name: "execute_script", NativeID: "call_123"},
				Response: map[string]any{"status": "error"},
			}},
		},
		ToolResults: []ai.ReActToolResult{{Name: "execute_script", Result: "repair required"}},
	})
	if strings.Contains(turn.Prompt, "full synthetic retry frame") {
		t.Fatalf("expected Gemini ReAct turn strategy to replace synthetic retry frame, got %q", turn.Prompt)
	}
	if !strings.Contains(turn.Prompt, "Continue from the supplied tool-call state.") {
		t.Fatalf("expected reduced continuation-first Gemini prompt, got %q", turn.Prompt)
	}
	if strings.Contains(turn.Prompt, "Original user query:") {
		t.Fatalf("expected original user query to move into carried state, got %q", turn.Prompt)
	}
	if strings.Contains(turn.Prompt, "Latest tool:") {
		t.Fatalf("expected latest tool summary to move into carried state, got %q", turn.Prompt)
	}
	if strings.Contains(turn.Prompt, "Repair directive:") {
		t.Fatalf("expected repair directive to move into carried state, got %q", turn.Prompt)
	}
	if len(turn.Options.ToolCallContinuations) != 1 {
		t.Fatalf("expected one carried tool-call continuation, got %#v", turn.Options.ToolCallContinuations)
	}
	response, ok := turn.Options.ToolCallContinuations[0].Response.(map[string]any)
	if !ok {
		t.Fatalf("expected structured continuation response envelope, got %#v", turn.Options.ToolCallContinuations[0].Response)
	}
	if response["tool_result"] == nil {
		t.Fatalf("expected tool_result in continuation response envelope, got %#v", response)
	}
	reactState, ok := response["react_state"].(map[string]any)
	if !ok {
		t.Fatalf("expected react_state in continuation response envelope, got %#v", response)
	}
	if reactState["user_query"] != "Find orders for John" {
		t.Fatalf("expected user query in carried state, got %#v", reactState)
	}
	if reactState["latest_tool"] != "execute_script" {
		t.Fatalf("expected latest tool in carried state, got %#v", reactState)
	}
	if reactState["repair_directive"] != "Repair directive: Retry execute_script with corrected arguments." {
		t.Fatalf("expected repair directive in carried state, got %#v", reactState)
	}
	if reactState["task_status"] != "repair_required" {
		t.Fatalf("expected repair task status in carried state, got %#v", reactState)
	}
	if reactState["phase"] != string(ai.ReActLoopPhaseRepair) {
		t.Fatalf("expected repair phase in carried state, got %#v", reactState)
	}
	actions, ok := reactState["allowed_next_actions"].([]string)
	if !ok {
		t.Fatalf("expected allowed_next_actions in carried state, got %#v", reactState)
	}
	if len(actions) != 2 || actions[0] != string(ai.ReActNextActionRetrySameTool) || actions[1] != string(ai.ReActNextActionAskClarification) {
		t.Fatalf("expected repair actions in carried state, got %#v", actions)
	}
}

func TestGeminiReActTurnStrategy_UsesReducedClarificationPromptOnFinalTurn(t *testing.T) {
	g := &gemini{}
	provider, ok := any(g).(ai.ReActTurnStrategyProvider)
	if !ok {
		t.Fatalf("expected gemini generator to provide ReAct turn strategy")
	}
	turn := provider.ReActTurnStrategy().PrepareTurn(context.Background(), ai.ReActTurn{
		Iteration:       4,
		UserQuery:       "Find orders for John",
		Prompt:          "full retry-cap clarification frame",
		RepairDirective: "Repair directive: Retry execute_script with corrected arguments.",
		Options: ai.GenOptions{
			ToolCallContinuations: []ai.ToolCallContinuation{{
				ToolCall: ai.ToolCall{Name: "execute_script", NativeID: "call_123"},
				Response: map[string]any{"status": "error"},
			}},
		},
		ToolResults: []ai.ReActToolResult{{Name: "execute_script", Result: "repair required"}},
		FinalTurn:   true,
	})
	if strings.Contains(turn.Prompt, "full retry-cap clarification frame") {
		t.Fatalf("expected Gemini final turn strategy to replace retry-cap frame, got %q", turn.Prompt)
	}
	if !strings.Contains(turn.Prompt, "do not call more tools") {
		t.Fatalf("expected reduced clarification prompt on final turn, got %q", turn.Prompt)
	}
	response, ok := turn.Options.ToolCallContinuations[0].Response.(map[string]any)
	if !ok {
		t.Fatalf("expected structured continuation response envelope, got %#v", turn.Options.ToolCallContinuations[0].Response)
	}
	reactState, ok := response["react_state"].(map[string]any)
	if !ok {
		t.Fatalf("expected react_state in continuation response envelope, got %#v", response)
	}
	if reactState["task_status"] != "clarification_required" {
		t.Fatalf("expected clarification task status in carried state, got %#v", reactState)
	}
	if reactState["phase"] != string(ai.ReActLoopPhaseClarification) {
		t.Fatalf("expected clarification phase in carried state, got %#v", reactState)
	}
	if reactState["has_more_tool_work"] != false {
		t.Fatalf("expected final turn to disable more tool work, got %#v", reactState)
	}
	actions, ok := reactState["allowed_next_actions"].([]string)
	if !ok {
		t.Fatalf("expected allowed_next_actions in carried state, got %#v", reactState)
	}
	if len(actions) != 2 || actions[0] != string(ai.ReActNextActionAskClarification) || actions[1] != string(ai.ReActNextActionAnswerUser) {
		t.Fatalf("expected clarification actions in carried state, got %#v", actions)
	}
	if reactState["iteration"] != 4 {
		t.Fatalf("expected iteration in carried state, got %#v", reactState)
	}
}

type geminiOwnedLoopTestGenerator struct {
	calls int
}

func (m *geminiOwnedLoopTestGenerator) Name() string { return "gemini_owned_loop_test" }

func (m *geminiOwnedLoopTestGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *geminiOwnedLoopTestGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	_ = ctx
	m.calls++
	switch m.calls {
	case 1:
		if !strings.Contains(prompt, "User Query: Find John") {
			return ai.GenOutput{Text: "missing initial query prompt"}, nil
		}
		if len(opts.ToolCallContinuations) != 0 {
			return ai.GenOutput{Text: "unexpected initial continuations"}, nil
		}
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{Name: "list_stores", Args: map[string]any{}}}}, nil
	case 2:
		if len(opts.ToolCallContinuations) != 1 {
			return ai.GenOutput{Text: fmt.Sprintf("expected one continuation, got %d", len(opts.ToolCallContinuations))}, nil
		}
		response, ok := opts.ToolCallContinuations[0].Response.(map[string]any)
		if !ok {
			return ai.GenOutput{Text: fmt.Sprintf("expected structured continuation response, got %#v", opts.ToolCallContinuations[0].Response)}, nil
		}
		reactState, ok := response["react_state"].(map[string]any)
		if !ok {
			return ai.GenOutput{Text: fmt.Sprintf("expected react_state in continuation response, got %#v", response)}, nil
		}
		if reactState["phase"] != string(ai.ReActLoopPhaseActive) {
			return ai.GenOutput{Text: fmt.Sprintf("expected active phase, got %#v", reactState)}, nil
		}
		actions, ok := reactState["allowed_next_actions"].([]string)
		if !ok || len(actions) != 2 || actions[0] != string(ai.ReActNextActionCallTool) || actions[1] != string(ai.ReActNextActionAnswerUser) {
			return ai.GenOutput{Text: fmt.Sprintf("unexpected allowed actions: %#v", reactState)}, nil
		}
		remaining, ok := reactState["remaining_tool_calls"].(int)
		if !ok || remaining != 2 {
			return ai.GenOutput{Text: fmt.Sprintf("expected remaining_tool_calls=2, got %#v", reactState)}, nil
		}
		return ai.GenOutput{
			ToolCalls: []ai.ToolCall{{
				Name: "execute_script",
				Args: map[string]any{
					"script": []any{map[string]any{"op": "scan"}},
				},
			}},
		}, nil
	case 3:
		if len(opts.ToolCallContinuations) != 2 {
			return ai.GenOutput{Text: fmt.Sprintf("expected two continuations, got %d", len(opts.ToolCallContinuations))}, nil
		}
		if !strings.Contains(prompt, "Continue from the supplied tool-call state.") {
			return ai.GenOutput{Text: "missing continuation prompt"}, nil
		}
		return ai.GenOutput{Text: "Final answer: Found John"}, nil
	default:
		return ai.GenOutput{Text: "unexpected extra call"}, nil
	}
}

type geminiOwnedLoopTestExecutor struct{}

func (e *geminiOwnedLoopTestExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	_ = ctx
	_ = args
	if toolName == "list_stores" {
		return `{"stores":[{"name":"users"}]}`, nil
	}
	return `{"tool_result":[{"name":"John"}]}`, nil
}

func (e *geminiOwnedLoopTestExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	_ = ctx
	return []ai.ToolDefinition{{Name: "list_stores"}, {Name: "execute_script"}}, nil
}

func TestGeminiOwnedReActLoop_AccumulatesToolCallContinuationsAcrossTurns(t *testing.T) {
	gen := &geminiOwnedLoopTestGenerator{}
	loop := geminiOwnedReActLoop{
		generator:     gen,
		maxIterations: 3,
	}
	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Find John",
		Executor:     &geminiOwnedLoopTestExecutor{},
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "Final answer: Found John" {
		t.Fatalf("expected final answer, got %q", resp.FinalText)
	}
	if len(resp.ToolCalls) != 2 {
		t.Fatalf("expected two tool calls, got %#v", resp.ToolCalls)
	}
}

type geminiOwnedLoopMultiToolGenerator struct {
	calls int
}

func (m *geminiOwnedLoopMultiToolGenerator) Name() string { return "gemini_owned_loop_multi_tool_test" }

func (m *geminiOwnedLoopMultiToolGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *geminiOwnedLoopMultiToolGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	_ = ctx
	m.calls++
	switch m.calls {
	case 1:
		if len(opts.ToolCallContinuations) != 0 {
			return ai.GenOutput{Text: "unexpected initial continuations"}, nil
		}
		return ai.GenOutput{ToolCalls: []ai.ToolCall{
			{Name: "list_stores", Args: map[string]any{}},
			{Name: "execute_script", Args: map[string]any{"script": []any{map[string]any{"op": "scan"}}}},
		}}, nil
	case 2:
		if len(opts.ToolCallContinuations) != 2 {
			return ai.GenOutput{Text: fmt.Sprintf("expected two continuations, got %d", len(opts.ToolCallContinuations))}, nil
		}
		if opts.ToolCallContinuations[0].ToolCall.Name != "list_stores" || opts.ToolCallContinuations[1].ToolCall.Name != "execute_script" {
			return ai.GenOutput{Text: fmt.Sprintf("unexpected continuation order: %#v", opts.ToolCallContinuations)}, nil
		}
		return ai.GenOutput{Text: "Final answer after multi-tool turn"}, nil
	default:
		return ai.GenOutput{Text: "unexpected extra call"}, nil
	}
}

func TestGeminiOwnedReActLoop_ExecutesAllToolCallsInATurn(t *testing.T) {
	gen := &geminiOwnedLoopMultiToolGenerator{}
	loop := geminiOwnedReActLoop{
		generator:     gen,
		maxIterations: 2,
	}
	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Find John",
		Executor:     &geminiOwnedLoopTestExecutor{},
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "Final answer after multi-tool turn" {
		t.Fatalf("expected final answer, got %q", resp.FinalText)
	}
	if len(resp.ToolCalls) != 2 {
		t.Fatalf("expected both tool calls to be executed, got %#v", resp.ToolCalls)
	}
	if resp.ToolCalls[0].Name != "list_stores" || resp.ToolCalls[1].Name != "execute_script" {
		t.Fatalf("unexpected tool call order: %#v", resp.ToolCalls)
	}
}

type geminiOwnedLoopFinalTurnGuardrailGenerator struct {
	calls int
}

func (m *geminiOwnedLoopFinalTurnGuardrailGenerator) Name() string {
	return "gemini_owned_loop_final_turn_guardrail_test"
}

func (m *geminiOwnedLoopFinalTurnGuardrailGenerator) EstimateCost(inTokens, outTokens int) float64 {
	return 0
}

func (m *geminiOwnedLoopFinalTurnGuardrailGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	_ = ctx
	m.calls++
	switch m.calls {
	case 1:
		if len(opts.Tools) == 0 {
			return ai.GenOutput{Text: "expected tools on active turn"}, nil
		}
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{Name: "list_stores", Args: map[string]any{}}}}, nil
	case 2:
		if len(opts.Tools) != 0 {
			return ai.GenOutput{Text: fmt.Sprintf("expected final turn tools to be disabled, got %d", len(opts.Tools))}, nil
		}
		if !strings.Contains(prompt, "do not call more tools") {
			return ai.GenOutput{Text: "missing final-turn hard-stop prompt"}, nil
		}
		if len(opts.ToolCallContinuations) != 1 {
			return ai.GenOutput{Text: fmt.Sprintf("expected one continuation on final turn, got %d", len(opts.ToolCallContinuations))}, nil
		}
		return ai.GenOutput{Text: "Need one clarification before continuing."}, nil
	default:
		return ai.GenOutput{Text: "unexpected extra call"}, nil
	}
}

func TestGeminiOwnedReActLoop_DisablesToolsOnFinalTurn(t *testing.T) {
	gen := &geminiOwnedLoopFinalTurnGuardrailGenerator{}
	loop := geminiOwnedReActLoop{
		generator:     gen,
		maxIterations: 1,
	}
	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Find John",
		Executor:     &geminiOwnedLoopTestExecutor{},
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "Need one clarification before continuing." {
		t.Fatalf("expected clarification text, got %q", resp.FinalText)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "list_stores" {
		t.Fatalf("unexpected executed tool calls: %#v", resp.ToolCalls)
	}
}

type geminiOwnedLoopStreamingGenerator struct {
	calls int
}

func (m *geminiOwnedLoopStreamingGenerator) Name() string { return "gemini_owned_loop_streaming_test" }

func (m *geminiOwnedLoopStreamingGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *geminiOwnedLoopStreamingGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	_ = ctx
	_ = prompt
	_ = opts
	m.calls++
	if m.calls == 1 {
		return ai.GenOutput{ToolCalls: []ai.ToolCall{{Name: "list_stores", Args: map[string]any{"scope": "users"}}}}, nil
	}
	return ai.GenOutput{Text: "Final answer after streaming"}, nil
}

type geminiOwnedLoopStreamingExecutor struct {
	sawNativeToolHints bool
	sawEventStreamer   bool
}

func (e *geminiOwnedLoopStreamingExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	_ = toolName
	_ = args
	if hinted, ok := ctx.Value(ai.CtxKeyNativeToolHints).(bool); ok && hinted {
		e.sawNativeToolHints = true
	}
	if streamer, ok := ctx.Value(ai.CtxKeyEventStreamer).(func(string, any)); ok && streamer != nil {
		e.sawEventStreamer = true
	}
	return `[{"name":"John Doe"}]`, nil
}

func (e *geminiOwnedLoopStreamingExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	_ = ctx
	return []ai.ToolDefinition{{Name: "list_stores"}}, nil
}

func TestGeminiOwnedReActLoop_StreamsStructuredToolEventsAndExecutionContext(t *testing.T) {
	gen := &geminiOwnedLoopStreamingGenerator{}
	exec := &geminiOwnedLoopStreamingExecutor{}
	loop := geminiOwnedReActLoop{
		generator:     gen,
		maxIterations: 2,
	}

	type streamedEvent struct {
		eventType string
		payload   map[string]any
	}
	var events []streamedEvent

	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Show me users",
		Executor:     exec,
		Generator:    gen,
		Streamer: func(eventType string, data any) {
			payload, _ := data.(map[string]any)
			events = append(events, streamedEvent{eventType: eventType, payload: payload})
		},
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "Final answer after streaming" {
		t.Fatalf("expected final answer, got %q", resp.FinalText)
	}
	if !exec.sawNativeToolHints {
		t.Fatalf("expected native tool hint context to be set")
	}
	if !exec.sawEventStreamer {
		t.Fatalf("expected event streamer context to be set")
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 streamed events, got %#v", events)
	}
	if events[0].eventType != "tool_call" || events[0].payload["tool"] != "list_stores" {
		t.Fatalf("unexpected tool_call event: %#v", events[0])
	}
	if events[1].eventType != "tool_result" || events[1].payload["tool"] != "list_stores" {
		t.Fatalf("unexpected tool_result event: %#v", events[1])
	}
	if events[1].payload["result"] != `[{"name":"John Doe"}]` {
		t.Fatalf("expected raw streamed result payload, got %#v", events[1].payload)
	}
}

type geminiOwnedLoopProgressGenerator struct {
	calls int
}

func (m *geminiOwnedLoopProgressGenerator) Name() string { return "gemini_owned_loop_progress_test" }

func (m *geminiOwnedLoopProgressGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *geminiOwnedLoopProgressGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	_ = ctx
	_ = prompt
	_ = opts
	m.calls++
	if m.calls == 1 {
		return ai.GenOutput{
			ToolCalls: []ai.ToolCall{{
				Name: "execute_script",
				Args: map[string]any{
					"script": []any{map[string]any{"op": "scan"}},
				},
			}},
		}, nil
	}
	return ai.GenOutput{Text: "Done"}, nil
}

type geminiOwnedLoopProgressExecutor struct{}

func (e *geminiOwnedLoopProgressExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	_ = ctx
	_ = toolName
	_ = args
	return `{"tool_result":{"rows":[{"name":"John"}]},"progress_hint":{"status":"progressing","completion_delta":0.25,"tips":["Use execute_script next"],"clues":["users relation confirmed"],"suggested_next_tools":["execute_script"]}}`, nil
}

func (e *geminiOwnedLoopProgressExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	_ = ctx
	return []ai.ToolDefinition{{Name: "execute_script"}}, nil
}

type geminiOwnedLoopProgressInspectStrategy struct {
	t          *testing.T
	inspected  bool
	seenResult bool
}

func (s *geminiOwnedLoopProgressInspectStrategy) PrepareTurn(ctx context.Context, turn ai.ReActTurn) ai.ReActTurn {
	_ = ctx
	if turn.Iteration == 2 {
		s.inspected = true
		if len(turn.ToolResults) != 1 {
			s.t.Fatalf("expected one prior tool result, got %#v", turn.ToolResults)
		}
		result := turn.ToolResults[0]
		if result.Result != `{"rows":[{"name":"John"}]}` {
			s.t.Fatalf("expected unwrapped tool_result payload, got %q", result.Result)
		}
		if result.Hint == nil {
			s.t.Fatalf("expected progress hint on prior tool result")
		}
		if result.Hint.Status != "progressing" || result.Hint.CompletionDelta != 0.25 {
			s.t.Fatalf("unexpected progress hint: %#v", result.Hint)
		}
		if len(result.Hint.Clues) != 1 || result.Hint.Clues[0] != "users relation confirmed" {
			s.t.Fatalf("unexpected progress hint clues: %#v", result.Hint)
		}
	}
	return turn
}

func TestGeminiOwnedReActLoop_UnwrapsProgressHintsIntoLoopState(t *testing.T) {
	gen := &geminiOwnedLoopProgressGenerator{}
	strategy := &geminiOwnedLoopProgressInspectStrategy{t: t}
	loop := geminiOwnedReActLoop{
		generator:     gen,
		turnStrategy:  strategy,
		maxIterations: 2,
	}

	type streamedEvent struct {
		eventType string
		payload   map[string]any
	}
	var events []streamedEvent

	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Find John",
		Executor:     &geminiOwnedLoopProgressExecutor{},
		Generator:    gen,
		Streamer: func(eventType string, data any) {
			payload, _ := data.(map[string]any)
			events = append(events, streamedEvent{eventType: eventType, payload: payload})
		},
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "Done" {
		t.Fatalf("expected final text, got %q", resp.FinalText)
	}
	if !strategy.inspected {
		t.Fatalf("expected strategy to inspect second turn state")
	}
	if len(events) != 2 {
		t.Fatalf("expected streamed tool lifecycle events, got %#v", events)
	}
	hint, ok := events[1].payload["progress_hint"].(*ai.ToolProgressHint)
	if !ok || hint == nil {
		t.Fatalf("expected streamed progress hint, got %#v", events[1].payload)
	}
	if hint.Status != "progressing" || len(hint.SuggestedNextTools) != 1 || hint.SuggestedNextTools[0] != "execute_script" {
		t.Fatalf("unexpected streamed progress hint: %#v", hint)
	}
}

type geminiOwnedLoopTerminalHintGenerator struct{ calls int }

func (m *geminiOwnedLoopTerminalHintGenerator) Name() string { return "gemini_owned_loop_terminal_hint_test" }

func (m *geminiOwnedLoopTerminalHintGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *geminiOwnedLoopTerminalHintGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	_ = ctx
	_ = prompt
	_ = opts
	m.calls++
	if m.calls == 1 {
		return ai.GenOutput{
			ToolCalls: []ai.ToolCall{{
				Name: "execute_script",
				Args: map[string]any{
					"script": []any{map[string]any{"op": "scan"}},
				},
			}},
		}, nil
	}
	return ai.GenOutput{Text: "unexpected follow-up generation after hard error"}, nil
}

type geminiOwnedLoopTerminalHintExecutor struct{ callCount int }

func (e *geminiOwnedLoopTerminalHintExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	_ = ctx
	_ = toolName
	_ = args
	e.callCount++
	return `{"tool_result":"Permission denied: write access is blocked for this operation.","progress_hint":{"status":"hard_error","tips":["Stop retrying this tool until permissions change."]}}`, nil
}

func (e *geminiOwnedLoopTerminalHintExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	_ = ctx
	return []ai.ToolDefinition{{Name: "execute_script"}}, nil
}

func TestGeminiOwnedReActLoop_ShortCircuitsOnTerminalHint(t *testing.T) {
	gen := &geminiOwnedLoopTerminalHintGenerator{}
	exec := &geminiOwnedLoopTerminalHintExecutor{}
	loop := geminiOwnedReActLoop{
		generator:     gen,
		maxIterations: 2,
	}

	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
		SystemPrompt: "You are a test assistant.",
		UserQuery:    "Try the blocked operation.",
		Executor:     exec,
		Generator:    gen,
	})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if resp.FinalText != "Permission denied: write access is blocked for this operation." {
		t.Fatalf("expected terminal tool result to be returned immediately, got %q", resp.FinalText)
	}
	if gen.calls != 1 {
		t.Fatalf("expected hard error hint to stop after the first generation call, got %d", gen.calls)
	}
	if exec.callCount != 1 {
		t.Fatalf("expected only one tool execution before short-circuit, got %d", exec.callCount)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "execute_script" {
		t.Fatalf("expected the terminal tool call to be recorded, got %#v", resp.ToolCalls)
	}
}

func TestBuildGeminiRequest_SanitizesUnsupportedSchemaKeywords(t *testing.T) {
	req := buildGeminiRequest("sanitize", ai.GenOptions{
		Tools: []ai.ToolDefinition{{
			Name:        "execute_script",
			Description: "Executes a script",
			Schema: `{
				"properties": {
					"script": {
						"type": "array",
						"items": {
							"properties": {
								"op": {
									"type": "string",
									"enum": ["scan"],
									"default": "scan"
								}
							},
							"required": ["op"],
							"additionalProperties": false
						},
						"description": "script steps"
					},
					"mode": {
						"oneOf": [{"type": "string"}, {"type": "integer"}],
						"description": "mode selector"
					}
				},
				"required": ["script"],
				"additionalProperties": false,
				"default": {}
			}`,
		}},
	})

	params := req.Tools[0].FunctionDeclarations[0].Parameters
	if params["type"] != "object" {
		t.Fatalf("expected root type to default to object, got %#v", params)
	}
	if _, found := params["additionalProperties"]; found {
		t.Fatalf("expected unsupported root keyword to be removed, got %#v", params)
	}
	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected sanitized properties, got %#v", params)
	}
	mode, ok := props["mode"].(map[string]any)
	if !ok {
		t.Fatalf("expected mode property, got %#v", props)
	}
	if _, found := mode["oneOf"]; found {
		t.Fatalf("expected unsupported nested keyword to be removed, got %#v", mode)
	}
	if mode["description"] != "mode selector" {
		t.Fatalf("expected supported nested description to remain, got %#v", mode)
	}
	if mode["type"] != "string" {
		t.Fatalf("expected missing nested type to fall back to string, got %#v", mode)
	}
	script, ok := props["script"].(map[string]any)
	if !ok {
		t.Fatalf("expected script property, got %#v", props)
	}
	items, ok := script["items"].(map[string]any)
	if !ok {
		t.Fatalf("expected array items to be preserved, got %#v", script)
	}
	if _, found := items["additionalProperties"]; found {
		t.Fatalf("expected unsupported items keyword to be removed, got %#v", items)
	}
	itemProps, ok := items["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested item properties, got %#v", items)
	}
	op, ok := itemProps["op"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested op property, got %#v", itemProps)
	}
	if _, found := op["default"]; found {
		t.Fatalf("expected unsupported nested default to be removed, got %#v", op)
	}
	if op["type"] != "string" {
		t.Fatalf("expected op type to remain, got %#v", op)
	}
	if required, ok := params["required"].([]string); !ok || len(required) != 1 || required[0] != "script" {
		t.Fatalf("expected required fields to be preserved as strings, got %#v", params["required"])
	}
}

func TestBuildGeminiRequest_PreservesStructuredListStoresSchema(t *testing.T) {
	req := buildGeminiRequest("research stores", ai.GenOptions{
		Tools: []ai.ToolDefinition{{
			Name:        "list_stores",
			Description: "Research store schema",
			Schema:      `{"type":"object","properties":{"database":{"type":"string"},"stores":{"type":"array","items":{"type":"string"}}}}`,
		}},
	})

	if len(req.Tools) != 1 || len(req.Tools[0].FunctionDeclarations) != 1 {
		t.Fatalf("expected one function declaration, got %#v", req.Tools)
	}
	params := req.Tools[0].FunctionDeclarations[0].Parameters
	if params["type"] != "object" {
		t.Fatalf("expected object root schema, got %#v", params)
	}
	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected properties to survive sanitization, got %#v", params)
	}
	stores, ok := props["stores"].(map[string]any)
	if !ok {
		t.Fatalf("expected stores property to remain structured, got %#v", props)
	}
	if stores["type"] != "array" {
		t.Fatalf("expected stores to remain an array, got %#v", stores)
	}
	items, ok := stores["items"].(map[string]any)
	if !ok || items["type"] != "string" {
		t.Fatalf("expected stores.items to remain a string schema, got %#v", stores)
	}
}

func TestExtractGeminiOutput_PreservesFunctionCallIDForTransportContinuity(t *testing.T) {
	resp := geminiResponse{
		Candidates: []struct {
			FinishReason string `json:"finishReason,omitempty"`
			Content      struct {
				Parts []geminiPart `json:"parts"`
			} `json:"content"`
		}{
			{
				Content: struct {
					Parts []geminiPart `json:"parts"`
				}{
					Parts: []geminiPart{
						{
							FunctionCall: &geminiFunctionCall{
								Name: "execute_script",
								Args: map[string]any{"script": []any{"scan"}},
								ID:   "call_12345xyz",
							},
							ThoughtSignature: "signature_12345xyz",
						},
					},
				},
			},
		},
	}

	out, err := extractGeminiOutput(resp)
	if err != nil {
		t.Fatalf("extractGeminiOutput failed: %v", err)
	}
	if len(out.ToolCalls) != 1 {
		t.Fatalf("expected one tool call, got %#v", out.ToolCalls)
	}
	if out.ToolCalls[0].NativeID != "call_12345xyz" {
		t.Fatalf("expected Gemini function call id to be preserved, got %#v", out.ToolCalls[0])
	}
	if out.ToolCalls[0].TransportMeta["provider"] != "gemini" {
		t.Fatalf("expected Gemini transport metadata, got %#v", out.ToolCalls[0].TransportMeta)
	}
	if out.ToolCalls[0].TransportMeta["function_call_id"] != "call_12345xyz" {
		t.Fatalf("expected Gemini function_call_id transport metadata, got %#v", out.ToolCalls[0].TransportMeta)
	}
	if out.ToolCalls[0].TransportMeta["thought_signature"] != "signature_12345xyz" {
		t.Fatalf("expected Gemini thought_signature transport metadata, got %#v", out.ToolCalls[0].TransportMeta)
	}
}
