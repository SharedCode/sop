package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/sharedcode/sop/ai"
)

type chatGPTOwnedLoopHydrationStub struct{}

func (chatGPTOwnedLoopHydrationStub) Run(ctx context.Context, req ai.ReasoningRequest) (ai.ReasoningResponse, error) {
	_ = ctx
	resp := chatGPTOwnedLoopResponse("gpt-4o", "Final answer from ChatGPT owned loop", []ai.ToolCall{
		{Name: "tool_1", Args: map[string]any{"step": 1}},
		{Name: "tool_2", Args: map[string]any{"step": 2}},
		{Name: "tool_3", Args: map[string]any{"step": 3}},
		{Name: "tool_4", Args: map[string]any{"step": 4}},
		{Name: "tool_5", Args: map[string]any{"step": 5}},
		{Name: "tool_6", Args: map[string]any{"step": 6}},
		{Name: "tool_7", Args: map[string]any{"step": 7}},
		{Name: "tool_8", Args: map[string]any{"step": 8}},
	}, []string{"confirmed users store"}, nil, "thread_123")
	emitChatGPTOwnedLoopHydration(req, ai.MemoryHydrationUpdate{
		FinalText:      resp.FinalText,
		ToolCalls:      resp.ToolCalls,
		OutcomeFacts:   resp.OutcomeFacts,
		CarryoverState: resp.CarryoverState,
	})
	resp.ToolCalls[7].Name = "mutated_tool"
	resp.CarryoverState.LastToolNames[0] = "mutated_tool"
	resp.CarryoverState.LastOutcomeFacts[0] = "mutated fact"
	return resp, nil
}

func TestChatGPTOwnedLoop_UsesSharedHydrationContract(t *testing.T) {
	gen := &chatgpt{model: "gpt-4o", ownedLoop: chatGPTOwnedLoopHydrationStub{}}
	var captured ai.MemoryHydrationUpdate

	loop := gen.ReActLoop()
	if loop == nil {
		t.Fatal("expected owned loop seam to be available")
	}

	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
		Generator: gen,
		UserQuery: "Find John",
		HydrationSink: func(update ai.MemoryHydrationUpdate) {
			captured = update
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if resp.FinalText != "Final answer from ChatGPT owned loop" {
		t.Fatalf("expected owned loop final answer, got %q", resp.FinalText)
	}
	if captured.FinalText != "Final answer from ChatGPT owned loop" {
		t.Fatalf("expected hydration final text to round-trip, got %q", captured.FinalText)
	}
	if len(captured.ToolCalls) != 6 {
		t.Fatalf("expected hydration helper to retain only the last 6 tool calls, got %d", len(captured.ToolCalls))
	}
	for index, toolCall := range captured.ToolCalls {
		expectedName := fmt.Sprintf("tool_%d", index+3)
		if toolCall.Name != expectedName {
			t.Fatalf("expected bounded hydration tool call %q at index %d, got %+v", expectedName, index, captured.ToolCalls)
		}
	}
	if captured.ToolCalls[5].Name != "tool_8" {
		t.Fatalf("expected captured hydration update to be cloned before source mutation, got %+v", captured.ToolCalls)
	}
	if captured.CarryoverState == nil {
		t.Fatal("expected carryover state in hydration update")
	}
	if captured.CarryoverState.ConversationHandle != "thread_123" {
		t.Fatalf("expected carryover conversation handle in hydration update, got %+v", captured.CarryoverState)
	}
	if len(captured.CarryoverState.LastToolNames) != ai.MemoryHydrationToolCallLimit {
		t.Fatalf("expected bounded carryover tool names in hydration update, got %+v", captured.CarryoverState)
	}
	if captured.CarryoverState.LastToolNames[0] != "tool_3" || captured.CarryoverState.LastToolNames[len(captured.CarryoverState.LastToolNames)-1] != "tool_8" {
		t.Fatalf("expected cloned carryover state in hydration update, got %+v", captured.CarryoverState)
	}
	if captured.CarryoverState.LastOutcomeFacts[0] != "confirmed users store" {
		t.Fatalf("expected cloned carryover facts in hydration update, got %+v", captured.CarryoverState)
	}
}

func TestChatGPTOwnedLoopResponse_UsesConversationHandleInCarryoverState(t *testing.T) {
	resp := chatGPTOwnedLoopResponse("gpt-4o", "answer", []ai.ToolCall{{Name: "tool_1"}}, []string{"confirmed users store"}, []ai.LearnedRecipe{{ID: "recipe_users_lookup"}}, "thread_123")
	if resp.CarryoverState == nil {
		t.Fatal("expected carryover state in owned loop response")
	}
	if resp.CarryoverState.Mode != ai.CarryoverModeLive {
		t.Fatalf("expected live carryover mode when a response handle exists, got %+v", resp.CarryoverState)
	}
	if resp.CarryoverState.Provider != "chatgpt" || resp.CarryoverState.Model != "gpt-4o" {
		t.Fatalf("expected provider/model metadata in carryover state, got %+v", resp.CarryoverState)
	}
	if resp.CarryoverState.ConversationHandle != "thread_123" {
		t.Fatalf("expected conversation handle in carryover state, got %+v", resp.CarryoverState)
	}
	if resp.CarryoverState.LastAssistantSummary != "answer" {
		t.Fatalf("expected last assistant summary in carryover state, got %+v", resp.CarryoverState)
	}
	if resp.CarryoverState.EstimatedRawToolTokens <= 0 {
		t.Fatalf("expected raw tool estimate in carryover state, got %+v", resp.CarryoverState)
	}
}

func TestChatGPTReActLoop_DefaultsToOwnedLoop(t *testing.T) {
	gen := &chatgpt{model: "gpt-4o", newOwnedLoop: newChatGPTOwnedReActLoop, supportsLive: true}
	if _, ok := gen.ReActLoop().(chatGPTOwnedReActLoop); !ok {
		t.Fatalf("expected owned loop by default, got %#v", gen.ReActLoop())
	}
}

func TestChatGPTReActLoop_UsesFactoryScaffoldWhenProvided(t *testing.T) {
	gen := &chatgpt{
		model: "gpt-4o",
		newOwnedLoop: func(g *chatgpt) ai.ReActLoop {
			if g.model != "gpt-4o" {
				t.Fatalf("expected factory to receive generator state, got model %q", g.model)
			}
			return newChatGPTOwnedReActLoop(g)
		},
	}
	loop := gen.ReActLoop()
	if loop == nil {
		t.Fatal("expected owned loop from scaffold factory")
	}
	ownedLoop, ok := loop.(chatGPTOwnedReActLoop)
	if !ok {
		t.Fatalf("expected scaffold factory loop type, got %#v", loop)
	}
	if ownedLoop.generator != gen {
		t.Fatalf("expected scaffold loop to retain generator pointer, got %#v", ownedLoop.generator)
	}
}

func TestChatGPTFactory_EnablesOwnedLoopByDefault(t *testing.T) {
	created, err := New("chatgpt", map[string]any{"api_key": "test-key", "model": "gpt-4o"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	gen, ok := created.(*chatgpt)
	if !ok {
		t.Fatalf("expected chatgpt generator, got %#v", created)
	}
	if _, ok := gen.ReActLoop().(chatGPTOwnedReActLoop); !ok {
		t.Fatalf("expected owned loop by default, got %#v", gen.ReActLoop())
	}
	if capability := gen.CarryoverCapability(); !capability.SupportsLive {
		t.Fatalf("expected live carryover by default, got %+v", capability)
	}
}

func TestChatGPTFactory_DisablesOwnedLoopScaffoldFromConfig(t *testing.T) {
	created, err := New("chatgpt", map[string]any{
		"api_key":                    "test-key",
		"model":                      "gpt-4o",
		"enable_owned_loop_scaffold": false,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	gen, ok := created.(*chatgpt)
	if !ok {
		t.Fatalf("expected chatgpt generator, got %#v", created)
	}
	if loop := gen.ReActLoop(); loop != nil {
		t.Fatalf("expected explicit config disable to turn off owned loop, got %#v", loop)
	}
	if capability := gen.CarryoverCapability(); capability.SupportsLive {
		t.Fatalf("expected explicit config disable to turn off live carryover, got %+v", capability)
	}
}

func TestChatGPTFactory_DisablesOwnedLoopScaffoldFromEnv(t *testing.T) {
	t.Setenv("OPENAI_ENABLE_OWNED_LOOP_SCAFFOLD", "false")
	created, err := New("chatgpt", map[string]any{"api_key": "test-key", "model": "gpt-4o"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	gen, ok := created.(*chatgpt)
	if !ok {
		t.Fatalf("expected chatgpt generator, got %#v", created)
	}
	if loop := gen.ReActLoop(); loop != nil {
		t.Fatalf("expected env disable to turn off owned loop, got %#v", loop)
	}
	if capability := gen.CarryoverCapability(); capability.SupportsLive {
		t.Fatalf("expected env disable to turn off live carryover, got %+v", capability)
	}
}

func TestChatGPTFactory_UsesConfiguredAPIURL(t *testing.T) {
	created, err := New("chatgpt", map[string]any{
		"api_key": "test-key",
		"model":   "gpt-5.4",
		"api_url": "http://127.0.0.1:3030/v1",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	gen, ok := created.(*chatgpt)
	if !ok {
		t.Fatalf("expected chatgpt generator, got %#v", created)
	}
	if gen.apiURL != "http://127.0.0.1:3030/v1" {
		t.Fatalf("expected api url to round-trip, got %q", gen.apiURL)
	}
	if got := gen.responsesURL(); got != "http://127.0.0.1:3030/v1/responses" {
		t.Fatalf("expected responses url to use configured base url, got %q", got)
	}
	if got := (&chatgpt{apiURL: "http://127.0.0.1:3030/v1/"}).responsesURL(); got != "http://127.0.0.1:3030/v1/responses" {
		t.Fatalf("expected trailing slash to be trimmed, got %q", got)
	}
}

func TestChatGPTCreateResponses_UsesConfiguredAPIURL(t *testing.T) {
	var capturedPath string
	var capturedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(openAIResponsesResponse{
			ID:         "resp_123",
			OutputText: "OK",
		})
	}))
	defer server.Close()

	gen := &chatgpt{apiKey: "test-key", model: "gpt-5.4", apiURL: server.URL + "/v1"}
	response, err := gen.createResponses(context.Background(), openAIResponsesRequest{Model: "gpt-5.4"})
	if err != nil {
		t.Fatalf("createResponses() error = %v", err)
	}
	if capturedPath != "/v1/responses" {
		t.Fatalf("expected configured path to be used, got %q", capturedPath)
	}
	if capturedAuth != "Bearer test-key" {
		t.Fatalf("expected bearer auth header, got %q", capturedAuth)
	}
	if response.ID != "resp_123" {
		t.Fatalf("expected response to decode, got %#v", response)
	}
}

func TestChatGPTGenerate_UsesConfiguredAPIURL(t *testing.T) {
	var capturedPath string
	var capturedAuth string
	var capturedMessages []openAIMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedAuth = r.Header.Get("Authorization")
		var req openAIRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}
		capturedMessages = req.Messages
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(openAIResponse{
			Choices: []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
				FinishReason string `json:"finish_reason"`
			}{
				{Message: struct {
					Content string `json:"content"`
				}{Content: "OK"}, FinishReason: "stop"},
			},
		})
	}))
	defer server.Close()

	gen := &chatgpt{apiKey: "test-key", model: "gpt-5.4", apiURL: server.URL + "/v1"}
	output, err := gen.Generate(context.Background(), "Reply with OK only.", ai.GenOptions{MaxTokens: 16, SystemPrompt: "Return JSON only."})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if capturedPath != "/v1/chat/completions" {
		t.Fatalf("expected configured chat completions path to be used, got %q", capturedPath)
	}
	if capturedAuth != "Bearer test-key" {
		t.Fatalf("expected bearer auth header, got %q", capturedAuth)
	}
	if len(capturedMessages) != 2 || capturedMessages[0].Role != "system" || capturedMessages[0].Content != "Return JSON only." || capturedMessages[1].Role != "user" || capturedMessages[1].Content != "Reply with OK only." {
		t.Fatalf("expected system and user messages to be sent, got %#v", capturedMessages)
	}
	if output.Text != "OK" {
		t.Fatalf("expected response text to decode, got %#v", output)
	}
}

func TestChatGPTOwnedReActLoop_DefaultRunReturnsExplicitScaffoldError(t *testing.T) {
	loop := chatGPTOwnedReActLoop{
		generator:     &chatgpt{model: "gpt-4o", apiKey: "test-key"},
		maxIterations: 1,
		create: func(ctx context.Context, request openAIResponsesRequest) (openAIResponsesResponse, error) {
			_ = ctx
			_ = request
			return openAIResponsesResponse{}, errors.New("transport offline")
		},
	}
	_, err := loop.Run(context.Background(), ai.ReasoningRequest{UserQuery: "Find John"})
	if err == nil || !strings.Contains(err.Error(), "transport offline") {
		t.Fatalf("expected transport attempt error, got %v", err)
	}
}

func TestBuildChatGPTResponsesRequest_UsesPreviousResponseIDAndMapsTools(t *testing.T) {
	request, err := buildChatGPTResponsesRequest(ai.ReasoningRequest{
		SystemPrompt:   "You are a test assistant.",
		ContextText:    "Focused context",
		HistoryText:    "Prior exchange",
		UserQuery:      "Find John",
		CarryoverState: &ai.CarryoverState{ConversationHandle: "resp_prev_123"},
	}, "gpt-5.4", []ai.ToolDefinition{{
		Name:        "list_stores",
		Description: "Lists stores",
		Schema:      `{"type":"object","properties":{"store_names":{"type":"array"}},"required":["store_names"],"additionalProperties":false}`,
	}})
	if err != nil {
		t.Fatalf("buildChatGPTResponsesRequest() error = %v", err)
	}
	if request.Model != "gpt-5.4" {
		t.Fatalf("expected model to round-trip, got %#v", request)
	}
	if request.PreviousResponseID != "resp_prev_123" {
		t.Fatalf("expected previous_response_id from carryover handle, got %#v", request)
	}
	if !strings.Contains(request.Instructions, "You are a test assistant.") || !strings.Contains(request.Instructions, "emit a native function_call instead of assistant text") || !strings.Contains(request.Instructions, "never display the script body in assistant text") {
		t.Fatalf("expected instructions to round-trip, got %#v", request)
	}
	if len(request.Input) != 1 || !strings.Contains(request.Input[0].Content, "Context:\nFocused context") || !strings.Contains(request.Input[0].Content, "History:\nPrior exchange") || !strings.Contains(request.Input[0].Content, "User Query: Find John") {
		t.Fatalf("expected owned-loop prompt to populate input content, got %#v", request.Input)
	}
	if len(request.Tools) != 1 || request.Tools[0].Type != "function" || request.Tools[0].Name != "list_stores" {
		t.Fatalf("expected function tool mapping, got %#v", request.Tools)
	}
	if request.Tools[0].Strict {
		t.Fatalf("expected scaffold request to keep strict mode disabled for now, got %#v", request.Tools[0])
	}
	if request.Reasoning == nil || request.Reasoning.Effort != "low" {
		t.Fatalf("expected low reasoning effort in scaffold request, got %#v", request.Reasoning)
	}
	if len(request.Include) != 1 || request.Include[0] != "reasoning.encrypted_content" {
		t.Fatalf("expected encrypted reasoning include in scaffold request, got %#v", request.Include)
	}
	if request.Stream != nil {
		t.Fatalf("expected streaming to stay disabled without loop streamer, got %#v", request)
	}
}

func TestChatGPTOwnedLoopCarryoverState_UsesLiveModeWhenHandleExists(t *testing.T) {
	state := chatGPTOwnedLoopCarryoverState("thread_123", []ai.ToolCall{{Name: "tool_1"}, {Name: "tool_2"}})
	if state == nil {
		t.Fatal("expected carryover state")
	}
	if state.Mode != ai.CarryoverModeLive {
		t.Fatalf("expected live carryover mode, got %+v", state)
	}
	if state.ConversationHandle != "thread_123" {
		t.Fatalf("expected conversation handle, got %+v", state)
	}
	if state.EstimatedRawToolTokens <= 0 {
		t.Fatalf("expected raw tool token estimate, got %+v", state)
	}
}

func TestBuildChatGPTResponsesRequest_EnablesStreamingWhenStreamerPresent(t *testing.T) {
	request, err := buildChatGPTResponsesRequest(ai.ReasoningRequest{
		UserQuery: "Find John",
		Streamer: func(eventType string, data any) {
			_ = eventType
			_ = data
		},
	}, "gpt-5.4", nil)
	if err != nil {
		t.Fatalf("buildChatGPTResponsesRequest() error = %v", err)
	}
	if request.Stream == nil || !*request.Stream {
		t.Fatalf("expected streaming to be enabled when a loop streamer is present, got %#v", request)
	}
}

func TestBuildChatGPTResponsesRequest_OmitsTemperatureForGPT5(t *testing.T) {
	request, err := buildChatGPTResponsesRequest(ai.ReasoningRequest{
		UserQuery: "Find John",
	}, "gpt-5.4", nil)
	if err != nil {
		t.Fatalf("buildChatGPTResponsesRequest() error = %v", err)
	}
	if request.Temperature != nil {
		t.Fatalf("expected GPT-5 Responses request to omit temperature, got %#v", request.Temperature)
	}
}

func TestBuildChatGPTResponsesRequest_SetsTemperatureZeroForGPT4O(t *testing.T) {
	request, err := buildChatGPTResponsesRequest(ai.ReasoningRequest{
		UserQuery: "Find John",
	}, "gpt-4o", nil)
	if err != nil {
		t.Fatalf("buildChatGPTResponsesRequest() error = %v", err)
	}
	if request.Temperature == nil || *request.Temperature != 0 {
		t.Fatalf("expected GPT-4o Responses request to set temperature=0, got %#v", request.Temperature)
	}
}

func TestChatGPTOwnedReActLoop_RunReturnsBuildErrorForInvalidToolSchema(t *testing.T) {
	loop := newChatGPTOwnedReActLoop(&chatgpt{model: "gpt-4o"})
	_, err := loop.Run(context.Background(), ai.ReasoningRequest{
		UserQuery: "Find John",
		Executor: chatGPTToolExecutorStub{listToolsErr: nil, tools: []ai.ToolDefinition{{
			Name:   "broken_tool",
			Schema: `{not-json}`,
		}}},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid OpenAI tool schema") {
		t.Fatalf("expected invalid schema error from scaffold request builder, got %v", err)
	}
}

func TestChatGPTOwnedReActLoop_RunExecutesFunctionCallsViaResponsesAPI(t *testing.T) {
	requests := make([]openAIResponsesRequest, 0, 2)
	loop := chatGPTOwnedReActLoop{
		generator:     &chatgpt{model: "gpt-5.4", apiKey: "test-key"},
		maxIterations: 3,
		create: func(ctx context.Context, request openAIResponsesRequest) (openAIResponsesResponse, error) {
			_ = ctx
			requests = append(requests, request)
			switch len(requests) {
			case 1:
				return openAIResponsesResponse{
					ID: "resp_first",
					Output: []openAIResponsesOutputItem{{
						ID:        "fc_1",
						Type:      "function_call",
						CallID:    "call_1",
						Name:      "lookup_user",
						Arguments: `{"name":"John"}`,
					}},
				}, nil
			case 2:
				return openAIResponsesResponse{
					ID:         "resp_final",
					OutputText: "John is in the users store.",
				}, nil
			default:
				return openAIResponsesResponse{}, fmt.Errorf("unexpected extra request")
			}
		},
	}

	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
		SystemPrompt: "Use tools when needed.",
		UserQuery:    "Find John",
		Executor: chatGPTToolExecutorStub{
			tools: []ai.ToolDefinition{{
				Name:        "lookup_user",
				Description: "Finds a user",
				Schema:      `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}`,
			}},
			executeResults: map[string]string{"lookup_user": `{"tool_result":"John is in the users store."}`},
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if resp.FinalText != "John is in the users store." {
		t.Fatalf("expected final output text, got %+v", resp)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "lookup_user" {
		t.Fatalf("expected executed tool call to round-trip, got %+v", resp.ToolCalls)
	}
	if resp.CarryoverState == nil || resp.CarryoverState.ConversationHandle != "resp_final" {
		t.Fatalf("expected final response id in carryover state, got %+v", resp.CarryoverState)
	}
	if resp.CarryoverState.LastAssistantSummary != "John is in the users store." {
		t.Fatalf("expected final text to populate carryover summary, got %+v", resp.CarryoverState)
	}
	if len(requests) != 2 {
		t.Fatalf("expected two responses requests, got %d", len(requests))
	}
	if requests[0].PreviousResponseID != "" {
		t.Fatalf("expected first request to start fresh, got %#v", requests[0])
	}
	if requests[1].PreviousResponseID != "resp_first" {
		t.Fatalf("expected second request to chain previous response id, got %#v", requests[1])
	}
	if len(requests[1].Input) != 1 || requests[1].Input[0].Type != "function_call_output" || requests[1].Input[0].CallID != "call_1" || requests[1].Input[0].Output != "John is in the users store." {
		t.Fatalf("expected function_call_output continuation, got %#v", requests[1].Input)
	}
	if !reflect.DeepEqual(requests[1].Tools, requests[0].Tools) {
		t.Fatalf("expected tool definitions to persist across turns, got %#v then %#v", requests[0].Tools, requests[1].Tools)
	}
	if !strings.Contains(requests[1].Instructions, "Use tools when needed.") || !strings.Contains(requests[1].Instructions, "emit a native function_call instead of assistant text") || !strings.Contains(requests[1].Instructions, "never display the script body in assistant text") {
		t.Fatalf("expected instructions to carry across turns, got %#v", requests[1])
	}
}

func TestChatGPTOwnedReActLoop_RunUsesStreamingTransportWhenStreamerPresent(t *testing.T) {
	streamCalled := false
	loop := chatGPTOwnedReActLoop{
		generator:     &chatgpt{model: "gpt-5.4", apiKey: "test-key"},
		maxIterations: 1,
		createStream: func(ctx context.Context, request openAIResponsesRequest, streamer func(string, any)) (openAIResponsesResponse, error) {
			_ = ctx
			streamCalled = true
			if request.Stream == nil || !*request.Stream {
				t.Fatalf("expected stream flag on streamed request, got %#v", request)
			}
			if streamer == nil {
				t.Fatal("expected loop streamer to be forwarded to streaming transport")
			}
			streamer("assistant_message", map[string]any{"phase": "commentary", "text": "Checking", "streaming": true, "stream_state": "delta"})
			return openAIResponsesResponse{ID: "resp_final", OutputText: "John is in the users store."}, nil
		},
	}
	events := make([]string, 0, 1)
	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
		UserQuery: "Find John",
		Streamer: func(eventType string, data any) {
			_ = data
			events = append(events, eventType)
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !streamCalled {
		t.Fatal("expected streaming transport to be used")
	}
	if resp.FinalText != "John is in the users store." {
		t.Fatalf("expected final text from streaming transport, got %+v", resp)
	}
	if !slices.Contains(events, "assistant_message") {
		t.Fatalf("expected streamed assistant event to reach loop streamer, got %#v", events)
	}
}

func TestChatGPTOwnedReActLoop_RunReplaysReasoningItemsWithoutResponseID(t *testing.T) {
	requests := make([]openAIResponsesRequest, 0, 2)
	loop := chatGPTOwnedReActLoop{
		generator:     &chatgpt{model: "gpt-5.4", apiKey: "test-key"},
		maxIterations: 2,
		create: func(ctx context.Context, request openAIResponsesRequest) (openAIResponsesResponse, error) {
			_ = ctx
			requests = append(requests, request)
			switch len(requests) {
			case 1:
				return openAIResponsesResponse{
					Output: []openAIResponsesOutputItem{
						{ID: "msg_1", Type: "message", Role: "assistant", Phase: "commentary", Content: []openAIResponsesContentItem{{Type: "output_text", Text: "Checking the users store."}}},
						{ID: "rs_1", Type: "reasoning", EncryptedContent: "enc_1"},
						{ID: "fc_1", Type: "function_call", CallID: "call_1", Name: "lookup_user", Arguments: `{"name":"John"}`},
					},
				}, nil
			case 2:
				return openAIResponsesResponse{OutputText: "John is in the users store."}, nil
			default:
				return openAIResponsesResponse{}, fmt.Errorf("unexpected extra request")
			}
		},
	}

	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
		UserQuery: "Find John",
		Executor: chatGPTToolExecutorStub{
			tools: []ai.ToolDefinition{{
				Name:        "lookup_user",
				Description: "Finds a user",
				Schema:      `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}`,
			}},
			executeResults: map[string]string{"lookup_user": `{"tool_result":"John is in the users store."}`},
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if resp.FinalText != "John is in the users store." {
		t.Fatalf("expected final output text, got %+v", resp)
	}
	if len(requests) != 2 {
		t.Fatalf("expected two responses requests, got %d", len(requests))
	}
	if requests[1].PreviousResponseID != "" {
		t.Fatalf("expected no previous_response_id fallback when first response has no id, got %#v", requests[1])
	}
	if len(requests[1].Input) != 4 {
		t.Fatalf("expected assistant + reasoning + function_call + function_call_output replay items, got %#v", requests[1].Input)
	}
	if requests[1].Input[0].Role != "assistant" || requests[1].Input[0].Phase != "commentary" || requests[1].Input[0].Content != "Checking the users store." {
		t.Fatalf("expected assistant commentary replay first, got %#v", requests[1].Input)
	}
	if requests[1].Input[1].Type != "reasoning" || requests[1].Input[1].EncryptedContent != "enc_1" {
		t.Fatalf("expected encrypted reasoning replay first, got %#v", requests[1].Input)
	}
	if requests[1].Input[2].Type != "function_call" || requests[1].Input[2].CallID != "call_1" || requests[1].Input[2].Arguments != `{"name":"John"}` {
		t.Fatalf("expected function_call replay second, got %#v", requests[1].Input)
	}
	if requests[1].Input[3].Type != "function_call_output" || requests[1].Input[3].CallID != "call_1" {
		t.Fatalf("expected function_call_output last, got %#v", requests[1].Input)
	}
}

func TestOpenAIResponseContinuationReplayItems_PreservesAssistantPhaseOrder(t *testing.T) {
	items := openAIResponseContinuationReplayItems(openAIResponsesResponse{
		Output: []openAIResponsesOutputItem{
			{ID: "msg_1", Type: "message", Role: "assistant", Phase: "commentary", Content: []openAIResponsesContentItem{{Type: "output_text", Text: "Checking the users store."}}},
			{ID: "rs_1", Type: "reasoning", EncryptedContent: "enc_1"},
			{ID: "fc_1", Type: "function_call", CallID: "call_1", Name: "lookup_user", Arguments: `{"name":"John"}`},
			{ID: "msg_2", Type: "message", Role: "assistant", Phase: "final_answer", Content: []openAIResponsesContentItem{{Type: "output_text", Text: "John is in the users store."}}},
		},
	})
	if len(items) != 4 {
		t.Fatalf("expected all replayable items in order, got %#v", items)
	}
	if items[0].Role != "assistant" || items[0].Phase != "commentary" || items[0].Content != "Checking the users store." {
		t.Fatalf("expected commentary assistant replay first, got %#v", items)
	}
	if items[1].Type != "reasoning" || items[1].EncryptedContent != "enc_1" {
		t.Fatalf("expected reasoning replay second, got %#v", items)
	}
	if items[2].Type != "function_call" || items[2].CallID != "call_1" {
		t.Fatalf("expected function_call replay third, got %#v", items)
	}
	if items[3].Role != "assistant" || items[3].Phase != "final_answer" || items[3].Content != "John is in the users store." {
		t.Fatalf("expected final_answer assistant replay last, got %#v", items)
	}
}

func TestChatGPTOwnedReActLoop_RunExtractsFinalTextFromAssistantMessage(t *testing.T) {
	loop := chatGPTOwnedReActLoop{
		generator:     &chatgpt{model: "gpt-5.4", apiKey: "test-key"},
		maxIterations: 1,
		create: func(ctx context.Context, request openAIResponsesRequest) (openAIResponsesResponse, error) {
			_ = ctx
			_ = request
			return openAIResponsesResponse{
				ID: "resp_final",
				Output: []openAIResponsesOutputItem{{
					Type: "message",
					Role: "assistant",
					Content: []openAIResponsesContentItem{{
						Type: "output_text",
						Text: "Resolved from assistant message content.",
					}},
				}},
			}, nil
		},
	}
	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{UserQuery: "Find John"})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if resp.FinalText != "Resolved from assistant message content." {
		t.Fatalf("expected assistant message content to become final text, got %+v", resp)
	}
	if resp.CarryoverState == nil || resp.CarryoverState.LastAssistantSummary != "Resolved from assistant message content." {
		t.Fatalf("expected assistant message text to populate carryover summary, got %+v", resp.CarryoverState)
	}
}

func TestOpenAIResponseToolCalls_RecoversTextualFunctionCallFromAssistantMessage(t *testing.T) {
	toolCalls, err := openAIResponseToolCalls(openAIResponsesResponse{
		Output: []openAIResponsesOutputItem{{
			Type: "message",
			Role: "assistant",
			Content: []openAIResponsesContentItem{{
				Type: "output_text",
				Text: "Function_call\n\"{\"\"arguments\"\":{\"\"mode\"\":\"\"read\"\"},\"\"name\"\":\"\"begin_tx\"\"}\"",
			}},
		}},
	})
	if err != nil {
		t.Fatalf("openAIResponseToolCalls() error = %v", err)
	}
	if len(toolCalls) != 1 {
		t.Fatalf("expected one recovered tool call, got %#v", toolCalls)
	}
	if toolCalls[0].Name != "begin_tx" {
		t.Fatalf("expected begin_tx tool call, got %#v", toolCalls[0])
	}
	if mode, _ := toolCalls[0].Args["mode"].(string); mode != "read" {
		t.Fatalf("expected recovered args to include mode=read, got %#v", toolCalls[0].Args)
	}
}

func TestOpenAIResponseFinalText_PrefersFinalAnswerPhase(t *testing.T) {
	response := openAIResponsesResponse{
		Output: []openAIResponsesOutputItem{
			{
				Type:  "message",
				Role:  "assistant",
				Phase: "commentary",
				Content: []openAIResponsesContentItem{{
					Type: "output_text",
					Text: "I will check the users store.",
				}},
			},
			{
				Type:  "message",
				Role:  "assistant",
				Phase: "final_answer",
				Content: []openAIResponsesContentItem{{
					Type: "output_text",
					Text: "John is in the users store.",
				}},
			},
		},
	}
	if got := openAIResponseFinalText(response); got != "John is in the users store." {
		t.Fatalf("expected final_answer phase text only, got %q", got)
	}
}

func TestChatGPTOwnedReActLoop_RunEmitsAssistantMessagePhases(t *testing.T) {
	events := make([]map[string]any, 0, 2)
	loop := chatGPTOwnedReActLoop{
		generator:     &chatgpt{model: "gpt-5.4", apiKey: "test-key"},
		maxIterations: 1,
		createStream: func(ctx context.Context, request openAIResponsesRequest, streamer func(string, any)) (openAIResponsesResponse, error) {
			_ = ctx
			_ = request
			_ = streamer
			return openAIResponsesResponse{
				ID: "resp_final",
				Output: []openAIResponsesOutputItem{
					{
						Type:    "message",
						Role:    "assistant",
						Phase:   "commentary",
						Content: []openAIResponsesContentItem{{Type: "output_text", Text: "Checking the store."}},
					},
					{
						Type:    "message",
						Role:    "assistant",
						Phase:   "final_answer",
						Content: []openAIResponsesContentItem{{Type: "output_text", Text: "John is in the users store."}},
					},
				},
			}, nil
		},
	}
	resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
		UserQuery: "Find John",
		Streamer: func(eventType string, data any) {
			if eventType != "assistant_message" {
				return
			}
			payload, ok := data.(map[string]any)
			if !ok {
				t.Fatalf("expected assistant_message payload map, got %#v", data)
			}
			events = append(events, payload)
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if resp.FinalText != "John is in the users store." {
		t.Fatalf("expected final answer text, got %+v", resp)
	}
	if len(events) != 2 {
		t.Fatalf("expected two assistant message events, got %#v", events)
	}
	phases := []string{fmt.Sprint(events[0]["phase"]), fmt.Sprint(events[1]["phase"])}
	if !slices.Equal(phases, []string{"commentary", "final_answer"}) {
		t.Fatalf("expected commentary then final_answer phases, got %#v", events)
	}
}

func TestParseChatGPTResponsesStream_SuppressesRawDeltaBridgeEventsAndReturnsCompletedResponse(t *testing.T) {
	stream := bytes.NewBufferString(strings.Join([]string{
		"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"status\":\"in_progress\",\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"commentary\",\"content\":[]}}",
		"",
		"data: {\"type\":\"response.reasoning_summary_text.delta\",\"item_id\":\"rs_1\",\"output_index\":0,\"summary_index\":0,\"delta\":\"Checking the store\"}",
		"",
		"data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"Checking\"}",
		"",
		"data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"id\":\"fc_1\",\"status\":\"in_progress\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup_user\",\"arguments\":\"\"}}",
		"",
		"data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"output_index\":1,\"delta\":\"{\\\"name\\\":\\\"John\\\"}\"}",
		"",
		"data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_1\",\"output_index\":1,\"name\":\"lookup_user\",\"arguments\":\"{\\\"name\\\":\\\"John\\\"}\"}",
		"",
		"data: {\"type\":\"response.reasoning_summary_text.done\",\"item_id\":\"rs_1\",\"output_index\":0,\"summary_index\":0,\"text\":\"Checking the store before final answer.\"}",
		"",
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_final\",\"status\":\"completed\",\"output_text\":\"John is in the users store.\",\"output\":[{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"John is in the users store.\"}]}]}}",
		"",
	}, "\n"))
	type streamedEvent struct {
		eventType string
		payload   map[string]any
	}
	var events []streamedEvent
	response, err := parseChatGPTResponsesStream(stream, func(eventType string, data any) {
		payload, ok := data.(map[string]any)
		if !ok {
			t.Fatalf("expected streamed payload map, got %#v", data)
		}
		events = append(events, streamedEvent{eventType: eventType, payload: payload})
	})
	if err != nil {
		t.Fatalf("parseChatGPTResponsesStream() error = %v", err)
	}
	if response.ID != "resp_final" || response.OutputText != "John is in the users store." {
		t.Fatalf("expected completed response payload, got %#v", response)
	}
	if len(events) != 0 {
		t.Fatalf("expected raw SSE parser to suppress user-visible bridge events, got %#v", events)
	}
}

type chatGPTToolExecutorStub struct {
	tools          []ai.ToolDefinition
	listToolsErr   error
	executeResults map[string]string
	executeErr     error
}

func (s chatGPTToolExecutorStub) Execute(ctx context.Context, tool string, args map[string]any) (string, error) {
	_ = ctx
	_ = args
	if s.executeErr != nil {
		return "", s.executeErr
	}
	if s.executeResults != nil {
		if result, ok := s.executeResults[tool]; ok {
			return result, nil
		}
	}
	return "", errors.New("not implemented")
}

func (s chatGPTToolExecutorStub) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	_ = ctx
	if s.listToolsErr != nil {
		return nil, s.listToolsErr
	}
	return s.tools, nil
}

func TestChatGPTOwnedLoopCarryoverState_UsesHandleAndRawToolEstimate(t *testing.T) {
	state := chatGPTOwnedLoopCarryoverState("thread_123", []ai.ToolCall{{Name: "tool_1"}, {Name: "tool_2"}})
	if state == nil {
		t.Fatal("expected carryover state")
	}
	if state.Mode != ai.CarryoverModeCompact {
		t.Fatalf("expected compact carryover mode, got %+v", state)
	}
	if state.ConversationHandle != "thread_123" {
		t.Fatalf("expected conversation handle to round-trip, got %+v", state)
	}
	if state.EstimatedRawToolTokens <= 0 {
		t.Fatalf("expected positive raw tool estimate, got %+v", state)
	}
}

func TestChatGPTOwnedLoopCarryoverState_ReturnsNilWithoutHandleOrTools(t *testing.T) {
	if state := chatGPTOwnedLoopCarryoverState("", nil); state != nil {
		t.Fatalf("expected nil carryover state when no provider runtime state exists, got %+v", state)
	}
}
