package generator

import (
	"context"
	"fmt"
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestOwnedLoops_EmitToolEventsWhenStreamerIsPresent(t *testing.T) {
	t.Run("gemini", func(t *testing.T) {
		gen := &geminiOwnedLoopStreamingGenerator{}
		exec := &geminiOwnedLoopStreamingExecutor{}
		loop := geminiOwnedReActLoop{
			generator:     gen,
			maxIterations: 2,
		}

		var events []providerStreamedEvent
		_, err := loop.Run(context.Background(), ai.ReasoningRequest{
			SystemPrompt: "You are a test assistant.",
			UserQuery:    "Show me users",
			Executor:     exec,
			Generator:    gen,
			Streamer: func(eventType string, data any) {
				payload, _ := data.(map[string]any)
				events = append(events, providerStreamedEvent{eventType: eventType, payload: payload})
			},
		})
		if err != nil {
			t.Fatalf("Run failed: %v", err)
		}
		if len(events) == 0 {
			t.Fatalf("expected streamed tool events when a streamer is present, got none")
		}
	})
}

func TestOwnedLoops_EmitToolResultEvenWhenToolExecutionFails(t *testing.T) {
	t.Run("gemini", func(t *testing.T) {
		gen := &geminiOwnedLoopStreamingGenerator{}
		exec := &failingGeminiExecutor{err: fmt.Errorf("boom")}
		loop := geminiOwnedReActLoop{generator: gen, maxIterations: 1}

		events := captureReasoningEvents(t, func(streamer func(string, any)) error {
			_, err := loop.Run(context.Background(), ai.ReasoningRequest{
				SystemPrompt: "You are a test assistant.",
				UserQuery:    "Show me users",
				Executor:     exec,
				Generator:    gen,
				Verbose:      true,
				Streamer:     streamer,
			})
			return err
		})

		if len(events) < 3 {
			t.Fatalf("expected tool_call + tool_result + tool_error events, got %#v", events)
		}
		if events[1].eventType != ai.ReasoningEventToolResult {
			t.Fatalf("expected tool_result event after tool_call, got %#v", events[1])
		}
		if got := events[1].payload["result"]; got != "boom" {
			t.Fatalf("expected tool_result to carry the failure message, got %#v", got)
		}
	})
}

func TestOwnedLoops_EmitCompatibleCoreToolEventsAcrossProviders(t *testing.T) {
	t.Run("gemini", func(t *testing.T) {
		gen := &geminiOwnedLoopStreamingGenerator{}
		exec := &geminiOwnedLoopStreamingExecutor{}
		loop := geminiOwnedReActLoop{
			generator:     gen,
			maxIterations: 2,
		}

		events := captureReasoningEvents(t, func(streamer func(string, any)) error {
			resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
				SystemPrompt: "You are a test assistant.",
				UserQuery:    "Show me users",
				Executor:     exec,
				Generator:    gen,
				Verbose:      true,
				Streamer:     streamer,
			})
			if err != nil {
				return err
			}
			if resp.FinalText != "Final answer after streaming" {
				t.Fatalf("expected Gemini final answer, got %q", resp.FinalText)
			}
			return nil
		})

		assertCoreToolEventContract(t, "gemini", events, "list_stores", map[string]any{"scope": "users"}, `[{"name":"John Doe"}]`)
	})

	t.Run("chatgpt", func(t *testing.T) {
		requests := make([]openAIResponsesRequest, 0, 2)
		handleRequest := func(ctx context.Context, request openAIResponsesRequest) (openAIResponsesResponse, error) {
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
				return openAIResponsesResponse{ID: "resp_final", OutputText: "John is in the users store."}, nil
			default:
				return openAIResponsesResponse{}, fmt.Errorf("unexpected extra request")
			}
		}
		loop := chatGPTOwnedReActLoop{
			generator:     &chatgpt{model: "gpt-5.4", apiKey: "test-key"},
			maxIterations: 2,
			create:        handleRequest,
			createStream: func(ctx context.Context, request openAIResponsesRequest, streamer func(string, any)) (openAIResponsesResponse, error) {
				_ = streamer
				return handleRequest(ctx, request)
			},
		}

		events := captureReasoningEvents(t, func(streamer func(string, any)) error {
			resp, err := loop.Run(context.Background(), ai.ReasoningRequest{
				SystemPrompt: "Use tools when needed.",
				UserQuery:    "Find John",
				Verbose:      true,
				Executor: chatGPTToolExecutorStub{
					tools: []ai.ToolDefinition{{
						Name:        "lookup_user",
						Description: "Finds a user",
						Schema:      `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}`,
					}},
					executeResults: map[string]string{"lookup_user": `{"tool_result":"John is in the users store."}`},
				},
				Streamer: streamer,
			})
			if err != nil {
				return err
			}
			if resp.FinalText != "John is in the users store." {
				t.Fatalf("expected ChatGPT final answer, got %q", resp.FinalText)
			}
			return nil
		})

		assertCoreToolEventContract(t, "chatgpt", events, "lookup_user", map[string]any{"name": "John"}, "John is in the users store.")
	})
}

type failingGeminiExecutor struct {
	err error
}

func (e *failingGeminiExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	_ = ctx
	_ = toolName
	_ = args
	return "", e.err
}

func (e *failingGeminiExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	_ = ctx
	return []ai.ToolDefinition{{Name: "list_stores"}}, nil
}

type providerStreamedEvent struct {
	eventType string
	payload   map[string]any
}

func captureReasoningEvents(t *testing.T, run func(func(string, any)) error) []providerStreamedEvent {
	t.Helper()
	var events []providerStreamedEvent
	err := run(func(eventType string, data any) {
		payload, ok := data.(map[string]any)
		if !ok {
			t.Fatalf("expected payload map for %s, got %#v", eventType, data)
		}
		events = append(events, providerStreamedEvent{eventType: eventType, payload: payload})
	})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	return events
}

func assertCoreToolEventContract(t *testing.T, provider string, events []providerStreamedEvent, wantTool string, wantArgs map[string]any, wantResult string) {
	t.Helper()
	if len(events) < 2 {
		t.Fatalf("%s: expected at least tool_call and tool_result events, got %#v", provider, events)
	}

	toolCall := events[0]
	if toolCall.eventType != ai.ReasoningEventToolCall {
		t.Fatalf("%s: expected first event %q, got %#v", provider, ai.ReasoningEventToolCall, toolCall)
	}
	if got := toolCall.payload["tool"]; got != wantTool {
		t.Fatalf("%s: unexpected tool_call tool %v", provider, got)
	}
	args, ok := toolCall.payload["args"].(map[string]any)
	if !ok {
		t.Fatalf("%s: expected tool_call args map, got %#v", provider, toolCall.payload)
	}
	for key, value := range wantArgs {
		if args[key] != value {
			t.Fatalf("%s: expected tool_call arg %q=%#v, got %#v", provider, key, value, args)
		}
	}
	if _, ok := toolCall.payload["iteration"].(int); !ok {
		t.Fatalf("%s: expected integer iteration on tool_call, got %#v", provider, toolCall.payload)
	}

	toolResult := events[1]
	if toolResult.eventType != ai.ReasoningEventToolResult {
		t.Fatalf("%s: expected second event %q, got %#v", provider, ai.ReasoningEventToolResult, toolResult)
	}
	if got := toolResult.payload["tool"]; got != wantTool {
		t.Fatalf("%s: unexpected tool_result tool %v", provider, got)
	}
	resultArgs, ok := toolResult.payload["args"].(map[string]any)
	if !ok {
		t.Fatalf("%s: expected tool_result args map, got %#v", provider, toolResult.payload)
	}
	for key, value := range wantArgs {
		if resultArgs[key] != value {
			t.Fatalf("%s: expected tool_result arg %q=%#v, got %#v", provider, key, value, resultArgs)
		}
	}
	if toolResult.payload["result"] != wantResult {
		t.Fatalf("%s: expected tool_result result %q, got %#v", provider, wantResult, toolResult.payload)
	}
	if toolResult.payload["result_chars"] != len(wantResult) {
		t.Fatalf("%s: expected tool_result result_chars=%d, got %#v", provider, len(wantResult), toolResult.payload)
	}
	if _, ok := toolResult.payload["iteration"].(int); !ok {
		t.Fatalf("%s: expected integer iteration on tool_result, got %#v", provider, toolResult.payload)
	}
}
