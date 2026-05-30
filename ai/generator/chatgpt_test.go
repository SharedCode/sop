package generator

import (
	"context"
	"fmt"
	"testing"

	"github.com/sharedcode/sop/ai"
)

type chatGPTOwnedLoopHydrationStub struct{}

func (chatGPTOwnedLoopHydrationStub) Run(ctx context.Context, req ai.ReasoningRequest) (ai.ReasoningResponse, error) {
	_ = ctx
	resp := ai.ReasoningResponse{
		FinalText:    "Final answer from ChatGPT owned loop",
		OutcomeFacts: []string{"confirmed users store"},
		ToolCalls: []ai.ToolCall{
			{Name: "tool_1", Args: map[string]any{"step": 1}},
			{Name: "tool_2", Args: map[string]any{"step": 2}},
			{Name: "tool_3", Args: map[string]any{"step": 3}},
			{Name: "tool_4", Args: map[string]any{"step": 4}},
			{Name: "tool_5", Args: map[string]any{"step": 5}},
			{Name: "tool_6", Args: map[string]any{"step": 6}},
			{Name: "tool_7", Args: map[string]any{"step": 7}},
			{Name: "tool_8", Args: map[string]any{"step": 8}},
		},
		CarryoverState: &ai.CarryoverState{
			Provider:         "chatgpt",
			Model:            "gpt-4o",
			LastOutcomeFacts: []string{"confirmed users store"},
			LastRecipeIDs:    []string{"recipe_users_lookup"},
			LastToolNames:    []string{"tool_7", "tool_8"},
		},
	}
	if req.HydrationSink != nil {
		req.HydrationSink(ai.BuildMemoryHydrationUpdate(resp))
	}
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
	if captured.CarryoverState.LastToolNames[0] != "tool_7" {
		t.Fatalf("expected cloned carryover state in hydration update, got %+v", captured.CarryoverState)
	}
	if captured.CarryoverState.LastOutcomeFacts[0] != "confirmed users store" {
		t.Fatalf("expected cloned carryover facts in hydration update, got %+v", captured.CarryoverState)
	}
}
