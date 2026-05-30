package ai

import (
	"reflect"
	"strings"
	"testing"
)

func TestBuildMemoryHydrationUpdate_DelegatesToFromParts(t *testing.T) {
	resp := ReasoningResponse{
		FinalText:      strings.Repeat("F", MemoryHydrationTextCharLimit+50),
		ToolCalls:      []ToolCall{{Name: "tool_1"}, {Name: "tool_2"}, {Name: "tool_3"}, {Name: "tool_4"}, {Name: "tool_5"}, {Name: "tool_6"}, {Name: "tool_7"}},
		OutcomeFacts:   []string{"fact_1", "fact_2", "fact_3", "fact_4", "fact_5", "fact_6", strings.Repeat("a", MemoryHydrationFactCharLimit+10)},
		OutcomeRecipes: []LearnedRecipe{{ID: "recipe_1"}},
		CarryoverState: &CarryoverState{
			LastAssistantSummary: strings.Repeat("S", MemoryHydrationTextCharLimit+25),
			LastOutcomeFacts:     []string{"carry_fact_1", "carry_fact_2", "carry_fact_3", "carry_fact_4", "carry_fact_5", "carry_fact_6", strings.Repeat("c", MemoryHydrationFactCharLimit+15)},
			LastToolNames:        []string{"tool_1", "tool_2", "tool_3", "tool_4", "tool_5", "tool_6", "tool_7"},
		},
	}

	got := BuildMemoryHydrationUpdate(resp)
	want := BuildMemoryHydrationUpdateFromParts(MemoryHydrationUpdate{
		FinalText:      resp.FinalText,
		ToolCalls:      resp.ToolCalls,
		OutcomeFacts:   resp.OutcomeFacts,
		OutcomeRecipes: resp.OutcomeRecipes,
		CarryoverState: resp.CarryoverState,
	})

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected response adapter to delegate to FromParts helper\n got: %+v\nwant: %+v", got, want)
	}
}

func TestBuildMemoryHydrationUpdateFromParts_BoundsAndClonesWithoutReasoningResponse(t *testing.T) {
	input := MemoryHydrationUpdate{
		FinalText: strings.Repeat("P", MemoryHydrationTextCharLimit+20),
		ToolCalls: []ToolCall{
			{Name: "tool_1"},
			{Name: "tool_2"},
			{Name: "tool_3"},
			{Name: "tool_4"},
			{Name: "tool_5"},
			{Name: "tool_6"},
			{Name: "tool_7"},
		},
		OutcomeFacts: []string{
			"fact_1",
			"fact_2",
			"fact_3",
			"fact_4",
			"fact_5",
			"fact_6",
			strings.Repeat("z", MemoryHydrationFactCharLimit+10),
		},
		CarryoverState: &CarryoverState{
			LastAssistantSummary: strings.Repeat("C", MemoryHydrationTextCharLimit+20),
			LastToolNames:        []string{"tool_1", "tool_2", "tool_3", "tool_4", "tool_5", "tool_6", "tool_7"},
		},
	}

	update := BuildMemoryHydrationUpdateFromParts(input)

	if len([]rune(update.FinalText)) != MemoryHydrationTextCharLimit {
		t.Fatalf("expected final text to be trimmed to %d chars, got %d", MemoryHydrationTextCharLimit, len([]rune(update.FinalText)))
	}
	if len(update.ToolCalls) != MemoryHydrationToolCallLimit {
		t.Fatalf("expected tool calls to be capped at %d, got %d", MemoryHydrationToolCallLimit, len(update.ToolCalls))
	}
	if update.ToolCalls[0].Name != "tool_2" || update.ToolCalls[len(update.ToolCalls)-1].Name != "tool_7" {
		t.Fatalf("expected builder to keep the most recent tool calls, got %+v", update.ToolCalls)
	}
	if len(update.OutcomeFacts) != MemoryHydrationOutcomeFactLimit {
		t.Fatalf("expected outcome facts to be capped at %d, got %d", MemoryHydrationOutcomeFactLimit, len(update.OutcomeFacts))
	}
	if len([]rune(update.OutcomeFacts[len(update.OutcomeFacts)-1])) != MemoryHydrationFactCharLimit {
		t.Fatalf("expected last fact to be trimmed to %d chars, got %d", MemoryHydrationFactCharLimit, len([]rune(update.OutcomeFacts[len(update.OutcomeFacts)-1])))
	}
	if update.CarryoverState == nil || len(update.CarryoverState.LastToolNames) != MemoryHydrationToolCallLimit {
		t.Fatalf("expected carryover tool names to be bounded, got %+v", update.CarryoverState)
	}

	input.ToolCalls[6].Name = "mutated"
	if update.ToolCalls[len(update.ToolCalls)-1].Name != "tool_7" {
		t.Fatalf("expected tool calls to be cloned, got %+v", update.ToolCalls)
	}
}
