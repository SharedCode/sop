package ai

import (
	"strings"
	"testing"
)

func TestBuildMemoryHydrationUpdate_BoundsTextFactsAndCarryover(t *testing.T) {
	resp := ReasoningResponse{
		FinalText: strings.Repeat("F", memoryHydrationTextCharLimit+50),
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
			strings.Repeat("a", memoryHydrationFactCharLimit+10),
			"fact_2",
			"fact_3",
			"fact_4",
			"fact_5",
			"fact_6",
			"fact_7",
		},
		CarryoverState: &CarryoverState{
			LastAssistantSummary: strings.Repeat("S", memoryHydrationTextCharLimit+25),
			LastOutcomeFacts: []string{
				strings.Repeat("c", memoryHydrationFactCharLimit+15),
				"carry_fact_2",
				"carry_fact_3",
				"carry_fact_4",
				"carry_fact_5",
				"carry_fact_6",
				"carry_fact_7",
			},
			LastToolNames: []string{"tool_1", "tool_2", "tool_3", "tool_4", "tool_5", "tool_6", "tool_7"},
		},
	}

	update := BuildMemoryHydrationUpdate(resp)

	if len([]rune(update.FinalText)) != memoryHydrationTextCharLimit {
		t.Fatalf("expected final text to be trimmed to %d chars, got %d", memoryHydrationTextCharLimit, len([]rune(update.FinalText)))
	}
	if !strings.HasSuffix(update.FinalText, "...") {
		t.Fatalf("expected final text trim to end with ellipsis, got %q", update.FinalText)
	}
	if len(update.ToolCalls) != memoryHydrationToolCallLimit {
		t.Fatalf("expected tool calls to be capped at %d, got %d", memoryHydrationToolCallLimit, len(update.ToolCalls))
	}
	if update.ToolCalls[0].Name != "tool_2" || update.ToolCalls[len(update.ToolCalls)-1].Name != "tool_7" {
		t.Fatalf("expected helper to keep the most recent tool calls, got %+v", update.ToolCalls)
	}
	if len(update.OutcomeFacts) != memoryHydrationOutcomeFactLimit {
		t.Fatalf("expected outcome facts to be capped at %d, got %d", memoryHydrationOutcomeFactLimit, len(update.OutcomeFacts))
	}
	if update.OutcomeFacts[0] != "fact_2" || update.OutcomeFacts[len(update.OutcomeFacts)-1] != "fact_7" {
		t.Fatalf("expected helper to keep the most recent outcome facts, got %+v", update.OutcomeFacts)
	}
	if update.CarryoverState == nil {
		t.Fatal("expected carryover state to survive cloning")
	}
	if len([]rune(update.CarryoverState.LastAssistantSummary)) != memoryHydrationTextCharLimit {
		t.Fatalf("expected carryover summary to be trimmed to %d chars, got %d", memoryHydrationTextCharLimit, len([]rune(update.CarryoverState.LastAssistantSummary)))
	}
	if len(update.CarryoverState.LastOutcomeFacts) != memoryHydrationOutcomeFactLimit {
		t.Fatalf("expected carryover facts to be capped at %d, got %d", memoryHydrationOutcomeFactLimit, len(update.CarryoverState.LastOutcomeFacts))
	}
	if update.CarryoverState.LastOutcomeFacts[0] != "carry_fact_2" || update.CarryoverState.LastOutcomeFacts[len(update.CarryoverState.LastOutcomeFacts)-1] != "carry_fact_7" {
		t.Fatalf("expected helper to keep the most recent carryover facts, got %+v", update.CarryoverState.LastOutcomeFacts)
	}
	if len(update.CarryoverState.LastToolNames) != memoryHydrationToolCallLimit {
		t.Fatalf("expected carryover tool names to be capped at %d, got %d", memoryHydrationToolCallLimit, len(update.CarryoverState.LastToolNames))
	}
	if update.CarryoverState.LastToolNames[0] != "tool_2" || update.CarryoverState.LastToolNames[len(update.CarryoverState.LastToolNames)-1] != "tool_7" {
		t.Fatalf("expected helper to keep the most recent carryover tool names, got %+v", update.CarryoverState.LastToolNames)
	}

	resp.ToolCalls[6].Name = "mutated_tool"
	resp.OutcomeFacts[6] = "mutated_fact"
	resp.CarryoverState.LastToolNames[6] = "mutated_tool"
	if update.ToolCalls[len(update.ToolCalls)-1].Name != "tool_7" {
		t.Fatalf("expected tool calls to be cloned, got %+v", update.ToolCalls)
	}
	if update.OutcomeFacts[len(update.OutcomeFacts)-1] != "fact_7" {
		t.Fatalf("expected outcome facts to be cloned, got %+v", update.OutcomeFacts)
	}
	if update.CarryoverState.LastToolNames[len(update.CarryoverState.LastToolNames)-1] != "tool_7" {
		t.Fatalf("expected carryover tool names to be cloned, got %+v", update.CarryoverState.LastToolNames)
	}
}