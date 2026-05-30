package ai

import "strings"

const memoryHydrationToolCallLimit = 6

// BuildMemoryHydrationUpdate converts a provider-owned loop response into the
// bounded, grounded update shape expected by the provisional in-loop memory
// sink. Future provider-owned loops should use this helper so Gemini, Claude,
// GPT, or others all emit the same contract instead of inventing provider-
// specific in-loop memory paths.
func BuildMemoryHydrationUpdate(resp ReasoningResponse) MemoryHydrationUpdate {
	return MemoryHydrationUpdate{
		FinalText:      strings.TrimSpace(resp.FinalText),
		ToolCalls:      cloneHydrationToolCalls(resp.ToolCalls),
		OutcomeFacts:   append([]string(nil), resp.OutcomeFacts...),
		OutcomeRecipes: append([]LearnedRecipe(nil), resp.OutcomeRecipes...),
		CarryoverState: cloneHydrationCarryoverState(resp.CarryoverState),
	}
}

func cloneHydrationToolCalls(toolCalls []ToolCall) []ToolCall {
	if len(toolCalls) == 0 {
		return nil
	}
	start := 0
	if len(toolCalls) > memoryHydrationToolCallLimit {
		start = len(toolCalls) - memoryHydrationToolCallLimit
	}
	cloned := make([]ToolCall, 0, len(toolCalls)-start)
	for _, toolCall := range toolCalls[start:] {
		cloned = append(cloned, ToolCall{
			Name:          toolCall.Name,
			Args:          cloneHydrationMap(toolCall.Args),
			NativeID:      toolCall.NativeID,
			TransportMeta: cloneHydrationMap(toolCall.TransportMeta),
		})
	}
	return cloned
}

func cloneHydrationMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(src))
	for key, value := range src {
		cloned[key] = value
	}
	return cloned
}

func cloneHydrationCarryoverState(state *CarryoverState) *CarryoverState {
	if state == nil {
		return nil
	}
	cloned := *state
	cloned.LastOutcomeFacts = append([]string(nil), state.LastOutcomeFacts...)
	cloned.LastRecipeIDs = append([]string(nil), state.LastRecipeIDs...)
	cloned.LastToolNames = append([]string(nil), state.LastToolNames...)
	return &cloned
}
