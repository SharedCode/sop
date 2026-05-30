package ai

import "strings"

const (
	memoryHydrationToolCallLimit    = 6
	memoryHydrationOutcomeFactLimit = 6
	memoryHydrationTextCharLimit    = 600
	memoryHydrationFactCharLimit    = 240
)

// BuildMemoryHydrationUpdate converts a provider-owned loop response into the
// bounded, grounded update shape expected by the provisional in-loop memory
// sink. Future provider-owned loops should use this helper so Gemini, Claude,
// GPT, or others all emit the same contract instead of inventing provider-
// specific in-loop memory paths.
func BuildMemoryHydrationUpdate(resp ReasoningResponse) MemoryHydrationUpdate {
	return MemoryHydrationUpdate{
		FinalText:      trimHydrationText(resp.FinalText, memoryHydrationTextCharLimit),
		ToolCalls:      cloneHydrationToolCalls(resp.ToolCalls),
		OutcomeFacts:   cloneHydrationFacts(resp.OutcomeFacts),
		OutcomeRecipes: append([]LearnedRecipe(nil), resp.OutcomeRecipes...),
		CarryoverState: cloneHydrationCarryoverState(resp.CarryoverState),
	}
}

func cloneHydrationFacts(facts []string) []string {
	if len(facts) == 0 {
		return nil
	}
	start := 0
	if len(facts) > memoryHydrationOutcomeFactLimit {
		start = len(facts) - memoryHydrationOutcomeFactLimit
	}
	cloned := make([]string, 0, len(facts)-start)
	for _, fact := range facts[start:] {
		trimmed := trimHydrationText(fact, memoryHydrationFactCharLimit)
		if trimmed == "" {
			continue
		}
		cloned = append(cloned, trimmed)
	}
	if len(cloned) == 0 {
		return nil
	}
	return cloned
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
	cloned.LastOutcomeFacts = cloneHydrationFacts(state.LastOutcomeFacts)
	cloned.LastRecipeIDs = append([]string(nil), state.LastRecipeIDs...)
	cloned.LastToolNames = cloneHydrationToolNames(state.LastToolNames)
	cloned.LastAssistantSummary = trimHydrationText(state.LastAssistantSummary, memoryHydrationTextCharLimit)
	return &cloned
}

func cloneHydrationToolNames(toolNames []string) []string {
	if len(toolNames) == 0 {
		return nil
	}
	start := 0
	if len(toolNames) > memoryHydrationToolCallLimit {
		start = len(toolNames) - memoryHydrationToolCallLimit
	}
	cloned := make([]string, 0, len(toolNames)-start)
	for _, name := range toolNames[start:] {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		cloned = append(cloned, trimmed)
	}
	if len(cloned) == 0 {
		return nil
	}
	return cloned
}

func trimHydrationText(text string, limit int) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" || limit <= 0 {
		return ""
	}
	runes := []rune(trimmed)
	if len(runes) <= limit {
		return trimmed
	}
	if limit <= 3 {
		return string(runes[:limit])
	}
	return string(runes[:limit-3]) + "..."
}
