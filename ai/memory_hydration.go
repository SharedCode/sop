package ai

import "strings"

const (
	// MemoryHydrationToolCallLimit caps the number of recent tool calls retained
	// in a provisional in-loop hydration update.
	MemoryHydrationToolCallLimit = 6
	// MemoryHydrationOutcomeFactLimit caps the number of recent grounded facts
	// retained in a provisional in-loop hydration update.
	MemoryHydrationOutcomeFactLimit = 6
	// MemoryHydrationTextCharLimit caps provisional final text and carryover
	// assistant summaries retained for in-loop hydration.
	MemoryHydrationTextCharLimit = 600
	// MemoryHydrationFactCharLimit caps each grounded fact retained for
	// provisional in-loop hydration.
	MemoryHydrationFactCharLimit = 240
)

// BuildMemoryHydrationUpdateFromParts normalizes provider-owned in-loop
// progress into the bounded, grounded shape expected by the provisional MRU
// sink. Provider-owned loops should prefer this narrower helper so they do not
// need to manufacture a full ReasoningResponse just to emit hydration.
func BuildMemoryHydrationUpdateFromParts(update MemoryHydrationUpdate) MemoryHydrationUpdate {
	return MemoryHydrationUpdate{
		FinalText:      trimHydrationText(update.FinalText, MemoryHydrationTextCharLimit),
		ToolCalls:      cloneHydrationToolCalls(update.ToolCalls),
		OutcomeFacts:   cloneHydrationFacts(update.OutcomeFacts),
		OutcomeRecipes: append([]LearnedRecipe(nil), update.OutcomeRecipes...),
		CarryoverState: cloneHydrationCarryoverState(update.CarryoverState),
	}
}

// BuildMemoryHydrationUpdate adapts a full provider-owned response into the
// narrower provisional hydration contract. Prefer
// BuildMemoryHydrationUpdateFromParts in provider loops when a full response is
// not otherwise needed.
func BuildMemoryHydrationUpdate(resp ReasoningResponse) MemoryHydrationUpdate {
	return BuildMemoryHydrationUpdateFromParts(MemoryHydrationUpdate{
		FinalText:      resp.FinalText,
		ToolCalls:      resp.ToolCalls,
		OutcomeFacts:   resp.OutcomeFacts,
		OutcomeRecipes: resp.OutcomeRecipes,
		CarryoverState: resp.CarryoverState,
	})
}

func cloneHydrationFacts(facts []string) []string {
	if len(facts) == 0 {
		return nil
	}
	start := 0
	if len(facts) > MemoryHydrationOutcomeFactLimit {
		start = len(facts) - MemoryHydrationOutcomeFactLimit
	}
	cloned := make([]string, 0, len(facts)-start)
	for _, fact := range facts[start:] {
		trimmed := trimHydrationText(fact, MemoryHydrationFactCharLimit)
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
	if len(toolCalls) > MemoryHydrationToolCallLimit {
		start = len(toolCalls) - MemoryHydrationToolCallLimit
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
	cloned.LastAssistantSummary = trimHydrationText(state.LastAssistantSummary, MemoryHydrationTextCharLimit)
	return &cloned
}

func cloneHydrationToolNames(toolNames []string) []string {
	if len(toolNames) == 0 {
		return nil
	}
	start := 0
	if len(toolNames) > MemoryHydrationToolCallLimit {
		start = len(toolNames) - MemoryHydrationToolCallLimit
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
