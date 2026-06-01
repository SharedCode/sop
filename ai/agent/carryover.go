package agent

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sharedcode/sop/ai"
)

type carryoverDecision struct {
	Mode                   ai.CarryoverMode
	Reason                 ai.CarryoverResetReason
	SuppressHistory        bool
	Summary                string
	EstimatedCarryTokens   int
	EstimatedHistoryTokens int
	State                  *ai.CarryoverState
}

func cloneCarryoverState(state *ai.CarryoverState) *ai.CarryoverState {
	if state == nil {
		return nil
	}
	cloned := *state
	cloned.LastOutcomeFacts = append([]string(nil), state.LastOutcomeFacts...)
	cloned.LastRecipeIDs = append([]string(nil), state.LastRecipeIDs...)
	cloned.LastToolNames = append([]string(nil), state.LastToolNames...)
	return &cloned
}

func compactCarryoverText(text string, maxChars int) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}
	trimmed = strings.Join(strings.Fields(trimmed), " ")
	if maxChars > 0 && len(trimmed) > maxChars {
		return trimmed[:maxChars-3] + "..."
	}
	return trimmed
}

func estimateCarryoverTokens(text string) int {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return 0
	}
	return (len(trimmed) + 3) / 4
}

func providerName(gen ai.Generator) string {
	if gen == nil {
		return ""
	}
	return strings.TrimSpace(gen.Name())
}

func carryoverCapability(gen ai.Generator) (ai.CarryoverCapability, bool) {
	if gen == nil {
		return ai.CarryoverCapability{}, false
	}
	provider, ok := gen.(ai.CarryoverCapabilityProvider)
	if !ok {
		return ai.CarryoverCapability{}, false
	}
	capability := provider.CarryoverCapability()
	if strings.TrimSpace(capability.Provider) == "" {
		capability.Provider = providerName(gen)
	}
	return capability, strings.TrimSpace(capability.Provider) != ""
}

func carryoverModeSupported(capability ai.CarryoverCapability, mode ai.CarryoverMode) bool {
	switch mode {
	case ai.CarryoverModeCompact:
		return capability.SupportsCompact
	case ai.CarryoverModeLive:
		return capability.SupportsLive
	case ai.CarryoverModeOff, "":
		return true
	default:
		return false
	}
}

func mergeCarryoverRuntimeState(state *ai.CarryoverState, previous *ai.CarryoverState, providerState *ai.CarryoverState, capability ai.CarryoverCapability) {
	if state == nil {
		return
	}
	if previous != nil && strings.EqualFold(strings.TrimSpace(previous.Provider), strings.TrimSpace(state.Provider)) && strings.TrimSpace(previous.TopicFingerprint) != "" && strings.TrimSpace(previous.TopicFingerprint) == strings.TrimSpace(state.TopicFingerprint) {
		if carryoverModeSupported(capability, previous.Mode) && previous.Mode != "" {
			state.Mode = previous.Mode
		}
		if state.ConversationHandle == "" {
			state.ConversationHandle = strings.TrimSpace(previous.ConversationHandle)
		}
		if state.EstimatedRawToolTokens == 0 {
			state.EstimatedRawToolTokens = previous.EstimatedRawToolTokens
		}
	}
	if providerState == nil {
		return
	}
	if carryoverModeSupported(capability, providerState.Mode) && providerState.Mode != "" {
		state.Mode = providerState.Mode
	}
	if handle := strings.TrimSpace(providerState.ConversationHandle); handle != "" {
		state.ConversationHandle = handle
	}
	if providerState.EstimatedRawToolTokens > 0 {
		state.EstimatedRawToolTokens = providerState.EstimatedRawToolTokens
	}
	if providerState.LastResetReason != "" {
		state.LastResetReason = providerState.LastResetReason
	}
}

func carryoverKBFingerprint(ctx context.Context, session *RunnerSession) string {
	if session != nil && session.Memory != nil {
		if thread := session.Memory.GetCurrentThread(); thread != nil && len(thread.Exchanges) > 0 {
			activeKB := strings.TrimSpace(thread.Exchanges[len(thread.Exchanges)-1].ActiveKB)
			if activeKB != "" {
				return activeKB
			}
		}
	}
	if payload := ai.GetSessionPayload(ctx); payload != nil && len(payload.SelectedKBs) > 0 {
		names := make([]string, 0, len(payload.SelectedKBs))
		for _, kb := range payload.SelectedKBs {
			if name := strings.TrimSpace(kb.Name); name != "" {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		return strings.Join(names, ",")
	}
	return ""
}

func compactCarryoverSummary(state *ai.CarryoverState) string {
	if state == nil {
		return ""
	}
	lines := make([]string, 0, 8)
	if query := compactCarryoverText(state.LastUserQuery, 220); query != "" {
		lines = append(lines, "Previous ask: "+query)
	}
	if answer := compactCarryoverText(state.LastAssistantSummary, 280); answer != "" {
		lines = append(lines, "Previous answer summary: "+answer)
	}
	if len(state.LastOutcomeFacts) > 0 {
		lines = append(lines, "Confirmed facts:")
		for _, fact := range state.LastOutcomeFacts {
			if fact = compactCarryoverText(fact, 180); fact != "" {
				lines = append(lines, "- "+fact)
			}
		}
	}
	if len(state.LastRecipeIDs) > 0 {
		lines = append(lines, "Learned recipes: "+strings.Join(state.LastRecipeIDs, ", "))
	}
	if len(state.LastToolNames) > 0 {
		lines = append(lines, "Recent tools: "+strings.Join(state.LastToolNames, " -> "))
	}
	if len(lines) == 0 {
		return ""
	}
	return "[Carryover Continuity]\n" + strings.Join(lines, "\n")
}

func carryoverFallbackReasonText(reason ai.CarryoverResetReason) string {
	switch reason {
	case ai.CarryoverResetUnsupportedProvider:
		return "Native carryover is unavailable for this provider here, so reconstruct continuity from memory."
	case ai.CarryoverResetProviderChanged:
		return "The provider changed between asks, so reconstruct continuity from memory instead of reusing a native thread."
	case ai.CarryoverResetModelChanged:
		return "The model changed between asks, so reconstruct continuity from memory instead of reusing a native thread."
	case ai.CarryoverResetExpired:
		return "Native carryover expired, so reconstruct continuity from memory."
	case ai.CarryoverResetBudgetExceeded:
		return "Native carryover hit its budget limit, so continue from compact memory instead."
	default:
		return "Reconstruct continuity from memory before broadening scope."
	}
}

func memoryContinuationSummary(state *ai.CarryoverState, reason ai.CarryoverResetReason) string {
	if state == nil {
		return ""
	}
	base := compactCarryoverSummary(state)
	if strings.TrimSpace(base) == "" {
		return ""
	}
	lines := []string{"[Memory Continuity Fallback]", carryoverFallbackReasonText(reason)}
	lines = append(lines, strings.Split(strings.TrimPrefix(base, "[Carryover Continuity]\n"), "\n")...)
	return strings.Join(lines, "\n")
}

func shouldUseMemoryContinuationFallback(reason ai.CarryoverResetReason) bool {
	switch reason {
	case ai.CarryoverResetUnsupportedProvider, ai.CarryoverResetProviderChanged, ai.CarryoverResetModelChanged, ai.CarryoverResetExpired, ai.CarryoverResetBudgetExceeded:
		return true
	default:
		return false
	}
}

func decisionWithFallback(decision carryoverDecision, state *ai.CarryoverState) carryoverDecision {
	if state == nil {
		return decision
	}
	cloned := cloneCarryoverState(state)
	cloned.LastResetReason = decision.Reason
	decision.State = cloned
	if shouldUseMemoryContinuationFallback(decision.Reason) {
		decision.Summary = memoryContinuationSummary(cloned, decision.Reason)
		decision.EstimatedCarryTokens = estimateCarryoverTokens(decision.Summary)
	}
	return decision
}

func decideCarryover(ctx context.Context, session *RunnerSession, gen ai.Generator, topicAssessment *TopicAssessment, historyText string) carryoverDecision {
	decision := carryoverDecision{Mode: ai.CarryoverModeOff, Reason: ai.CarryoverResetDisabled}
	if session == nil || session.Memory == nil {
		decision.Reason = ai.CarryoverResetMissingState
		return decision
	}
	if topicAssessment == nil {
		decision.Reason = ai.CarryoverResetMissingState
		return decision
	}
	if topicAssessment.IsNewTopic {
		decision.Reason = ai.CarryoverResetTopicSwitch
		return decision
	}
	capability, ok := carryoverCapability(gen)
	if !ok || !capability.SupportsCompact {
		decision.Reason = ai.CarryoverResetUnsupportedProvider
		return decisionWithFallback(decision, session.Memory.GetCarryoverState())
	}
	provider := strings.TrimSpace(capability.Provider)
	state := session.Memory.GetCarryoverState()
	if state == nil {
		decision.Reason = ai.CarryoverResetMissingState
		return decision
	}
	if !strings.EqualFold(strings.TrimSpace(state.Provider), provider) {
		decision.Reason = ai.CarryoverResetProviderChanged
		return decisionWithFallback(decision, state)
	}
	if strings.TrimSpace(capability.Model) != "" && strings.TrimSpace(state.Model) != "" && !strings.EqualFold(strings.TrimSpace(state.Model), strings.TrimSpace(capability.Model)) {
		decision.Reason = ai.CarryoverResetModelChanged
		return decisionWithFallback(decision, state)
	}
	if topicUUID := strings.TrimSpace(topicAssessment.TopicUUID); topicUUID != "" && strings.TrimSpace(state.TopicFingerprint) != "" && strings.TrimSpace(state.TopicFingerprint) != topicUUID {
		decision.Reason = ai.CarryoverResetTopicSwitch
		return decision
	}
	kbFingerprint := carryoverKBFingerprint(ctx, session)
	if kbFingerprint != "" && state.KBFingerprint != "" && state.KBFingerprint != kbFingerprint {
		decision.Reason = ai.CarryoverResetKBChanged
		return decision
	}
	policy := ai.DefaultCarryoverPolicy()
	if state.LastUsedAtUnixMilli > 0 && time.Since(time.UnixMilli(state.LastUsedAtUnixMilli)) > policy.LiveCarryoverIdleTTL {
		decision.Reason = ai.CarryoverResetExpired
		return decisionWithFallback(decision, state)
	}
	if state.AskCount >= policy.LiveCarryoverMaxAsks || state.EstimatedCarryTokens > policy.LiveCarryoverHardTokens || state.EstimatedRawToolTokens > policy.MaxRawToolCarryTokens {
		decision.Reason = ai.CarryoverResetBudgetExceeded
		return decisionWithFallback(decision, state)
	}
	if capability.SupportsLive && strings.TrimSpace(state.ConversationHandle) != "" {
		decision.Mode = ai.CarryoverModeLive
		decision.Reason = ai.CarryoverResetLiveContinuation
		decision.SuppressHistory = true
		decision.EstimatedHistoryTokens = estimateCarryoverTokens(historyText)
		decision.State = state
		return decision
	}
	summary := compactCarryoverSummary(state)
	if strings.TrimSpace(summary) == "" {
		decision.Reason = ai.CarryoverResetMissingState
		return decision
	}
	decision.Mode = ai.CarryoverModeCompact
	decision.Reason = ai.CarryoverResetCompactContinuation
	decision.SuppressHistory = true
	decision.Summary = summary
	decision.EstimatedCarryTokens = estimateCarryoverTokens(summary)
	decision.EstimatedHistoryTokens = estimateCarryoverTokens(historyText)
	decision.State = state
	return decision
}

func learnedRecipeIDs(recipes []ai.LearnedRecipe) []string {
	if len(recipes) == 0 {
		return nil
	}
	ids := make([]string, 0, len(recipes))
	seen := make(map[string]bool, len(recipes))
	for _, recipe := range recipes {
		id := compactCarryoverText(recipe.ID, 120)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids
}

func toolNamesFromCalls(toolCalls []ai.ToolCall) []string {
	if len(toolCalls) == 0 {
		return nil
	}
	names := make([]string, 0, len(toolCalls))
	for _, toolCall := range toolCalls {
		name := compactCarryoverText(toolCall.Name, 60)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	return names
}

func buildCarryoverState(ctx context.Context, session *RunnerSession, gen ai.Generator, currentThread *ConversationThread, query string, finalText string, toolCalls []ai.ToolCall, outcomeFacts []string, outcomeRecipes []ai.LearnedRecipe, providerState *ai.CarryoverState) *ai.CarryoverState {
	capability, ok := carryoverCapability(gen)
	if !ok || (!capability.SupportsCompact && !capability.SupportsLive) {
		return nil
	}
	provider := strings.TrimSpace(capability.Provider)
	now := time.Now().UnixMilli()
	topicFingerprint := ""
	if currentThread != nil {
		topicFingerprint = currentThread.ID.String()
	}
	kbFingerprint := carryoverKBFingerprint(ctx, session)
	previous := (*ai.CarryoverState)(nil)
	if session != nil && session.Memory != nil {
		previous = session.Memory.GetCarryoverState()
	}
	askCount := 1
	startedAt := now
	if previous != nil && strings.EqualFold(previous.Provider, provider) && previous.TopicFingerprint != "" && previous.TopicFingerprint == topicFingerprint {
		askCount = previous.AskCount + 1
		if previous.StartedAtUnixMilli > 0 {
			startedAt = previous.StartedAtUnixMilli
		}
	}
	state := &ai.CarryoverState{
		Mode:                 ai.CarryoverModeCompact,
		Provider:             provider,
		Model:                strings.TrimSpace(capability.Model),
		TopicFingerprint:     topicFingerprint,
		KBFingerprint:        kbFingerprint,
		StartedAtUnixMilli:   startedAt,
		LastUsedAtUnixMilli:  now,
		AskCount:             askCount,
		LastOutcomeFacts:     append([]string(nil), outcomeFacts...),
		LastRecipeIDs:        learnedRecipeIDs(outcomeRecipes),
		LastToolNames:        toolNamesFromCalls(toolCalls),
		LastUserQuery:        compactCarryoverText(query, 220),
		LastAssistantSummary: compactCarryoverText(sanitizeAssistantContinuityText(finalText), 280),
	}
	mergeCarryoverRuntimeState(state, previous, providerState, capability)
	summary := compactCarryoverSummary(state)
	state.EstimatedCarryTokens = estimateCarryoverTokens(summary)
	return state
}

func persistCarryoverState(stm *ShortTermMemory, state *ai.CarryoverState) {
	if stm == nil {
		return
	}
	stm.SetCarryoverState(state)
}

func appendCarryoverToContext(contextText string, summary string) string {
	summary = strings.TrimSpace(summary)
	if summary == "" {
		return contextText
	}
	if strings.TrimSpace(contextText) == "" {
		return summary
	}
	return fmt.Sprintf("%s\n\n%s", contextText, summary)
}
