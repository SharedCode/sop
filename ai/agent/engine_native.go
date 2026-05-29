package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	log "log/slog"
	"sort"
	"strings"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/obfuscation"
)

// NativeReActEngine implements ReasoningEngine using native LLM API tool calling.
type NativeReActEngine struct {
	EnableObfuscation bool
}

const nativeReActBaseToolIterations = 5
const nativeReActMaxToolIterations = 15
const nativeReActToolCallTemperature float32 = 0.1
const nativeReActRepairTemperature float32 = 0.0

type nativeToolResult struct {
	Name   string
	Result string
	Args   map[string]any
	Hint   *ai.ToolProgressHint
}

const askOutcomeFactsLimit = 4

type askAnchoredMRUState []MRUItem

type pendingToolRepair struct {
	ToolName       string
	Strategy       nativeRepairStrategy
	ResearchTool   string
	ResearchReason string
}

type pendingGenerationRepair struct{}

type askLoopBudgetState struct {
	allowedIterations int
	maxIterations     int
}

type askLoopProgressDelta struct {
	Progressing bool
	NewFacts    []string
	NewRecipes  []string
	HintSignal  *ai.ToolProgressHint
}

type nativeRepairStrategy string

const (
	askMRUCategoryConfirmed                = "ASK_CONFIRMED"
	askMRUCategoryConfirmedStoreSchema     = "ASK_CONFIRMED_STORE_SCHEMA"
	askMRUCategoryConfirmedStoreRelation   = "ASK_CONFIRMED_STORE_RELATIONS"
	askMRUCategoryConfirmedJoinSelection   = "ASK_CONFIRMED_JOIN_SELECTION"
	askMRUCategoryConfirmedFilterSelection = "ASK_CONFIRMED_FILTER_SELECTION"

	nativeRepairStrategySameTool      nativeRepairStrategy = "same_tool"
	nativeRepairStrategyResearchFirst nativeRepairStrategy = "research_first"
)

type progressSink func(string)

// Run executes the orchestration loop relying on native tool calls.
func (e *NativeReActEngine) Run(ctx context.Context, req ai.ReasoningRequest) (ai.ReasoningResponse, error) {
	if req.Generator == nil {
		if e.EnableObfuscation {
			return ai.ReasoningResponse{FinalText: obfuscation.GlobalObfuscator.DeobfuscateText(req.ContextText)}, nil
		}
		return ai.ReasoningResponse{FinalText: req.ContextText}, nil
	}

	var tools []ai.ToolDefinition
	var err error
	if req.Executor != nil {
		tools, err = req.Executor.ListTools(ctx)
		if err != nil {
			return ai.ReasoningResponse{}, fmt.Errorf("failed to list tools: %w", err)
		}
	}
	log.Info("Native ReAct Engine Start",
		"generator", req.Generator.Name(),
		"default_format", getRequestedOutputFormat(ctx),
		"tool_count", len(tools),
		"tools", summarizeToolDefinitions(tools),
		"query_chars", len(req.UserQuery),
		"system_prompt_chars", len(req.SystemPrompt),
	)
	emitVerboseProgress(ctx, "Planning request with native multi-step loop.")

	executedToolCalls := make([]ai.ToolCall, 0)
	toolResults := make([]nativeToolResult, 0)
	budgetState := newAskLoopBudgetState()
	var pendingRepair *pendingToolRepair
	var pendingMalformedCall *pendingGenerationRepair
	for iteration := 0; iteration < budgetState.maxIterations && iteration < budgetState.allowedIterations; iteration++ {
		emitVerboseProgress(ctx, "Reasoning iteration %d of %d.", iteration+1, budgetState.allowedIterations)
		mainPrompt := buildNativeReActPrompt(req, toolResults, buildAskAnchoredMRUState(toolResults))
		emitVerboseProgress(ctx, "Waiting for model response.")
		temperature := nativeReActToolCallTemperature
		if pendingRepair != nil || pendingMalformedCall != nil {
			temperature = nativeReActRepairTemperature
		}
		output, err := req.Generator.Generate(ctx, mainPrompt, ai.GenOptions{
			SystemPrompt: req.SystemPrompt,
			MaxTokens:    1024,
			Temperature:  temperature,
			Tools:        tools,
		})
		if err != nil {
			if isRecoverableGenerationError(err) {
				emitVerboseProgress(ctx, "Model emitted a malformed native tool call; retrying.")
				pendingMalformedCall = &pendingGenerationRepair{}
				toolResults = append(toolResults, nativeToolResult{
					Name:   "native_tool_call",
					Result: formatRecoverableGenerationError(err),
				})
				log.Warn("Native ReAct Engine Recoverable Generation Failure", "error", err)
				continue
			}
			return ai.ReasoningResponse{}, fmt.Errorf("generation failed: %w", err)
		}
		pendingMalformedCall = nil
		log.Info("Native ReAct Engine Output",
			"iteration", iteration+1,
			"text_chars", len(output.Text),
			"tool_call_count", len(output.ToolCalls),
		)

		if req.Executor == nil || len(output.ToolCalls) == 0 {
			emitVerboseProgress(ctx, "No further tools required; preparing final answer.")
			outcomeRecipes := summarizeOutcomeRecipes(toolResults)
			if shouldPreserveStructuredToolResult(ctx, toolResults) {
				return ai.ReasoningResponse{
					FinalText:      preserveStructuredToolResult(toolResults[len(toolResults)-1].Result, e.EnableObfuscation),
					ToolCalls:      executedToolCalls,
					OutcomeFacts:   summarizeOutcomeFacts(toolResults),
					OutcomeRecipes: outcomeRecipes,
				}, nil
			}

			finalText := output.Text
			if e.EnableObfuscation {
				finalText = obfuscation.GlobalObfuscator.DeobfuscateText(output.Text)
			}

			return ai.ReasoningResponse{
				FinalText:      finalText,
				ToolCalls:      executedToolCalls,
				OutcomeFacts:   summarizeOutcomeFacts(toolResults),
				OutcomeRecipes: outcomeRecipes,
			}, nil
		}

		toolCall := output.ToolCalls[0]
		if pendingRepair != nil && !pendingRepair.allowsTool(toolCall.Name) {
			emitVerboseProgress(ctx, "Tool `%s` must be corrected before other actions.", pendingRepair.ToolName)
			toolResults = append(toolResults, nativeToolResult{
				Name:   pendingRepair.ToolName,
				Result: formatPendingRepairReminder(*pendingRepair, toolCall.Name),
			})
			log.Warn("Native ReAct Engine Deferred Non-Repair Tool Call", "expected_tool", pendingRepair.ToolName, "received_tool", toolCall.Name)
			continue
		}
		emitVerboseProgress(ctx, "Calling tool `%s`.", toolCall.Name)
		log.Info("Native ReAct Engine Tool Call",
			"iteration", iteration+1,
			"tool", toolCall.Name,
			"arg_keys", summarizeToolArgKeys(toolCall.Args),
		)

		sanitizeToolCallArgs(toolCall.Args, e.EnableObfuscation)
		emitReasoningEvent(req, "tool_call", map[string]any{
			"tool":      toolCall.Name,
			"args":      cloneToolEventMap(toolCall.Args),
			"iteration": iteration + 1,
		})

		execCtx := context.WithValue(ctx, ai.CtxKeyNativeToolHints, true)
		rawResult, err := req.Executor.Execute(execCtx, toolCall.Name, toolCall.Args)
		if err != nil {
			emitReasoningEvent(req, "tool_error", map[string]any{
				"tool":      toolCall.Name,
				"args":      cloneToolEventMap(toolCall.Args),
				"error":     err.Error(),
				"iteration": iteration + 1,
			})
			if isRecoverableToolExecutionError(err) {
				repairPlan := classifyRecoverableToolRepair(toolCall.Name, err)
				emitVerboseProgress(ctx, "Tool `%s` needs corrected arguments; retrying.", toolCall.Name)
				pendingRepair = &repairPlan
				toolResults = append(toolResults, nativeToolResult{
					Name:   toolCall.Name,
					Result: formatRecoverableToolError(repairPlan, toolCall.Args, err),
				})
				log.Warn("Native ReAct Engine Recoverable Tool Failure", "tool", toolCall.Name, "error", err)
				continue
			}
			log.Error("Native ReAct Engine Tool Failure", "tool", toolCall.Name, "error", err)
			return ai.ReasoningResponse{}, fmt.Errorf("tool execution failed: %w", err)
		}
		result, hint := unwrapToolResultEnvelope(rawResult)
		emitVerboseProgress(ctx, "Tool `%s` completed.", toolCall.Name)
		pendingRepair = nil
		log.Info("Native ReAct Engine Tool Success", "iteration", iteration+1, "tool", toolCall.Name, "result_chars", len(result))
		emitReasoningEvent(req, "tool_result", map[string]any{
			"tool":          toolCall.Name,
			"args":          cloneToolEventMap(toolCall.Args),
			"result":        result,
			"progress_hint": hint,
			"result_chars":  len(result),
			"iteration":     iteration + 1,
		})

		executedToolCalls = append(executedToolCalls, toolCall)
		previousResults := append([]nativeToolResult(nil), toolResults...)
		toolResults = append(toolResults, nativeToolResult{Name: toolCall.Name, Result: result, Args: cloneToolEventMap(toolCall.Args), Hint: cloneToolProgressHint(hint)})
		if shouldShortCircuitAskLoopOnToolHint(hint) {
			emitVerboseProgress(ctx, "Tool `%s` reported a terminal hard error; short-circuiting the Ask loop.", toolCall.Name)
			outcomeRecipes := summarizeOutcomeRecipes(toolResults)
			return ai.ReasoningResponse{
				FinalText:      preserveStructuredToolResult(result, e.EnableObfuscation),
				ToolCalls:      executedToolCalls,
				OutcomeFacts:   summarizeOutcomeFacts(toolResults),
				OutcomeRecipes: outcomeRecipes,
			}, nil
		}
		budgetDelta := detectAskLoopProgress(previousResults, toolResults)
		if budgetState.extendIfProgressing(budgetDelta) {
			emitVerboseProgress(ctx, "Loop budget extended to %d because the Ask is progressing.", budgetState.allowedIterations)
		}
		if shouldPreserveStructuredToolResult(ctx, toolResults) {
			outcomeRecipes := summarizeOutcomeRecipes(toolResults)
			return ai.ReasoningResponse{
				FinalText:      preserveStructuredToolResult(result, e.EnableObfuscation),
				ToolCalls:      executedToolCalls,
				OutcomeFacts:   summarizeOutcomeFacts(toolResults),
				OutcomeRecipes: outcomeRecipes,
			}, nil
		}
	}

	emitVerboseProgress(ctx, "Reached tool iteration limit; synthesizing final answer.")
	if shouldPreserveStructuredToolResult(ctx, toolResults) {
		outcomeRecipes := summarizeOutcomeRecipes(toolResults)
		return ai.ReasoningResponse{
			FinalText:      preserveStructuredToolResult(toolResults[len(toolResults)-1].Result, e.EnableObfuscation),
			ToolCalls:      executedToolCalls,
			OutcomeFacts:   summarizeOutcomeFacts(toolResults),
			OutcomeRecipes: outcomeRecipes,
		}, nil
	}

	log.Warn("Native ReAct Engine Reached Tool Iteration Limit", "limit", budgetState.allowedIterations, "hard_cap", budgetState.maxIterations)
	finalPrompt := buildNativeReActPrompt(req, toolResults, buildAskAnchoredMRUState(toolResults)) + "\n\nUser: Analyze the tool response and provide the final answer to the user. Do not call any more tools."
	emitVerboseProgress(ctx, "Waiting for model response.")
	output, err := req.Generator.Generate(ctx, finalPrompt, ai.GenOptions{
		SystemPrompt: req.SystemPrompt,
		MaxTokens:    1024,
		Temperature:  0.7,
	})
	if err != nil {
		return ai.ReasoningResponse{}, fmt.Errorf("final synthesis generation failed: %w", err)
	}
	finalText := output.Text
	if e.EnableObfuscation {
		finalText = obfuscation.GlobalObfuscator.DeobfuscateText(output.Text)
	}
	return ai.ReasoningResponse{
		FinalText:      finalText,
		ToolCalls:      executedToolCalls,
		OutcomeFacts:   summarizeOutcomeFacts(toolResults),
		OutcomeRecipes: summarizeOutcomeRecipes(toolResults),
	}, nil
}

func newAskLoopBudgetState() askLoopBudgetState {
	return askLoopBudgetState{
		allowedIterations: nativeReActBaseToolIterations,
		maxIterations:     nativeReActMaxToolIterations,
	}
}

func (s *askLoopBudgetState) extendIfProgressing(delta askLoopProgressDelta) bool {
	if s == nil || !delta.Progressing || s.allowedIterations >= s.maxIterations {
		return false
	}
	s.allowedIterations++
	if s.allowedIterations > s.maxIterations {
		s.allowedIterations = s.maxIterations
	}
	return true
}

func emitReasoningEvent(req ai.ReasoningRequest, eventType string, data any) {
	if req.Streamer != nil {
		req.Streamer(eventType, data)
	}
}

func cloneToolEventMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	clone := make(map[string]any, len(src))
	for key, value := range src {
		clone[key] = cloneToolEventValue(value)
	}
	return clone
}

func cloneToolEventValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneToolEventMap(typed)
	case []any:
		items := make([]any, len(typed))
		for i, item := range typed {
			items[i] = cloneToolEventValue(item)
		}
		return items
	default:
		return typed
	}
}

func isRecoverableToolExecutionError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if message == "" {
		return false
	}
	recoverableHints := []string{
		"missing",
		"required",
		"not a string",
		"invalid type",
		"must be",
		"expected",
	}
	for _, hint := range recoverableHints {
		if strings.Contains(message, hint) {
			return true
		}
	}
	return false
}

func isRecoverableGenerationError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if message == "" {
		return false
	}
	return strings.Contains(message, "malformed_function_call") || strings.Contains(message, "malformed function call")
}

func emitVerboseProgress(ctx context.Context, format string, args ...any) {
	if !isVerboseEnabled(ctx) {
		return
	}
	msg := fmt.Sprintf(format, args...)
	if sink, ok := ctx.Value(ai.CtxKeyProgressSink).(func(string)); ok && sink != nil {
		sink(msg)
		return
	}
}

func isVerboseEnabled(ctx context.Context) bool {
	if v, ok := ctx.Value("verbose").(bool); ok {
		return v
	}
	return true
}

func buildNativeReActPrompt(req ai.ReasoningRequest, toolResults []nativeToolResult, askState askAnchoredMRUState) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Context:\n%s%s\n\nUser Query: %s", req.ContextText, req.HistoryText, req.UserQuery))
	if len(toolResults) == 0 {
		return sb.String()
	}

	sb.WriteString("\n\nAsk-anchored MRU:\n")
	sb.WriteString(formatAskAnchoredMRUState(askState))
	sb.WriteString("\nProgression history:\n")
	sb.WriteString(formatNativeProgressionHistory(toolResults))

	if last := toolResults[len(toolResults)-1]; strings.Contains(last.Result, "Retry instruction:") {
		if last.Name == "native_tool_call" {
			sb.WriteString("\n\nRepair directive: The last model output produced an invalid native tool call. Your next step should be to call exactly one valid tool using arguments that conform to the provided tool schema and repair guidance below. Do not summarize the error as a final answer unless correction is impossible.")
		} else if strings.Contains(last.Result, "Repair strategy: research_first") {
			sb.WriteString(fmt.Sprintf("\n\nRepair directive: The last tool call to %s failed because grounded schema or relation facts are still missing. Your next step should be to call list_stores first, reuse its schema/relations output as the source of truth, and only then return to %s if needed. Do not summarize the error as a final answer unless correction is impossible.", last.Name, last.Name))
		} else {
			sb.WriteString(fmt.Sprintf("\n\nRepair directive: The last tool call to %s failed because its arguments were invalid. Your next step should be to call the same tool again with corrected arguments using the repair guidance below. Do not summarize the error as a final answer unless correction is impossible.", last.Name))
		}
	}

	sb.WriteString("\n\nTool results:\n")
	for i, result := range toolResults {
		sb.WriteString(fmt.Sprintf("Step %d Tool: %s\n", i+1, result.Name))
		if formattedArgs := formatNativePromptArgs(result.Args); formattedArgs != "" {
			sb.WriteString(fmt.Sprintf("[Tool Args]:\n%s\n", formattedArgs))
		}
		sb.WriteString(fmt.Sprintf("[System Tool Response]:\n%s\n\n", result.Result))
	}
	sb.WriteString("User: Analyze the tool response and continue the task. If another tool is required, call it. Otherwise provide the final answer to the user.")
	return sb.String()
}

func formatNativePromptArgs(args map[string]any) string {
	if len(args) == 0 {
		return ""
	}
	bytes, err := json.MarshalIndent(args, "", "  ")
	if err != nil {
		return ""
	}
	return string(bytes)
}

func formatNativeProgressionHistory(toolResults []nativeToolResult) string {
	entries := make([]map[string]any, 0, len(toolResults))
	for i, result := range toolResults {
		entry := map[string]any{
			"step":           i + 1,
			"tool":           result.Name,
			"ingredients":    buildNativeProgressionIngredients(result),
			"generated_call": result.Args,
			"result":         strings.TrimSpace(result.Result),
			"progression":    buildNativeProgressionMetadata(result),
		}
		entries = append(entries, entry)
	}
	bytes, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return "- progression history unavailable\n"
	}
	return string(bytes) + "\n"
}

func buildNativeProgressionIngredients(result nativeToolResult) map[string]any {
	ingredients := map[string]any{
		"tool_info": result.Name,
	}
	if !strings.Contains(result.Result, "Retry instruction:") {
		if facts := summarizeSuccessfulToolResult(result); len(facts) > 0 {
			ingredients["confirmed_facts"] = facts
		}
	}
	if repairStrategy := extractRepairStrategy(result.Result); repairStrategy != "" {
		ingredients["repair_strategy"] = repairStrategy
	}
	return ingredients
}

func buildNativeProgressionMetadata(result nativeToolResult) map[string]any {
	metadata := make(map[string]any)
	if retryInstruction := extractRetryInstruction(result.Result); retryInstruction != "" {
		metadata["retry_instruction"] = retryInstruction
	}
	if result.Hint != nil {
		if status := strings.TrimSpace(result.Hint.Status); status != "" {
			metadata["status"] = status
		}
		if result.Hint.CompletionDelta > 0 {
			metadata["completion_delta"] = result.Hint.CompletionDelta
		}
		if len(result.Hint.Tips) > 0 {
			metadata["tips"] = append([]string(nil), result.Hint.Tips...)
		}
		if len(result.Hint.Clues) > 0 {
			metadata["clues"] = append([]string(nil), result.Hint.Clues...)
		}
		if len(result.Hint.Missing) > 0 {
			metadata["missing"] = append([]string(nil), result.Hint.Missing...)
		}
		if len(result.Hint.SuggestedNextTools) > 0 {
			metadata["suggested_next_tools"] = append([]string(nil), result.Hint.SuggestedNextTools...)
		}
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func extractRetryInstruction(resultText string) string {
	const marker = "Retry instruction:"
	idx := strings.Index(resultText, marker)
	if idx == -1 {
		return ""
	}
	return strings.TrimSpace(resultText[idx+len(marker):])
}

func extractRepairStrategy(resultText string) string {
	const marker = "Repair strategy:"
	idx := strings.Index(resultText, marker)
	if idx == -1 {
		return ""
	}
	remaining := resultText[idx+len(marker):]
	if newline := strings.IndexByte(remaining, '\n'); newline >= 0 {
		remaining = remaining[:newline]
	}
	return strings.TrimSpace(remaining)
}

func buildAskAnchoredMRUState(toolResults []nativeToolResult) askAnchoredMRUState {
	state := make(askAnchoredMRUState, 0, 8)
	if len(toolResults) == 0 {
		return state
	}

	state = appendAskMRUItem(state, "ASK_ITERATION", fmt.Sprintf("%d", len(toolResults)))
	last := toolResults[len(toolResults)-1]
	state = appendAskMRUItem(state, "ASK_LAST_TOOL", last.Name)

	if strings.Contains(last.Result, "Retry instruction:") {
		state = appendAskMRUItem(state, "ASK_LAST_OUTCOME", "repair_required")
		state = appendAskMRUItem(state, "ASK_CURRENT_FOCUS", "Preserve valid work, classify the failure, and change only the broken slice in the next call.")
		state = appendAskMRUItem(state, "ASK_NEXT_DELTA", summarizeNextRepairNeed(last))
		if attempted := summarizeAttemptedArgs(last.Result); attempted != "" {
			state = appendAskMRUItem(state, "ASK_PRESERVE", attempted)
		}
	} else {
		state = appendAskMRUItem(state, "ASK_LAST_OUTCOME", "tool_completed")
		state = appendAskMRUItem(state, "ASK_CURRENT_FOCUS", "Use confirmed tool output to narrow the next step instead of replaying the full task.")
		for _, fact := range summarizeSuccessfulToolResult(last) {
			state = appendAskConfirmedMRUItem(state, fact)
		}
		state = appendAskProgressHintMRUItems(state, last.Hint)
	}

	for i := len(toolResults) - 2; i >= 0; i-- {
		result := toolResults[i]
		if strings.Contains(result.Result, "Retry instruction:") {
			continue
		}
		for _, fact := range summarizeSuccessfulToolResult(result) {
			state = appendAskConfirmedMRUItem(state, fact)
		}
		state = appendAskProgressHintMRUItems(state, result.Hint)
		state = appendAskMRUItem(state, "ASK_PRESERVE", fmt.Sprintf("Previous successful tool preserved: %s", result.Name))
		break
	}

	if !askMRUHasCategoryPrefix(state, askMRUCategoryConfirmed) && !askMRUHasCategory(state, "ASK_PRESERVE") {
		state = appendAskMRUItem(state, "ASK_PRESERVE", "No grounded facts yet; keep the next action narrow and schema-conformant.")
	}

	return state
}

func formatAskAnchoredMRUState(state askAnchoredMRUState) string {
	if len(state) == 0 {
		return "- No Ask-anchored MRU state yet.\n"
	}

	var sb strings.Builder
	for _, item := range state {
		switch {
		case item.Category == "ASK_ITERATION":
			sb.WriteString(fmt.Sprintf("- Iteration: %s\n", item.Context))
		case item.Category == "ASK_LAST_TOOL":
			sb.WriteString(fmt.Sprintf("- Last tool: %s\n", item.Context))
		case item.Category == "ASK_LAST_OUTCOME":
			sb.WriteString(fmt.Sprintf("- Last outcome: %s\n", item.Context))
		case item.Category == "ASK_CURRENT_FOCUS":
			sb.WriteString(fmt.Sprintf("- Current focus: %s\n", item.Context))
		case strings.HasPrefix(item.Category, askMRUCategoryConfirmed):
			sb.WriteString(fmt.Sprintf("- Confirmed: %s\n", item.Context))
		case item.Category == "ASK_PRESERVE":
			sb.WriteString(fmt.Sprintf("- Preserve: %s\n", item.Context))
		case item.Category == "ASK_NEXT_DELTA":
			sb.WriteString(fmt.Sprintf("- Next delta: %s\n", item.Context))
		case item.Category == "ASK_PROGRESS":
			sb.WriteString(fmt.Sprintf("- Progress: %s\n", item.Context))
		case item.Category == "ASK_TIP":
			sb.WriteString(fmt.Sprintf("- Tip: %s\n", item.Context))
		case item.Category == "ASK_CLUE":
			sb.WriteString(fmt.Sprintf("- Clue: %s\n", item.Context))
		case item.Category == "ASK_SUGGESTED_TOOL":
			sb.WriteString(fmt.Sprintf("- Suggested tool: %s\n", item.Context))
		case item.Category == "ASK_MISSING":
			sb.WriteString(fmt.Sprintf("- Missing: %s\n", item.Context))
		}
	}
	return sb.String()
}

func appendAskProgressHintMRUItems(state askAnchoredMRUState, hint *ai.ToolProgressHint) askAnchoredMRUState {
	if hint == nil {
		return state
	}
	if status := strings.TrimSpace(hint.Status); status != "" || hint.CompletionDelta > 0 {
		progressText := status
		if hint.CompletionDelta > 0 {
			if progressText != "" {
				progressText += fmt.Sprintf(" (+%.2f)", hint.CompletionDelta)
			} else {
				progressText = fmt.Sprintf("+%.2f", hint.CompletionDelta)
			}
		}
		state = appendAskMRUItem(state, "ASK_PROGRESS", progressText)
	}
	for _, tip := range hint.Tips {
		state = appendAskMRUItem(state, "ASK_TIP", tip)
	}
	for _, clue := range hint.Clues {
		state = appendAskMRUItem(state, "ASK_CLUE", clue)
	}
	for _, toolName := range hint.SuggestedNextTools {
		state = appendAskMRUItem(state, "ASK_SUGGESTED_TOOL", toolName)
	}
	for _, missing := range hint.Missing {
		state = appendAskMRUItem(state, "ASK_MISSING", missing)
	}
	return state
}

func appendAskConfirmedMRUItem(state askAnchoredMRUState, fact string) askAnchoredMRUState {
	category := askMRUCategoryConfirmed
	if joinSelectionKey, ok := classifyExecuteScriptJoinFactKey(fact); ok {
		category = fmt.Sprintf("%s_%s", askMRUCategoryConfirmedJoinSelection, normalizeMRUFactKey(joinSelectionKey))
	} else if filterSelectionKey, ok := classifyExecuteScriptFilterFactKey(fact); ok {
		category = fmt.Sprintf("%s_%s", askMRUCategoryConfirmedFilterSelection, normalizeMRUFactKey(filterSelectionKey))
	} else if details, ok := classifyConfirmedStoreFact(fact); ok && details.FactType == confirmedStoreFactTypeSchema {
		category = fmt.Sprintf("%s_%s", askMRUCategoryConfirmedStoreSchema, normalizeMRUFactKey(details.StoreName))
	} else if details, ok := classifyConfirmedStoreFact(fact); ok && details.FactType == confirmedStoreFactTypeRelations {
		category = fmt.Sprintf("%s_%s", askMRUCategoryConfirmedStoreRelation, normalizeMRUFactKey(details.CategoryKey()))
	}
	return appendAskMRUItem(state, category, fact)
}

func appendAskMRUItem(state askAnchoredMRUState, category string, context string) askAnchoredMRUState {
	if strings.TrimSpace(context) == "" {
		return state
	}
	return append(state, MRUItem{Category: category, Context: context, Scope: MRUScopeAsk})
}

func askMRUHasCategory(state askAnchoredMRUState, category string) bool {
	for _, item := range state {
		if item.Category == category {
			return true
		}
	}
	return false
}

func askMRUHasCategoryPrefix(state askAnchoredMRUState, prefix string) bool {
	for _, item := range state {
		if strings.HasPrefix(item.Category, prefix) {
			return true
		}
	}
	return false
}

func summarizeNextRepairNeed(result nativeToolResult) string {
	if result.Name == "native_tool_call" {
		return "Return exactly one valid tool call and avoid malformed function-call output."
	}
	if strings.Contains(result.Result, "Repair strategy: research_first") {
		return "Research missing schema or relation facts with list_stores before retrying execute_script."
	}
	return fmt.Sprintf("Repair %s without restarting the whole plan or broadening scope.", result.Name)
}

func summarizeAttemptedArgs(resultText string) string {
	const marker = "Attempted args:\n"
	start := strings.Index(resultText, marker)
	if start == -1 {
		return ""
	}
	start += len(marker)
	end := strings.Index(resultText[start:], "\nRetry instruction:")
	if end == -1 {
		end = len(resultText) - start
	}
	args := strings.TrimSpace(resultText[start : start+end])
	if args == "" || args == "{}" {
		return "Previous call shape existed but carried no reusable arguments."
	}
	return fmt.Sprintf("Reuse the valid structure from attempted args before changing invalid fields:\n%s", args)
}

func summarizeSuccessfulToolResult(result nativeToolResult) []string {
	if result.Name == "list_stores" {
		if facts := extractListStoresFacts(result.Result); len(facts) > 0 {
			return facts
		}
	}
	if result.Name == "execute_script" {
		facts := extractExecuteScriptPlanFacts(result.Args)
		if summary := summarizeGenericSuccessfulToolResult(result); summary != "" {
			facts = append(facts, summary)
		}
		if len(facts) > 0 {
			return facts
		}
	}

	if summary := summarizeGenericSuccessfulToolResult(result); summary != "" {
		return []string{summary}
	}
	return nil
}

func summarizeGenericSuccessfulToolResult(result nativeToolResult) string {
	trimmed := strings.TrimSpace(result.Result)
	if trimmed == "" {
		return fmt.Sprintf("%s completed successfully.", result.Name)
	}
	trimmed = strings.ReplaceAll(trimmed, "\n", " ")
	trimmed = strings.Join(strings.Fields(trimmed), " ")
	if len(trimmed) > 180 {
		trimmed = trimmed[:177] + "..."
	}
	return fmt.Sprintf("%s returned: %s", result.Name, trimmed)
}

func extractExecuteScriptPlanFacts(args map[string]any) []string {
	if len(args) == 0 {
		return nil
	}
	rawScript, ok := args["script"]
	if !ok {
		return nil
	}
	steps, ok := rawScript.([]any)
	if !ok {
		return nil
	}
	facts := make([]string, 0, len(steps))
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		op := strings.TrimSpace(fmt.Sprintf("%v", step["op"]))
		stepArgs, ok := step["args"].(map[string]any)
		if !ok {
			continue
		}
		facts = append(facts, extractExecuteScriptJoinFacts(op, stepArgs)...)
		facts = append(facts, extractExecuteScriptFilterFacts(op, stepArgs)...)
	}
	return facts
}

func extractExecuteScriptJoinFacts(op string, stepArgs map[string]any) []string {
	if op != "join" && op != "join_right" {
		return nil
	}
	store := strings.TrimSpace(fmt.Sprintf("%v", stepArgs["store"]))
	if store == "" {
		return nil
	}
	onMap, ok := stepArgs["on"].(map[string]any)
	if !ok || len(onMap) == 0 {
		return nil
	}
	leftFields := make([]string, 0, len(onMap))
	for leftField := range onMap {
		leftFields = append(leftFields, leftField)
	}
	sort.Strings(leftFields)
	facts := make([]string, 0, len(leftFields))
	for _, leftField := range leftFields {
		rightField := strings.TrimSpace(fmt.Sprintf("%v", onMap[leftField]))
		if strings.TrimSpace(leftField) == "" || rightField == "" {
			continue
		}
		facts = append(facts, fmt.Sprintf("execute_script confirmed %s store=%s on=%s->%s", op, store, leftField, rightField))
	}
	return facts
}

func extractExecuteScriptFilterFacts(op string, stepArgs map[string]any) []string {
	var rawCondition any
	switch op {
	case "filter":
		rawCondition = stepArgs["condition"]
	case "scan", "select":
		rawCondition = stepArgs["filter"]
	default:
		return nil
	}
	conditionMap, ok := rawCondition.(map[string]any)
	if !ok || len(conditionMap) == 0 {
		return nil
	}
	fields := make([]string, 0, len(conditionMap))
	for field := range conditionMap {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	facts := make([]string, 0, len(fields))
	for _, field := range fields {
		operator := extractPrimaryConditionOperator(conditionMap[field])
		if strings.TrimSpace(field) == "" || operator == "" {
			continue
		}
		facts = append(facts, fmt.Sprintf("execute_script confirmed filter field=%s op=%s", field, operator))
	}
	return facts
}

func extractPrimaryConditionOperator(rawCondition any) string {
	conditionMap, ok := rawCondition.(map[string]any)
	if !ok || len(conditionMap) == 0 {
		return ""
	}
	operators := make([]string, 0, len(conditionMap))
	for operator := range conditionMap {
		operators = append(operators, operator)
	}
	sort.Strings(operators)
	return strings.TrimSpace(operators[0])
}

func summarizeOutcomeFacts(toolResults []nativeToolResult) []string {
	if len(toolResults) == 0 {
		return nil
	}

	facts := make([]string, 0, askOutcomeFactsLimit)
	seen := make(map[string]bool, askOutcomeFactsLimit)
	for i := len(toolResults) - 1; i >= 0 && len(facts) < askOutcomeFactsLimit; i-- {
		result := toolResults[i]
		if strings.Contains(result.Result, "Retry instruction:") {
			continue
		}
		for _, fact := range summarizeSuccessfulToolResult(result) {
			fact = strings.TrimSpace(fact)
			if fact == "" || seen[fact] {
				continue
			}
			seen[fact] = true
			facts = append(facts, fact)
			if len(facts) >= askOutcomeFactsLimit {
				break
			}
		}
	}
	return facts
}

func summarizeOutcomeRecipes(toolResults []nativeToolResult) []ai.LearnedRecipe {
	if len(toolResults) == 0 {
		return nil
	}

	recipes := make([]ai.LearnedRecipe, 0, 2)
	seen := make(map[string]bool, 2)
	appendRecipe := func(recipe ai.LearnedRecipe) {
		if strings.TrimSpace(recipe.ID) == "" || seen[recipe.ID] {
			return
		}
		seen[recipe.ID] = true
		recipes = append(recipes, recipe)
	}

	for i, result := range toolResults {
		if !isRecoverableRepairResult(result, "execute_script", nativeRepairStrategyResearchFirst) {
			continue
		}
		if hasSuccessfulToolSequence(toolResults[i+1:], "list_stores", "execute_script") {
			appendRecipe(ai.LearnedRecipe{
				ID:      "implicit.execute_script.research_then_retry",
				Kind:    RecipeKindImplicit,
				Scope:   RecipeScopeMicro,
				Domain:  StoresDomain,
				Topic:   "Research grounded schema before execute_script retry",
				Trigger: "execute_script repair requires missing schema or relation facts",
				Protocol: []string{
					"Call list_stores first to confirm the active store schema and relations.",
					"Reuse the confirmed names as the source of truth instead of guessing field or join paths.",
					"Retry execute_script with corrected grounded arguments without restarting the whole plan.",
				},
				Invariants: []string{
					"Preserve valid script slices that already conform to the plan.",
					"Do not broaden scope before the grounded retry is attempted.",
				},
				Confidence: 0.95,
				Source:     "inner_loop_success",
			})
		}
	}

	for i, result := range toolResults {
		if !isRecoverableRepairResult(result, "execute_script", nativeRepairStrategySameTool) {
			continue
		}
		if hasSuccessfulTool(toolResults[i+1:], "execute_script") {
			appendRecipe(ai.LearnedRecipe{
				ID:      "implicit.execute_script.repair_in_place",
				Kind:    RecipeKindImplicit,
				Scope:   RecipeScopeMicro,
				Domain:  StoresDomain,
				Topic:   "Repair execute_script in place",
				Trigger: "execute_script has a recoverable argument-shape error",
				Protocol: []string{
					"Retry the same tool instead of abandoning the plan.",
					"Preserve valid arguments and rewrite only the malformed or missing slice.",
					"Keep the repaired call grounded in the already confirmed store and field names.",
				},
				Invariants: []string{
					"Do not replace valid join or filter clauses that already work.",
					"Do not switch to unrelated tools until the repair attempt succeeds or is disproven.",
				},
				Confidence: 0.9,
				Source:     "inner_loop_success",
			})
		}
	}

	return recipes
}

func detectAskLoopProgress(previousResults []nativeToolResult, currentResults []nativeToolResult) askLoopProgressDelta {
	if len(currentResults) == 0 {
		return askLoopProgressDelta{}
	}
	latest := currentResults[len(currentResults)-1]
	if strings.Contains(latest.Result, "Retry instruction:") {
		return askLoopProgressDelta{}
	}
	delta := askLoopProgressDelta{
		NewFacts:   diffStringsAsSet(summarizeOutcomeFacts(currentResults), summarizeOutcomeFacts(previousResults)),
		NewRecipes: diffRecipeIDs(summarizeOutcomeRecipes(currentResults), summarizeOutcomeRecipes(previousResults)),
		HintSignal: cloneToolProgressHint(latest.Hint),
	}
	delta.Progressing = len(delta.NewFacts) > 0 || len(delta.NewRecipes) > 0 || toolProgressHintSignalsProgress(latest.Hint)
	return delta
}

func diffStringsAsSet(current []string, previous []string) []string {
	if len(current) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(previous))
	for _, item := range previous {
		seen[strings.TrimSpace(item)] = true
	}
	delta := make([]string, 0, len(current))
	for _, item := range current {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		delta = append(delta, trimmed)
	}
	return delta
}

func diffRecipeIDs(current []ai.LearnedRecipe, previous []ai.LearnedRecipe) []string {
	if len(current) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(previous))
	for _, item := range previous {
		seen[strings.TrimSpace(item.ID)] = true
	}
	delta := make([]string, 0, len(current))
	for _, item := range current {
		id := strings.TrimSpace(item.ID)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		delta = append(delta, id)
	}
	return delta
}

func toolProgressHintSignalsProgress(hint *ai.ToolProgressHint) bool {
	if hint == nil {
		return false
	}
	status := strings.ToLower(strings.TrimSpace(hint.Status))
	if status == "progressing" || status == "complete" || status == "closer" {
		return true
	}
	return hint.CompletionDelta > 0 || len(hint.Tips) > 0 || len(hint.Clues) > 0 || len(hint.SuggestedNextTools) > 0
}

func shouldShortCircuitAskLoopOnToolHint(hint *ai.ToolProgressHint) bool {
	if hint == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(hint.Status)) {
	case "anti_success", "blocked", "error", "failed", "fatal", "hard_error", "terminal_error":
		return true
	default:
		return false
	}
}

func unwrapToolResultEnvelope(rawResult string) (string, *ai.ToolProgressHint) {
	trimmed := strings.TrimSpace(rawResult)
	if trimmed == "" {
		return rawResult, nil
	}
	var envelope ai.ToolResultEnvelope
	if err := json.Unmarshal([]byte(trimmed), &envelope); err != nil {
		return rawResult, nil
	}
	if len(envelope.ToolResult) == 0 && envelope.ProgressHint == nil {
		return rawResult, nil
	}
	result := formatEnvelopeToolResult(envelope.ToolResult)
	if strings.TrimSpace(result) == "" {
		result = rawResult
	}
	return result, cloneToolProgressHint(envelope.ProgressHint)
}

func formatEnvelopeToolResult(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var value any
	if err := json.Unmarshal(raw, &value); err == nil {
		if bytes, marshalErr := json.Marshal(value); marshalErr == nil {
			return string(bytes)
		}
	}
	return string(raw)
}

func cloneToolProgressHint(hint *ai.ToolProgressHint) *ai.ToolProgressHint {
	if hint == nil {
		return nil
	}
	cloned := *hint
	cloned.Missing = append([]string(nil), hint.Missing...)
	cloned.Tips = append([]string(nil), hint.Tips...)
	cloned.Clues = append([]string(nil), hint.Clues...)
	cloned.SuggestedNextTools = append([]string(nil), hint.SuggestedNextTools...)
	return &cloned
}

func isRecoverableRepairResult(result nativeToolResult, toolName string, strategy nativeRepairStrategy) bool {
	if result.Name != toolName || !strings.Contains(result.Result, "Retry instruction:") {
		return false
	}
	if strategy == "" {
		return true
	}
	return strings.Contains(result.Result, fmt.Sprintf("Repair strategy: %s", strategy))
}

func hasSuccessfulTool(results []nativeToolResult, toolName string) bool {
	for _, result := range results {
		if result.Name == toolName && !strings.Contains(result.Result, "Retry instruction:") {
			return true
		}
	}
	return false
}

func hasSuccessfulToolSequence(results []nativeToolResult, names ...string) bool {
	if len(names) == 0 {
		return false
	}
	nameIdx := 0
	for _, result := range results {
		if strings.Contains(result.Result, "Retry instruction:") {
			continue
		}
		if result.Name != names[nameIdx] {
			continue
		}
		nameIdx++
		if nameIdx == len(names) {
			return true
		}
	}
	return false
}

func extractListStoresFacts(resultText string) []string {
	trimmed := strings.TrimSpace(resultText)
	if trimmed == "" {
		return nil
	}

	lines := strings.Split(trimmed, "\n")
	facts := make([]string, 0, len(lines)*2)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.EqualFold(line, "Stores:") {
			continue
		}

		if idx := strings.Index(line, " schema="); idx > 0 {
			storeName := strings.TrimSpace(line[:idx])
			remainder := strings.TrimSpace(line[idx+1:])
			relationIdx := strings.Index(remainder, " relations=")
			if relationIdx >= 0 {
				schemaPart := strings.TrimSpace(remainder[:relationIdx])
				relationsPart := strings.TrimSpace(remainder[relationIdx+1:])
				facts = append(facts, fmt.Sprintf("list_stores confirmed %s %s", storeName, schemaPart))
				facts = append(facts, fmt.Sprintf("list_stores confirmed %s %s", storeName, relationsPart))
				continue
			}
			facts = append(facts, fmt.Sprintf("list_stores confirmed %s %s", storeName, remainder))
			continue
		}

		facts = append(facts, fmt.Sprintf("list_stores returned: %s", line))
	}
	return facts
}

func formatRecoverableToolError(repair pendingToolRepair, args map[string]any, err error) string {
	argsJSON := "{}"
	if len(args) > 0 {
		if b, marshalErr := json.MarshalIndent(args, "", "  "); marshalErr == nil {
			argsJSON = string(b)
		}
	}

	categoryLine := ""
	exampleLine := ""
	strategyLine := ""
	var validationErr *executeScriptValidationError
	if errors.As(err, &validationErr) {
		if validationErr.Category != "" {
			categoryLine = fmt.Sprintf("\nRepair category: %s", validationErr.Category)
		}
		if validationErr.Example != "" {
			exampleLine = fmt.Sprintf("\nSuggested fix example:\n%s", validationErr.Example)
		}
	}
	if repair.Strategy != "" {
		strategyLine = fmt.Sprintf("\nRepair strategy: %s", repair.Strategy)
		if repair.Strategy == nativeRepairStrategyResearchFirst && repair.ResearchReason != "" {
			strategyLine += fmt.Sprintf("\nResearch reason: %s", repair.ResearchReason)
		}
	}

	retryInstruction := "Retry instruction: Return a corrected call for the same tool. Preserve valid arguments, fix invalid or missing arguments, and do not repeat the same malformed shape."
	if repair.Strategy == nativeRepairStrategyResearchFirst {
		researchTool := repair.ResearchTool
		if researchTool == "" {
			researchTool = "list_stores"
		}
		retryInstruction = fmt.Sprintf("Retry instruction: Call %s first to research the missing schema or relation facts, then return to %s with corrected grounded arguments. Preserve valid arguments and do not restart the whole plan.", researchTool, repair.ToolName)
	}

	return fmt.Sprintf(
		"Tool execution error: %v\nTool: %s%s%s%s\nAttempted args:\n%s\n%s",
		err,
		repair.ToolName,
		categoryLine,
		exampleLine,
		strategyLine,
		argsJSON,
		retryInstruction,
	)
}

func formatRecoverableGenerationError(err error) string {
	return fmt.Sprintf(
		"Model generation error: %v\nRetry instruction: Return exactly one valid native tool call that conforms to the provided tool schema. Do not emit malformed function calls, partial arguments, or placeholder-only argument shapes.",
		err,
	)
}

func formatPendingRepairReminder(repair pendingToolRepair, attemptedToolName string) string {
	if repair.Strategy == nativeRepairStrategyResearchFirst {
		allowedTool := repair.ResearchTool
		if allowedTool == "" {
			allowedTool = "list_stores"
		}
		return fmt.Sprintf(
			"Repair required before continuing. The previous call to %s failed with recoverable argument errors and needs grounded research first. The model attempted %s instead. Retry instruction: Call %s next, use its schema/relations output as source of truth, then return to %s. Do not switch to unrelated tools or provide a final answer until the research attempt is made.",
			repair.ToolName,
			attemptedToolName,
			allowedTool,
			repair.ToolName,
		)
	}
	return fmt.Sprintf(
		"Repair required before continuing. The previous call to %s failed with recoverable argument errors and must be corrected first. The model attempted %s instead. Retry instruction: Call %s next with corrected arguments. Do not switch tools or provide a final answer until the repair attempt is made.",
		repair.ToolName,
		attemptedToolName,
		repair.ToolName,
	)
}

func (r pendingToolRepair) allowsTool(toolName string) bool {
	if r.Strategy == nativeRepairStrategyResearchFirst {
		researchTool := r.ResearchTool
		if researchTool == "" {
			researchTool = "list_stores"
		}
		return toolName == researchTool
	}
	return toolName == r.ToolName
}

func classifyRecoverableToolRepair(toolName string, err error) pendingToolRepair {
	repair := pendingToolRepair{ToolName: toolName, Strategy: nativeRepairStrategySameTool}
	if toolName != "execute_script" || err == nil {
		return repair
	}

	if validationErr := new(executeScriptValidationError); errors.As(err, &validationErr) {
		switch validationErr.Category {
		case "invalid_join_on_placeholder", "invalid_join_on_field_placeholder", "invalid_filter_field_placeholder":
			repair.Strategy = nativeRepairStrategyResearchFirst
			repair.ResearchTool = "list_stores"
			repair.ResearchReason = "The failure indicates missing grounded schema or relation mapping facts."
			return repair
		}
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(message, "relation") || strings.Contains(message, "schema") || strings.Contains(message, "join mapping") {
		repair.Strategy = nativeRepairStrategyResearchFirst
		repair.ResearchTool = "list_stores"
		repair.ResearchReason = "The failure text points to unresolved schema or relation ambiguity."
	}
	return repair
}

func sanitizeToolCallArgs(args map[string]any, enableObfuscation bool) {
	cleanToken := func(s string) string {
		s = strings.Trim(s, "*_`")
		s = strings.ReplaceAll(s, "\u00a0", " ")
		s = strings.TrimSpace(s)
		s = strings.Trim(s, "\"'")
		return strings.TrimSpace(s)
	}

	var sanitize func(any) any
	sanitize = func(v any) any {
		switch val := v.(type) {
		case string:
			val = cleanToken(val)
			if enableObfuscation {
				val = obfuscation.GlobalObfuscator.DeobfuscateText(val)
			}
			return val
		case []any:
			for i, item := range val {
				val[i] = sanitize(item)
			}
			return val
		case map[string]any:
			cleaned := make(map[string]any, len(val))
			for k, item := range val {
				cleaned[cleanToken(k)] = sanitize(item)
			}
			return cleaned
		default:
			return val
		}
	}

	for k, v := range args {
		args[k] = sanitize(v)
	}
}

func summarizeToolDefinitions(tools []ai.ToolDefinition) string {
	if len(tools) == 0 {
		return ""
	}
	names := make([]string, 0, len(tools))
	for i, tool := range tools {
		if i >= 12 {
			names = append(names, "...")
			break
		}
		names = append(names, tool.Name)
	}
	return strings.Join(names, ",")
}

func summarizeToolArgKeys(args map[string]any) string {
	if len(args) == 0 {
		return ""
	}
	keys := make([]string, 0, len(args))
	for key := range args {
		keys = append(keys, key)
	}
	return strings.Join(keys, ",")
}

func getRequestedOutputFormat(ctx context.Context) string {
	format, _ := ctx.Value(ai.CtxKeyDefaultFormat).(string)
	return strings.ToLower(strings.TrimSpace(format))
}

func shouldPreserveStructuredToolResult(ctx context.Context, toolResults []nativeToolResult) bool {
	if len(toolResults) == 0 {
		return false
	}
	lastTool := toolResults[len(toolResults)-1]
	if !shouldPreserveStructuredToolName(lastTool.Name) {
		return false
	}
	switch getRequestedOutputFormat(ctx) {
	case "csv", "json", "ndjson", "tsv":
		return true
	default:
		return false
	}
}

func shouldPreserveStructuredToolName(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "execute_script":
		return true
	default:
		return false
	}
}

func preserveStructuredToolResult(result string, enableObfuscation bool) string {
	if enableObfuscation {
		return obfuscation.GlobalObfuscator.DeobfuscateText(result)
	}
	return result
}
