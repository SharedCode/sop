package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	log "log/slog"
	"strings"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/obfuscation"
)

// NativeReActEngine implements ReasoningEngine using native LLM API tool calling.
type NativeReActEngine struct {
	EnableObfuscation bool
}

const nativeReActMaxToolIterations = 4

type nativeToolResult struct {
	Name   string
	Result string
}

type pendingToolRepair struct {
	ToolName string
}

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
	var pendingRepair *pendingToolRepair
	for iteration := 0; iteration < nativeReActMaxToolIterations; iteration++ {
		emitVerboseProgress(ctx, "Reasoning iteration %d of %d.", iteration+1, nativeReActMaxToolIterations)
		mainPrompt := buildNativeReActPrompt(req, toolResults)
		emitVerboseProgress(ctx, "Waiting for model response.")
		output, err := req.Generator.Generate(ctx, mainPrompt, ai.GenOptions{
			SystemPrompt: req.SystemPrompt,
			MaxTokens:    1024,
			Temperature:  0.7,
			Tools:        tools,
		})
		if err != nil {
			return ai.ReasoningResponse{}, fmt.Errorf("generation failed: %w", err)
		}
		log.Info("Native ReAct Engine Output",
			"iteration", iteration+1,
			"text_chars", len(output.Text),
			"tool_call_count", len(output.ToolCalls),
		)

		if req.Executor == nil || len(output.ToolCalls) == 0 {
			emitVerboseProgress(ctx, "No further tools required; preparing final answer.")
			if shouldPreserveStructuredToolResult(ctx, toolResults) {
				return ai.ReasoningResponse{
					FinalText: preserveStructuredToolResult(toolResults[len(toolResults)-1].Result, e.EnableObfuscation),
					ToolCalls: executedToolCalls,
				}, nil
			}

			finalText := output.Text
			if e.EnableObfuscation {
				finalText = obfuscation.GlobalObfuscator.DeobfuscateText(output.Text)
			}

			return ai.ReasoningResponse{
				FinalText: finalText,
				ToolCalls: executedToolCalls,
			}, nil
		}

		toolCall := output.ToolCalls[0]
		if pendingRepair != nil && toolCall.Name != pendingRepair.ToolName {
			emitVerboseProgress(ctx, "Tool `%s` must be corrected before other actions.", pendingRepair.ToolName)
			toolResults = append(toolResults, nativeToolResult{
				Name:   pendingRepair.ToolName,
				Result: formatPendingRepairReminder(pendingRepair.ToolName, toolCall.Name),
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

		result, err := req.Executor.Execute(ctx, toolCall.Name, toolCall.Args)
		if err != nil {
			emitReasoningEvent(req, "tool_error", map[string]any{
				"tool":      toolCall.Name,
				"args":      cloneToolEventMap(toolCall.Args),
				"error":     err.Error(),
				"iteration": iteration + 1,
			})
			if isRecoverableToolExecutionError(err) {
				emitVerboseProgress(ctx, "Tool `%s` needs corrected arguments; retrying.", toolCall.Name)
				pendingRepair = &pendingToolRepair{ToolName: toolCall.Name}
				toolResults = append(toolResults, nativeToolResult{
					Name:   toolCall.Name,
					Result: formatRecoverableToolError(toolCall.Name, toolCall.Args, err),
				})
				log.Warn("Native ReAct Engine Recoverable Tool Failure", "tool", toolCall.Name, "error", err)
				continue
			}
			log.Error("Native ReAct Engine Tool Failure", "tool", toolCall.Name, "error", err)
			return ai.ReasoningResponse{}, fmt.Errorf("tool execution failed: %w", err)
		}
		emitVerboseProgress(ctx, "Tool `%s` completed.", toolCall.Name)
		pendingRepair = nil
		log.Info("Native ReAct Engine Tool Success", "iteration", iteration+1, "tool", toolCall.Name, "result_chars", len(result))
		emitReasoningEvent(req, "tool_result", map[string]any{
			"tool":         toolCall.Name,
			"args":         cloneToolEventMap(toolCall.Args),
			"result":       result,
			"result_chars": len(result),
			"iteration":    iteration + 1,
		})

		executedToolCalls = append(executedToolCalls, toolCall)
		toolResults = append(toolResults, nativeToolResult{Name: toolCall.Name, Result: result})
		if shouldPreserveStructuredToolResult(ctx, toolResults) {
			return ai.ReasoningResponse{
				FinalText: preserveStructuredToolResult(result, e.EnableObfuscation),
				ToolCalls: executedToolCalls,
			}, nil
		}
	}

	emitVerboseProgress(ctx, "Reached tool iteration limit; synthesizing final answer.")
	if shouldPreserveStructuredToolResult(ctx, toolResults) {
		return ai.ReasoningResponse{
			FinalText: preserveStructuredToolResult(toolResults[len(toolResults)-1].Result, e.EnableObfuscation),
			ToolCalls: executedToolCalls,
		}, nil
	}

	log.Warn("Native ReAct Engine Reached Tool Iteration Limit", "limit", nativeReActMaxToolIterations)
	finalPrompt := buildNativeReActPrompt(req, toolResults) + "\n\nUser: Analyze the tool response and provide the final answer to the user. Do not call any more tools."
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
	return ai.ReasoningResponse{FinalText: finalText, ToolCalls: executedToolCalls}, nil
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

func buildNativeReActPrompt(req ai.ReasoningRequest, toolResults []nativeToolResult) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Context:\n%s%s\n\nUser Query: %s", req.ContextText, req.HistoryText, req.UserQuery))
	if len(toolResults) == 0 {
		return sb.String()
	}

	if last := toolResults[len(toolResults)-1]; strings.Contains(last.Result, "Retry instruction:") {
		sb.WriteString(fmt.Sprintf("\n\nRepair directive: The last tool call to %s failed because its arguments were invalid. Your next step should be to call the same tool again with corrected arguments using the repair guidance below. Do not summarize the error as a final answer unless correction is impossible.", last.Name))
	}

	sb.WriteString("\n\nTool results:\n")
	for i, result := range toolResults {
		sb.WriteString(fmt.Sprintf("Step %d Tool: %s\n[System Tool Response]:\n%s\n\n", i+1, result.Name, result.Result))
	}
	sb.WriteString("User: Analyze the tool response and continue the task. If another tool is required, call it. Otherwise provide the final answer to the user.")
	return sb.String()
}

func formatRecoverableToolError(toolName string, args map[string]any, err error) string {
	argsJSON := "{}"
	if len(args) > 0 {
		if b, marshalErr := json.MarshalIndent(args, "", "  "); marshalErr == nil {
			argsJSON = string(b)
		}
	}

	categoryLine := ""
	exampleLine := ""
	var validationErr *executeScriptValidationError
	if errors.As(err, &validationErr) {
		if validationErr.Category != "" {
			categoryLine = fmt.Sprintf("\nRepair category: %s", validationErr.Category)
		}
		if validationErr.Example != "" {
			exampleLine = fmt.Sprintf("\nSuggested fix example:\n%s", validationErr.Example)
		}
	}

	return fmt.Sprintf(
		"Tool execution error: %v\nTool: %s%s%s\nAttempted args:\n%s\nRetry instruction: Return a corrected call for the same tool. Preserve valid arguments, fix invalid or missing arguments, and do not repeat the same malformed shape.",
		err,
		toolName,
		categoryLine,
		exampleLine,
		argsJSON,
	)
}

func formatPendingRepairReminder(expectedToolName, attemptedToolName string) string {
	return fmt.Sprintf(
		"Repair required before continuing. The previous call to %s failed with recoverable argument errors and must be corrected first. The model attempted %s instead. Retry instruction: Call %s next with corrected arguments. Do not switch tools or provide a final answer until the repair attempt is made.",
		expectedToolName,
		attemptedToolName,
		expectedToolName,
	)
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
