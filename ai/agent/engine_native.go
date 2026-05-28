package agent

import (
	"context"
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
				toolResults = append(toolResults, nativeToolResult{
					Name:   toolCall.Name,
					Result: fmt.Sprintf("Tool execution error: %v", err),
				})
				log.Warn("Native ReAct Engine Recoverable Tool Failure", "tool", toolCall.Name, "error", err)
				continue
			}
			log.Error("Native ReAct Engine Tool Failure", "tool", toolCall.Name, "error", err)
			return ai.ReasoningResponse{}, fmt.Errorf("tool execution failed: %w", err)
		}
		emitVerboseProgress(ctx, "Tool `%s` completed.", toolCall.Name)
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

	sb.WriteString("\n\nTool results:\n")
	for i, result := range toolResults {
		sb.WriteString(fmt.Sprintf("Step %d Tool: %s\n[System Tool Response]:\n%s\n\n", i+1, result.Name, result.Result))
	}
	sb.WriteString("User: Analyze the tool response and continue the task. If another tool is required, call it. Otherwise provide the final answer to the user.")
	return sb.String()
}

func sanitizeToolCallArgs(args map[string]any, enableObfuscation bool) {
	var sanitize func(any) any
	sanitize = func(v any) any {
		switch val := v.(type) {
		case string:
			val = strings.Trim(val, "*_`")
			val = strings.ReplaceAll(val, "\u00a0", " ")
			val = strings.TrimSpace(val)
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
			for k, item := range val {
				val[k] = sanitize(item)
			}
			return val
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
