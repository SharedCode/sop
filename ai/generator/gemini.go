package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// gemini implements the Generator interface for Google's Gemini models.
type gemini struct {
	apiKey string
	model  string
}

func init() {
	Register("gemini", func(cfg map[string]any) (ai.Generator, error) {
		apiKey, _ := cfg["api_key"].(string)
		if apiKey == "" {
			apiKey = os.Getenv("LLM_API_KEY")
			if apiKey == "" {
				apiKey = os.Getenv("GEMINI_API_KEY")
			}
		}
		if strings.HasPrefix(apiKey, "sk-") {
			return nil, fmt.Errorf("detected OpenAI API key (starts with 'sk-') but generator type is 'gemini'. Please check your configuration")
		}
		model, _ := cfg["model"].(string)
		if model == "" {
			model = os.Getenv("GEMINI_MODEL")
		}
		if model == "" {
			model = ai.DefaultModelGemini
		}
		return &gemini{apiKey: apiKey, model: model}, nil
	})
}

// Name returns the name of the generator.
func (g *gemini) Name() string { return "gemini" }

func (g *gemini) CarryoverCapability() ai.CarryoverCapability {
	return ai.CarryoverCapability{
		Provider:        g.Name(),
		Model:           strings.TrimSpace(g.model),
		SupportsCompact: true,
		SupportsLive:    false,
	}
}

func (g *gemini) ReActLoop() ai.ReActLoop {
	return geminiOwnedReActLoop{
		generator:     g,
		maxIterations: 3,
	}
}

func (g *gemini) ReActTurnStrategy() ai.ReActTurnStrategy {
	return geminiReActTurnStrategy{}
}

type geminiReActTurnStrategy struct {
	Base ai.ReActTurnStrategy
}

func (geminiReActTurnStrategy) ShouldBypassPrompt(turn ai.ReActTurn) bool {
	return len(turn.Options.ToolCallContinuations) > 0
}

func (l geminiReActTurnStrategy) PrepareTurn(ctx context.Context, turn ai.ReActTurn) ai.ReActTurn {
	base := l.Base
	if base == nil {
		base = ai.DefaultReActTurnStrategy{}
	}
	turn = base.PrepareTurn(ctx, turn)
	return prepareGeminiContinuationTurn(turn, 0)
}

type geminiOwnedReActLoop struct {
	generator ai.Generator
	// turnStrategy is optional in the owned loop and is kept only for narrow
	// test hooks or explicit local overrides. The owned loop prepares its own
	// Gemini-native continuation turns instead of depending on shared strategy logic.
	turnStrategy  ai.ReActTurnStrategy
	maxIterations int
}

func (l geminiOwnedReActLoop) Run(ctx context.Context, req ai.ReasoningRequest) (ai.ReasoningResponse, error) {
	if l.generator == nil {
		return ai.ReasoningResponse{}, fmt.Errorf("gemini owned loop requires a generator")
	}

	var tools []ai.ToolDefinition
	var err error
	if req.Executor != nil {
		tools, err = req.Executor.ListTools(ctx)
		if err != nil {
			return ai.ReasoningResponse{}, fmt.Errorf("failed to list tools: %w", err)
		}
	}

	continuations := make([]ai.ToolCallContinuation, 0)
	toolResults := make([]ai.ReActToolResult, 0)
	executedToolCalls := make([]ai.ToolCall, 0)

	for iteration := 1; iteration <= l.maxIterations; iteration++ {
		turn := ai.ReActTurn{
			Iteration: iteration,
			UserQuery: req.UserQuery,
			Prompt:    geminiOwnedLoopPrompt(req),
			Options: ai.GenOptions{
				SystemPrompt:          req.SystemPrompt,
				Tools:                 tools,
				Temperature:           0.1,
				ToolCallContinuations: append([]ai.ToolCallContinuation(nil), continuations...),
			},
			ToolResults: cloneGeminiReActToolResults(toolResults),
		}
		turn = prepareGeminiContinuationTurn(turn, l.maxIterations)
		if l.turnStrategy != nil {
			turn = l.turnStrategy.PrepareTurn(ctx, turn)
		}
		if strings.TrimSpace(turn.Prompt) == "" {
			turn.Prompt = geminiOwnedLoopPrompt(req)
		}

		output, err := l.generator.Generate(ctx, turn.Prompt, turn.Options)
		if err != nil {
			return ai.ReasoningResponse{}, fmt.Errorf("generation failed: %w", err)
		}
		if req.Executor == nil || len(output.ToolCalls) == 0 {
			resp := geminiOwnedLoopResponse(output.Text, executedToolCalls, toolResults, continuations)
			emitGeminiOwnedLoopHydration(req, resp)
			return resp, nil
		}

		for _, toolCall := range output.ToolCalls {
			emitGeminiOwnedLoopEvent(req, "tool_call", map[string]any{
				"tool":      toolCall.Name,
				"args":      cloneGeminiToolArgs(toolCall.Args),
				"iteration": iteration,
			})
			executedToolCalls = append(executedToolCalls, toolCall)
			toolResult, continuation := executeGeminiOwnedLoopToolCall(ctx, req, iteration, toolCall)
			toolResults = append(toolResults, toolResult)
			continuations = append(continuations, continuation)
			emitGeminiOwnedLoopHydration(req, geminiOwnedLoopResponse("", executedToolCalls, toolResults, continuations))
			if shouldShortCircuitGeminiOwnedLoopOnToolHint(toolResult.Hint) {
				resp := geminiOwnedLoopResponse(toolResult.Result, executedToolCalls, toolResults, continuations)
				emitGeminiOwnedLoopHydration(req, resp)
				return resp, nil
			}
		}
	}

	finalTurn := ai.ReActTurn{
		Iteration: l.maxIterations + 1,
		UserQuery: req.UserQuery,
		Options: ai.GenOptions{
			SystemPrompt:          req.SystemPrompt,
			Temperature:           0.7,
			ToolCallContinuations: append([]ai.ToolCallContinuation(nil), continuations...),
		},
		ToolResults: cloneGeminiReActToolResults(toolResults),
		FinalTurn:   true,
	}
	finalTurn = prepareGeminiContinuationTurn(finalTurn, l.maxIterations)
	if l.turnStrategy != nil {
		finalTurn = l.turnStrategy.PrepareTurn(ctx, finalTurn)
	}
	finalTurn.Options.Tools = nil
	if strings.TrimSpace(finalTurn.Prompt) == "" {
		finalTurn.Prompt = "Using the supplied tool-call state, do not call more tools. Briefly explain what is still blocking progress and ask one short, concrete clarification question."
	}
	output, err := l.generator.Generate(ctx, finalTurn.Prompt, finalTurn.Options)
	if err != nil {
		return ai.ReasoningResponse{}, fmt.Errorf("final generation failed: %w", err)
	}
	resp := geminiOwnedLoopResponse(output.Text, executedToolCalls, toolResults, continuations)
	emitGeminiOwnedLoopHydration(req, resp)
	return resp, nil
}

func geminiOwnedLoopResponse(finalText string, toolCalls []ai.ToolCall, toolResults []ai.ReActToolResult, continuations []ai.ToolCallContinuation) ai.ReasoningResponse {
	resp := ai.ReasoningResponse{
		FinalText:      finalText,
		ToolCalls:      toolCalls,
		OutcomeFacts:   ai.SummarizeOutcomeFacts(toolResults),
		OutcomeRecipes: ai.SummarizeOutcomeRecipes(toolResults),
	}
	if carryState := geminiOwnedLoopCarryoverState(continuations); carryState != nil {
		resp.CarryoverState = carryState
	}
	return resp
}

func geminiOwnedLoopCarryoverState(continuations []ai.ToolCallContinuation) *ai.CarryoverState {
	if len(continuations) == 0 {
		return nil
	}
	raw, err := json.Marshal(continuations)
	if err != nil {
		return &ai.CarryoverState{Mode: ai.CarryoverModeCompact}
	}
	return &ai.CarryoverState{
		Mode:                   ai.CarryoverModeCompact,
		EstimatedRawToolTokens: (len(raw) + 3) / 4,
	}
}

func emitGeminiOwnedLoopHydration(req ai.ReasoningRequest, resp ai.ReasoningResponse) {
	if req.HydrationSink == nil {
		return
	}
	req.HydrationSink(ai.BuildMemoryHydrationUpdateFromParts(ai.MemoryHydrationUpdate{
		FinalText:      resp.FinalText,
		ToolCalls:      resp.ToolCalls,
		OutcomeFacts:   resp.OutcomeFacts,
		OutcomeRecipes: resp.OutcomeRecipes,
		CarryoverState: resp.CarryoverState,
	}))
}

func geminiOwnedLoopPrompt(req ai.ReasoningRequest) string {
	parts := make([]string, 0, 3)
	if contextText := strings.TrimSpace(req.ContextText); contextText != "" {
		parts = append(parts, "Context:\n"+contextText)
	}
	if historyText := strings.TrimSpace(req.HistoryText); historyText != "" {
		parts = append(parts, "History:\n"+historyText)
	}
	if query := strings.TrimSpace(req.UserQuery); query != "" {
		parts = append(parts, "User Query: "+query)
	}
	return strings.Join(parts, "\n\n")
}

func prepareGeminiContinuationTurn(turn ai.ReActTurn, maxIterations int) ai.ReActTurn {
	if len(turn.Options.ToolCallContinuations) == 0 {
		return turn
	}
	turn.Options.ToolCallContinuations = geminiToolCallContinuations(turn, maxIterations)
	if turn.FinalTurn {
		turn.Prompt = "Using the supplied tool-call state, do not call more tools. Briefly explain what is still blocking progress and ask one short, concrete clarification question."
		turn.Options.Tools = nil
		return turn
	}
	turn.Prompt = "Continue from the supplied tool-call state. Use the structured function response as the source of truth, avoid replaying prior history, emit the next tool call if more work is needed, and otherwise answer the user directly."
	return turn
}

func geminiToolCallContinuations(turn ai.ReActTurn, maxIterations int) []ai.ToolCallContinuation {
	continuations := turn.Options.ToolCallContinuations
	if len(continuations) == 0 {
		return nil
	}

	wrapped := make([]ai.ToolCallContinuation, 0, len(continuations))
	for idx, continuation := range continuations {
		response := continuation.Response
		if idx == len(continuations)-1 {
			phase, actions, remainingToolCalls := geminiLoopMetadata(turn, maxIterations)
			response = map[string]any{
				"tool_result": continuation.Response,
				"react_state": map[string]any{
					"user_query":           strings.TrimSpace(turn.UserQuery),
					"latest_tool":          strings.TrimSpace(continuation.ToolCall.Name),
					"repair_directive":     strings.TrimSpace(turn.RepairDirective),
					"iteration":            turn.Iteration,
					"task_status":          geminiTaskStatus(turn),
					"has_more_tool_work":   !turn.FinalTurn,
					"phase":                string(phase),
					"allowed_next_actions": geminiAllowedActions(actions),
					"remaining_tool_calls": remainingToolCalls,
				},
			}
		}
		wrapped = append(wrapped, ai.ToolCallContinuation{
			ToolCall: continuation.ToolCall,
			Response: response,
		})
	}
	return wrapped
}

func geminiTaskStatus(turn ai.ReActTurn) string {
	if turn.FinalTurn {
		return "clarification_required"
	}
	if strings.TrimSpace(turn.RepairDirective) != "" {
		return "repair_required"
	}
	if len(turn.ToolResults) > 0 {
		return "tool_progressed"
	}
	return "active"
}

func geminiLoopMetadata(turn ai.ReActTurn, maxIterations int) (ai.ReActLoopPhase, []ai.ReActNextAction, int) {
	if turn.FinalTurn {
		return ai.ReActLoopPhaseClarification, []ai.ReActNextAction{ai.ReActNextActionAskClarification, ai.ReActNextActionAnswerUser}, 0
	}
	if strings.TrimSpace(turn.RepairDirective) != "" {
		return ai.ReActLoopPhaseRepair, []ai.ReActNextAction{ai.ReActNextActionRetrySameTool, ai.ReActNextActionAskClarification}, 0
	}
	remainingToolCalls := 0
	if maxIterations <= 0 {
		maxIterations = turn.LoopState.MaxIterations
		if maxIterations <= 0 {
			maxIterations = turn.LoopState.AllowedIterations
		}
	}
	if maxIterations > 0 {
		remainingToolCalls = max(0, maxIterations-turn.Iteration+1)
	}
	return ai.ReActLoopPhaseActive, []ai.ReActNextAction{ai.ReActNextActionCallTool, ai.ReActNextActionAnswerUser}, remainingToolCalls
}

func geminiAllowedActions(actions []ai.ReActNextAction) []string {
	if len(actions) == 0 {
		return nil
	}
	allowed := make([]string, 0, len(actions))
	for _, action := range actions {
		allowed = append(allowed, string(action))
	}
	return allowed
}

func shouldShortCircuitGeminiOwnedLoopOnToolHint(hint *ai.ToolProgressHint) bool {
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

func cloneGeminiReActToolResults(results []ai.ReActToolResult) []ai.ReActToolResult {
	if len(results) == 0 {
		return nil
	}
	cloned := make([]ai.ReActToolResult, 0, len(results))
	for _, result := range results {
		cloned = append(cloned, ai.ReActToolResult{
			Name:   result.Name,
			Args:   cloneGeminiToolArgs(result.Args),
			Result: result.Result,
			Hint:   result.Hint,
		})
	}
	return cloned
}

func cloneGeminiToolArgs(args map[string]any) map[string]any {
	if len(args) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(args))
	for key, value := range args {
		cloned[key] = value
	}
	return cloned
}

func executeGeminiOwnedLoopToolCall(ctx context.Context, req ai.ReasoningRequest, iteration int, toolCall ai.ToolCall) (ai.ReActToolResult, ai.ToolCallContinuation) {
	execCtx := context.WithValue(ctx, ai.CtxKeyNativeToolHints, true)
	if req.Streamer != nil {
		execCtx = context.WithValue(execCtx, ai.CtxKeyEventStreamer, req.Streamer)
	}

	rawResult, execErr := req.Executor.Execute(execCtx, toolCall.Name, toolCall.Args)
	resultText, hint := unwrapGeminiToolResultEnvelope(rawResult)
	continuationResponse := coerceGeminiToolContinuationResponse(rawResult)
	if execErr != nil {
		emitGeminiOwnedLoopEvent(req, "tool_error", map[string]any{
			"tool":      toolCall.Name,
			"args":      cloneGeminiToolArgs(toolCall.Args),
			"error":     execErr.Error(),
			"iteration": iteration,
		})
		resultText = execErr.Error()
		continuationResponse = map[string]any{
			"tool_error": map[string]any{
				"message": execErr.Error(),
			},
		}
	} else {
		emitGeminiOwnedLoopEvent(req, "tool_result", map[string]any{
			"tool":          toolCall.Name,
			"args":          cloneGeminiToolArgs(toolCall.Args),
			"result":        resultText,
			"progress_hint": cloneGeminiToolProgressHint(hint),
			"result_chars":  len(resultText),
			"iteration":     iteration,
		})
	}

	return ai.ReActToolResult{
			Name:   toolCall.Name,
			Args:   cloneGeminiToolArgs(toolCall.Args),
			Result: resultText,
			Hint:   cloneGeminiToolProgressHint(hint),
		}, ai.ToolCallContinuation{
			ToolCall: toolCall,
			Response: continuationResponse,
		}
}

func emitGeminiOwnedLoopEvent(req ai.ReasoningRequest, eventType string, data any) {
	if req.Streamer != nil {
		req.Streamer(eventType, data)
	}
}

func unwrapGeminiToolResultEnvelope(rawResult string) (string, *ai.ToolProgressHint) {
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

	result := formatGeminiEnvelopeToolResult(envelope.ToolResult)
	if strings.TrimSpace(result) == "" {
		result = rawResult
	}
	return result, cloneGeminiToolProgressHint(envelope.ProgressHint)
}

func formatGeminiEnvelopeToolResult(raw json.RawMessage) string {
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

func cloneGeminiToolProgressHint(hint *ai.ToolProgressHint) *ai.ToolProgressHint {
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

func coerceGeminiToolContinuationResponse(rawResult string) any {
	trimmed := strings.TrimSpace(rawResult)
	if trimmed == "" {
		return map[string]any{"result": ""}
	}

	var envelope ai.ToolResultEnvelope
	if json.Unmarshal([]byte(trimmed), &envelope) == nil && len(envelope.ToolResult) > 0 {
		var decoded any
		if json.Unmarshal(envelope.ToolResult, &decoded) == nil {
			return decoded
		}
	}

	var decoded any
	if json.Unmarshal([]byte(trimmed), &decoded) == nil {
		return decoded
	}

	return map[string]any{"result": rawResult}
}

type geminiRequest struct {
	SystemInstruction *geminiContent          `json:"systemInstruction,omitempty"`
	Contents          []geminiContent         `json:"contents"`
	Tools             []geminiTool            `json:"tools,omitempty"`
	ToolConfig        *geminiToolConfig       `json:"toolConfig,omitempty"`
	GenerationConfig  *geminiGenerationConfig `json:"generationConfig,omitempty"`
}

type geminiToolConfig struct {
	FunctionCallingConfig *geminiFunctionCallingConfig `json:"functionCallingConfig,omitempty"`
}

type geminiFunctionCallingConfig struct {
	Mode string `json:"mode,omitempty"`
}

type geminiGenerationConfig struct {
	Temperature     float32 `json:"temperature,omitempty"`
	TopP            float32 `json:"topP,omitempty"`
	MaxOutputTokens int     `json:"maxOutputTokens,omitempty"`
}

type geminiContent struct {
	Role  string       `json:"role,omitempty"`
	Parts []geminiPart `json:"parts"`
}

type geminiPart struct {
	Text             string                  `json:"text,omitempty"`
	FunctionCall     *geminiFunctionCall     `json:"functionCall,omitempty"`
	FunctionResponse *geminiFunctionResponse `json:"functionResponse,omitempty"`
	ThoughtSignature string                  `json:"thoughtSignature,omitempty"`
}

type geminiTool struct {
	FunctionDeclarations []geminiFunction `json:"functionDeclarations,omitempty"`
}

type geminiFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

type geminiFunctionCall struct {
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
	ID   string         `json:"id,omitempty"`
}

type geminiFunctionResponse struct {
	Name     string `json:"name"`
	Response any    `json:"response,omitempty"`
	ID       string `json:"id,omitempty"`
}

type geminiResponse struct {
	Candidates []struct {
		FinishReason string `json:"finishReason,omitempty"`
		Content      struct {
			Parts []geminiPart `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
	PromptFeedback *struct {
		BlockReason string `json:"blockReason,omitempty"`
	} `json:"promptFeedback,omitempty"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

var geminiAllowedSchemaKeys = map[string]struct{}{
	"description": {},
	"enum":        {},
	"items":       {},
	"properties":  {},
	"required":    {},
	"type":        {},
}

func describeGeminiEmptyResponse(resp geminiResponse) string {
	parts := []string{"no candidates returned from gemini"}
	if resp.PromptFeedback != nil && strings.TrimSpace(resp.PromptFeedback.BlockReason) != "" {
		parts = append(parts, fmt.Sprintf("block_reason=%s", resp.PromptFeedback.BlockReason))
	}
	if len(resp.Candidates) > 0 && strings.TrimSpace(resp.Candidates[0].FinishReason) != "" {
		parts = append(parts, fmt.Sprintf("finish_reason=%s", resp.Candidates[0].FinishReason))
	}
	return strings.Join(parts, "; ")
}

func extractGeminiOutput(resp geminiResponse) (ai.GenOutput, error) {
	if resp.Error != nil {
		return ai.GenOutput{}, fmt.Errorf("gemini api returned error: %s", resp.Error.Message)
	}

	if len(resp.Candidates) == 0 || len(resp.Candidates[0].Content.Parts) == 0 {
		return ai.GenOutput{}, fmt.Errorf("%s", describeGeminiEmptyResponse(resp))
	}

	var out ai.GenOutput
	for _, p := range resp.Candidates[0].Content.Parts {
		if p.FunctionCall != nil {
			toolCall := ai.ToolCall{
				Name:     p.FunctionCall.Name,
				Args:     p.FunctionCall.Args,
				NativeID: strings.TrimSpace(p.FunctionCall.ID),
			}
			if toolCall.NativeID != "" || strings.TrimSpace(p.ThoughtSignature) != "" {
				toolCall.TransportMeta = map[string]any{
					"provider": "gemini",
				}
				if toolCall.NativeID != "" {
					toolCall.TransportMeta["function_call_id"] = toolCall.NativeID
				}
				if signature := strings.TrimSpace(p.ThoughtSignature); signature != "" {
					toolCall.TransportMeta["thought_signature"] = signature
				}
			}
			out.ToolCalls = append(out.ToolCalls, toolCall)
		} else if p.Text != "" {
			out.Text += p.Text
		}
	}

	return out, nil
}

func buildGeminiRequest(prompt string, opts ai.GenOptions) geminiRequest {
	reqBody := geminiRequest{}

	for _, continuation := range opts.ToolCallContinuations {
		if strings.TrimSpace(continuation.ToolCall.Name) == "" {
			continue
		}

		reqBody.Contents = append(reqBody.Contents,
			geminiContent{
				Role: "model",
				Parts: []geminiPart{{FunctionCall: &geminiFunctionCall{
					Name: continuation.ToolCall.Name,
					Args: continuation.ToolCall.Args,
					ID:   strings.TrimSpace(continuation.ToolCall.NativeID),
				}, ThoughtSignature: geminiThoughtSignature(continuation.ToolCall)}},
			},
			geminiContent{
				Role: "user",
				Parts: []geminiPart{{FunctionResponse: &geminiFunctionResponse{
					Name:     continuation.ToolCall.Name,
					Response: continuation.Response,
					ID:       strings.TrimSpace(continuation.ToolCall.NativeID),
				}}},
			},
		)
	}

	reqBody.Contents = append(reqBody.Contents,
		geminiContent{Role: "user", Parts: []geminiPart{{Text: prompt}}},
	)

	if opts.SystemPrompt != "" {
		reqBody.SystemInstruction = &geminiContent{
			Parts: []geminiPart{{Text: opts.SystemPrompt}},
		}
	}

	if len(opts.Tools) > 0 {
		var funcs []geminiFunction
		for _, t := range opts.Tools {
			var params map[string]any
			if t.Schema != "" && strings.HasPrefix(strings.TrimSpace(t.Schema), "{") {
				json.Unmarshal([]byte(t.Schema), &params)
			} else if t.Schema != "" {
				params = map[string]any{"type": "object"}
			}
			params = sanitizeGeminiSchema(params)
			funcs = append(funcs, geminiFunction{
				Name:        t.Name,
				Description: t.Description,
				Parameters:  params,
			})
		}
		reqBody.Tools = []geminiTool{{FunctionDeclarations: funcs}}
		reqBody.ToolConfig = &geminiToolConfig{
			FunctionCallingConfig: &geminiFunctionCallingConfig{Mode: "VALIDATED"},
		}
	}

	if opts.Temperature > 0 || opts.TopP > 0 || opts.MaxTokens > 0 {
		reqBody.GenerationConfig = &geminiGenerationConfig{
			Temperature:     opts.Temperature,
			TopP:            opts.TopP,
			MaxOutputTokens: opts.MaxTokens,
		}
	}

	return reqBody
}

func geminiThoughtSignature(toolCall ai.ToolCall) string {
	if toolCall.TransportMeta == nil {
		return ""
	}
	if signature, ok := toolCall.TransportMeta["thought_signature"].(string); ok {
		return strings.TrimSpace(signature)
	}
	return ""
}

func sanitizeGeminiSchema(schema map[string]any) map[string]any {
	sanitized, ok := sanitizeGeminiSchemaNode(schema)
	if !ok {
		return map[string]any{"type": "object"}
	}
	if _, ok := sanitized["type"].(string); !ok || strings.TrimSpace(sanitized["type"].(string)) == "" {
		sanitized["type"] = inferGeminiSchemaType(sanitized, "object")
	}
	return sanitized
}

func sanitizeGeminiSchemaNode(schema map[string]any) (map[string]any, bool) {
	if len(schema) == 0 {
		return nil, false
	}

	sanitized := make(map[string]any)
	for key := range geminiAllowedSchemaKeys {
		value, ok := schema[key]
		if !ok {
			continue
		}
		switch key {
		case "type":
			typeName, ok := value.(string)
			if ok && strings.TrimSpace(typeName) != "" {
				sanitized[key] = strings.ToLower(strings.TrimSpace(typeName))
			}
		case "description":
			description, ok := value.(string)
			if ok && strings.TrimSpace(description) != "" {
				sanitized[key] = description
			}
		case "enum":
			if enumValues := sanitizeGeminiEnum(value); len(enumValues) > 0 {
				sanitized[key] = enumValues
			}
		case "required":
			if required := sanitizeGeminiRequired(value); len(required) > 0 {
				sanitized[key] = required
			}
		case "properties":
			if props := sanitizeGeminiProperties(value); len(props) > 0 {
				sanitized[key] = props
			}
		case "items":
			if itemSchema, ok := value.(map[string]any); ok {
				child, childOK := sanitizeGeminiSchemaNode(itemSchema)
				if childOK {
					if _, hasType := child["type"]; !hasType {
						child["type"] = inferGeminiSchemaType(child, "string")
					}
					sanitized[key] = child
				}
			}
		}
	}

	if len(sanitized) == 0 {
		return nil, false
	}
	return sanitized, true
}

func sanitizeGeminiProperties(value any) map[string]any {
	propMap, ok := value.(map[string]any)
	if !ok || len(propMap) == 0 {
		return nil
	}
	props := make(map[string]any)
	for name, rawChild := range propMap {
		childMap, ok := rawChild.(map[string]any)
		if !ok {
			continue
		}
		child, childOK := sanitizeGeminiSchemaNode(childMap)
		if !childOK {
			continue
		}
		if _, hasType := child["type"]; !hasType {
			child["type"] = inferGeminiSchemaType(child, "string")
		}
		props[name] = child
	}
	if len(props) == 0 {
		return nil
	}
	return props
}

func sanitizeGeminiRequired(value any) []string {
	switch required := value.(type) {
	case []string:
		out := make([]string, 0, len(required))
		for _, name := range required {
			if strings.TrimSpace(name) != "" {
				out = append(out, name)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(required))
		for _, item := range required {
			name, ok := item.(string)
			if ok && strings.TrimSpace(name) != "" {
				out = append(out, name)
			}
		}
		return out
	default:
		return nil
	}
}

func sanitizeGeminiEnum(value any) []any {
	enumValues, ok := value.([]any)
	if ok && len(enumValues) > 0 {
		return enumValues
	}
	if stringValues, ok := value.([]string); ok && len(stringValues) > 0 {
		out := make([]any, 0, len(stringValues))
		for _, item := range stringValues {
			out = append(out, item)
		}
		return out
	}
	return nil
}

func inferGeminiSchemaType(schema map[string]any, fallback string) string {
	if _, ok := schema["properties"]; ok {
		return "object"
	}
	if _, ok := schema["items"]; ok {
		return "array"
	}
	return fallback
}

// Generate sends a prompt to the Gemini API and returns the generated text.
func (g *gemini) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	if g.apiKey == "" || g.apiKey == "YOUR_API_KEY" {
		return ai.GenOutput{
			Text: fmt.Sprintf("[Gemini Stub] Missing API Key. Please set LLM_API_KEY or GEMINI_API_KEY environment variable. Would send: %q", prompt),
		}, nil
	}

	apiURL := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent", g.model)

	reqBody := buildGeminiRequest(prompt, opts)

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", g.apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("gemini api request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return ai.GenOutput{}, fmt.Errorf("gemini api error (status %d): %s", resp.StatusCode, string(body))
	}

	var geminiResp geminiResponse
	if err := json.Unmarshal(body, &geminiResp); err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	out, err := extractGeminiOutput(geminiResp)
	if err != nil {
		return ai.GenOutput{}, err
	}

	// Default rough estimate
	out.TokensUsed = len(prompt) / 4
	return out, nil
}

// EstimateCost estimates the cost of the generation based on token usage.
func (g *gemini) EstimateCost(inTokens, outTokens int) float64 {
	// Placeholder pricing
	return float64(inTokens)*0.0001 + float64(outTokens)*0.0002
}
