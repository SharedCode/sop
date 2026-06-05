package generator

// chatgpt_react_loop.go — ChatGPT-owned ReAct (Reason + Act) loop.
//
// Responsibilities:
//   - Owning the multi-turn tool-call loop against the Responses API.
//   - Building the initial and continuation Responses API requests.
//   - Executing tool calls and collecting results.
//   - Assembling the final ReasoningResponse (text, tool calls, carryover state).
//   - Parsing textual Function_call payloads as a fallback for proxy/stub APIs.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// ----------------------------------------------------------------------------
// Loop struct and constructor
// ----------------------------------------------------------------------------

type chatGPTOwnedReActLoop struct {
	generator     *chatgpt
	run           func(context.Context, ai.ReasoningRequest) (ai.ReasoningResponse, error)
	create        func(context.Context, openAIResponsesRequest) (openAIResponsesResponse, error)
	createStream  func(context.Context, openAIResponsesRequest, func(string, any)) (openAIResponsesResponse, error)
	maxIterations int
}

func newChatGPTOwnedReActLoop(g *chatgpt) ai.ReActLoop {
	return chatGPTOwnedReActLoop{generator: g, maxIterations: 3}
}

func (l chatGPTOwnedReActLoop) modelName() string {
	if l.generator == nil {
		return ""
	}
	return l.generator.model
}

// chatGPTReasoningEffort returns the appropriate reasoning effort level for the model.
// GPT-x.x-pro models only support 'medium', 'high', and 'xhigh' (not 'low').
func chatGPTReasoningEffort(model string) string {
	modelLower := strings.ToLower(strings.TrimSpace(model))
	if strings.Contains(modelLower, "-pro") {
		return "medium"
	}
	return "low"
}

// ----------------------------------------------------------------------------
// Main loop
// ----------------------------------------------------------------------------

func (l chatGPTOwnedReActLoop) Run(ctx context.Context, req ai.ReasoningRequest) (ai.ReasoningResponse, error) {
	if l.run != nil {
		return l.run(ctx, req)
	}

	tools, err := listToolsForLoop(ctx, req)
	if err != nil {
		return ai.ReasoningResponse{}, err
	}

	request, err := buildChatGPTResponsesRequest(req, l.modelName(), tools)
	if err != nil {
		return ai.ReasoningResponse{}, fmt.Errorf("failed to build chatgpt owned loop scaffold request: %w", err)
	}

	maxIterations := l.maxIterations
	if maxIterations <= 0 {
		maxIterations = 3
	}

	executedToolCalls := make([]ai.ToolCall, 0)
	toolResults := make([]ai.ReActToolResult, 0)

	for iteration := 1; iteration <= maxIterations; iteration++ {
		if err := ctx.Err(); err != nil {
			return ai.ReasoningResponse{}, err
		}
		response, err := l.sendRequest(ctx, request, req.Streamer)
		if err != nil {
			return ai.ReasoningResponse{}, err
		}
		if err := ctx.Err(); err != nil {
			return ai.ReasoningResponse{}, err
		}
		emitChatGPTOwnedLoopAssistantMessages(req, response)

		toolCalls, err := openAIResponseToolCalls(response)
		if err != nil {
			return ai.ReasoningResponse{}, err
		}

		// No tool calls — final answer reached.
		if req.Executor == nil || len(toolCalls) == 0 {
			resp := buildFinalResponse(l.modelName(), openAIResponseFinalText(response), executedToolCalls, toolResults, response.ID, conversationIDFromResponse(response))
			emitChatGPTOwnedLoopHydration(req, resp)
			return resp, nil
		}

		// Execute tool calls and build continuation input for the next turn.
		toolOutputItems, newExecuted, newResults := executeToolCalls(ctx, req, iteration, toolCalls)
		if err := ctx.Err(); err != nil {
			return ai.ReasoningResponse{}, err
		}
		executedToolCalls = append(executedToolCalls, newExecuted...)
		toolResults = append(toolResults, newResults...)

		partial := buildFinalResponse(l.modelName(), "", executedToolCalls, toolResults, response.ID, conversationIDFromResponse(response))
		emitChatGPTOwnedLoopHydration(req, partial)

		request = buildContinuationRequest(l.modelName(), req, request, response, toolOutputItems)
	}
	return ai.ReasoningResponse{}, fmt.Errorf("chatgpt owned loop exceeded %d iterations for model %q", maxIterations, l.modelName())
}

// sendRequest dispatches to streaming or blocking depending on the request flag.
func (l chatGPTOwnedReActLoop) sendRequest(ctx context.Context, request openAIResponsesRequest, streamer func(string, any)) (openAIResponsesResponse, error) {
	if request.Stream != nil && *request.Stream {
		if l.createStream != nil {
			return l.createStream(ctx, request, streamer)
		}
		if l.generator == nil {
			return openAIResponsesResponse{}, fmt.Errorf("chatgpt owned loop requires a generator")
		}
		return l.generator.createResponsesStream(ctx, request, streamer)
	}
	if l.create != nil {
		return l.create(ctx, request)
	}
	if l.generator == nil {
		return openAIResponsesResponse{}, fmt.Errorf("chatgpt owned loop requires a generator")
	}
	return l.generator.createResponses(ctx, request)
}

// ----------------------------------------------------------------------------
// Request building
// ----------------------------------------------------------------------------

func listToolsForLoop(ctx context.Context, req ai.ReasoningRequest) ([]ai.ToolDefinition, error) {
	if req.Executor == nil {
		return nil, nil
	}
	tools, err := req.Executor.ListTools(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list tools for chatgpt owned loop scaffold: %w", err)
	}
	return tools, nil
}

func buildChatGPTResponsesRequest(req ai.ReasoningRequest, model string, tools []ai.ToolDefinition) (openAIResponsesRequest, error) {
	responsesTools, err := buildChatGPTResponsesTools(tools)
	if err != nil {
		return openAIResponsesRequest{}, err
	}
	parallelToolCalls := true
	store := true

	request := openAIResponsesRequest{
		Model:        model,
		Instructions: chatGPTSystemInstructions(req.SystemPrompt, len(responsesTools) > 0),
		Input: []openAIResponsesInputItem{{
			Role:    "user",
			Content: chatGPTUserPrompt(req),
		}},
		Tools:             responsesTools,
		Include:           []string{"reasoning.encrypted_content"},
		Reasoning:         &openAIResponsesReasoning{Effort: chatGPTReasoningEffort(model)},
		ParallelToolCalls: &parallelToolCalls,
		Store:             &store,
	}
	if chatGPTResponsesSupportsTemperature(model) {
		temperature := float32(0.0)
		request.Temperature = &temperature
	}

	if req.Streamer != nil {
		stream := true
		request.Stream = &stream
	}
	if state := req.CarryoverState; state != nil {
		if conversationID := strings.TrimSpace(state.ConversationID); conversationID != "" {
			request.Conversation = conversationID
		} else if handle := strings.TrimSpace(state.ConversationHandle); handle != "" {
			request.PreviousResponseID = handle
		}
	}
	if req.ForceToolCall && len(responsesTools) > 0 {
		request.ToolChoice = buildToolChoiceForceCall(responsesTools)
	}
	return request, nil
}

func chatGPTResponsesSupportsTemperature(model string) bool {
	trimmedModel := strings.ToLower(strings.TrimSpace(model))
	if trimmedModel == "" {
		return true
	}
	return !strings.HasPrefix(trimmedModel, "gpt-5")
}

// buildToolChoiceForceCall pins tool_choice to execute_script when present,
// otherwise falls back to "required" (any tool).
func buildToolChoiceForceCall(tools []openAIResponsesTool) any {
	for _, t := range tools {
		if t.Name == "execute_script" {
			return map[string]any{"type": "function", "name": t.Name}
		}
	}
	return "required"
}

// buildContinuationRequest constructs the follow-up request after tool execution.
func buildContinuationRequest(model string, req ai.ReasoningRequest, prev openAIResponsesRequest, response openAIResponsesResponse, toolOutputs []openAIResponsesInputItem) openAIResponsesRequest {
	return openAIResponsesRequest{
		Model:              model,
		Instructions:       chatGPTSystemInstructions(req.SystemPrompt, len(prev.Tools) > 0),
		Input:              buildChatGPTContinuationInput(response, toolOutputs),
		Tools:              prev.Tools,
		PreviousResponseID: response.ID,
		Include:            []string{"reasoning.encrypted_content"},
		Reasoning:          &openAIResponsesReasoning{Effort: chatGPTReasoningEffort(model)},
		ParallelToolCalls:  prev.ParallelToolCalls,
		Store:              prev.Store,
		MaxOutputTokens:    prev.MaxOutputTokens,
		Stream:             prev.Stream,
	}
}

// chatGPTSystemInstructions appends the tool-call guardrail to the system prompt
// when tools are available, so the model emits native function_calls.
func chatGPTSystemInstructions(systemPrompt string, hasTools bool) string {
	base := strings.TrimSpace(systemPrompt)
	if !hasTools {
		return base
	}
	guardrail := "If a tool is needed, emit a native function_call instead of assistant text. Do not print tool names, tool arguments, script parameters, JSON ASTs, CSV step tables, or markdown code blocks as a substitute for a tool call. For execute_script specifically, place the script only inside the function_call arguments and never display the script body in assistant text."
	if base == "" {
		return guardrail
	}
	return base + "\n\n" + guardrail
}

// chatGPTUserPrompt assembles the user turn content from the request fields.
func chatGPTUserPrompt(req ai.ReasoningRequest) string {
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

func buildChatGPTResponsesTools(tools []ai.ToolDefinition) ([]openAIResponsesTool, error) {
	if len(tools) == 0 {
		return nil, nil
	}
	built := make([]openAIResponsesTool, 0, len(tools))
	for _, tool := range tools {
		parameters := map[string]any(nil)
		if strings.TrimSpace(tool.Schema) != "" {
			if err := json.Unmarshal([]byte(tool.Schema), &parameters); err != nil {
				return nil, fmt.Errorf("invalid OpenAI tool schema for %q: %w", tool.Name, err)
			}
		}
		built = append(built, openAIResponsesTool{
			Type:        "function",
			Name:        tool.Name,
			Description: tool.Description,
			Parameters:  parameters,
			Strict:      false,
		})
	}
	return built, nil
}

// ----------------------------------------------------------------------------
// Tool execution
// ----------------------------------------------------------------------------

// executeToolCalls runs each tool call, emits events, and returns:
//   - toolOutputItems: input items for the next Responses API request
//   - executed: tool calls that were run
//   - results: tool result records for carryover / outcome tracking
func executeToolCalls(ctx context.Context, req ai.ReasoningRequest, iteration int, toolCalls []ai.ToolCall) ([]openAIResponsesInputItem, []ai.ToolCall, []ai.ReActToolResult) {
	outputItems := make([]openAIResponsesInputItem, 0, len(toolCalls))
	executed := make([]ai.ToolCall, 0, len(toolCalls))
	results := make([]ai.ReActToolResult, 0, len(toolCalls))

	for _, toolCall := range toolCalls {
		if err := ctx.Err(); err != nil {
			break
		}
		toolCall = normalizeChatGPTWrappedToolCall(toolCall)
		emitChatGPTOwnedLoopEvent(req, ai.ReasoningEventToolCall, ai.BuildToolCallEvent(toolCall.Name, cloneChatGPTToolArgs(toolCall.Args), iteration))
		executed = append(executed, toolCall)
		result, outputItem := executeSingleToolCall(ctx, req, iteration, toolCall)
		results = append(results, result)
		outputItems = append(outputItems, outputItem)
	}
	return outputItems, executed, results
}

func normalizeChatGPTWrappedToolCall(toolCall ai.ToolCall) ai.ToolCall {
	if !strings.EqualFold(strings.TrimSpace(toolCall.Name), "call_function") {
		return toolCall
	}
	if len(toolCall.Args) == 0 {
		return toolCall
	}

	target := ""
	if v, _ := toolCall.Args["function"].(string); strings.TrimSpace(v) != "" {
		target = strings.TrimSpace(v)
	} else if v, _ := toolCall.Args["tool"].(string); strings.TrimSpace(v) != "" {
		target = strings.TrimSpace(v)
	} else if v, _ := toolCall.Args["name"].(string); strings.TrimSpace(v) != "" {
		target = strings.TrimSpace(v)
	}
	if target == "" {
		return toolCall
	}

	normalizedArgs := map[string]any{}
	if nested, ok := toolCall.Args["args"].(map[string]any); ok && len(nested) > 0 {
		normalizedArgs = cloneChatGPTToolArgs(nested)
	} else {
		for k, v := range toolCall.Args {
			switch strings.ToLower(strings.TrimSpace(k)) {
			case "type", "command", "function", "tool", "name", "result_var":
				continue
			}
			normalizedArgs[k] = v
		}
	}

	return ai.ToolCall{
		Name:          target,
		Args:          normalizedArgs,
		NativeID:      toolCall.NativeID,
		TransportMeta: toolCall.TransportMeta,
	}
}

func executeSingleToolCall(ctx context.Context, req ai.ReasoningRequest, iteration int, toolCall ai.ToolCall) (ai.ReActToolResult, openAIResponsesInputItem) {
	execCtx := context.WithValue(ctx, ai.CtxKeyNativeToolHints, true)
	if req.Streamer != nil {
		execCtx = context.WithValue(execCtx, ai.CtxKeyEventStreamer, req.Streamer)
	}

	rawResult, execErr := req.Executor.Execute(execCtx, toolCall.Name, toolCall.Args)
	resultText, hint := unwrapChatGPTToolResultEnvelope(rawResult)

	if execErr != nil {
		emitChatGPTOwnedLoopEvent(req, ai.ReasoningEventToolError, ai.BuildToolErrorEvent(toolCall.Name, cloneChatGPTToolArgs(toolCall.Args), execErr, iteration))
		resultText = execErr.Error()
	} else {
		emitChatGPTOwnedLoopEvent(req, ai.ReasoningEventToolResult, ai.BuildToolResultEvent(toolCall.Name, cloneChatGPTToolArgs(toolCall.Args), resultText, cloneChatGPTToolProgressHint(hint), iteration))
	}

	return ai.ReActToolResult{
			Name:   toolCall.Name,
			Args:   cloneChatGPTToolArgs(toolCall.Args),
			Result: resultText,
			Hint:   cloneChatGPTToolProgressHint(hint),
		}, openAIResponsesInputItem{
			Type:   "function_call_output",
			CallID: strings.TrimSpace(toolCall.NativeID),
			Output: summarizeChatGPTContinuationToolOutput(toolCall.Name, resultText),
		}
}

func summarizeChatGPTContinuationToolOutput(toolName, resultText string) string {
	trimmedTool := strings.TrimSpace(toolName)
	trimmedResult := strings.TrimSpace(resultText)
	if !strings.EqualFold(trimmedTool, "execute_script") {
		return resultText
	}
	if trimmedResult == "" {
		return "execute_script completed with no textual payload. Results, if any, were already streamed to the client."
	}

	var rows []json.RawMessage
	if err := json.Unmarshal([]byte(trimmedResult), &rows); err == nil {
		return fmt.Sprintf("execute_script completed successfully and returned %d row(s). The full row payload was already streamed to the client. Do not restate the rows; provide at most a brief summary.", len(rows))
	}

	var record map[string]any
	if err := json.Unmarshal([]byte(trimmedResult), &record); err == nil {
		return "execute_script completed successfully and returned one structured record. The full payload was already streamed to the client. Do not restate the record; provide at most a brief summary."
	}

	if len(trimmedResult) > 1000 {
		return fmt.Sprintf("execute_script completed successfully and returned a large textual payload (%d chars). The full payload was already streamed to the client. Do not restate it; provide at most a brief summary.", len(trimmedResult))
	}

	return resultText
}

// ----------------------------------------------------------------------------
// Response assembly
// ----------------------------------------------------------------------------

func buildFinalResponse(model string, finalText string, toolCalls []ai.ToolCall, toolResults []ai.ReActToolResult, responseID string, conversationID string) ai.ReasoningResponse {
	resp := ai.ReasoningResponse{
		FinalText:      finalText,
		ToolCalls:      toolCalls,
		OutcomeFacts:   append([]string(nil), ai.SummarizeOutcomeFacts(toolResults)...),
		OutcomeRecipes: append([]ai.LearnedRecipe(nil), ai.SummarizeOutcomeRecipes(toolResults)...),
	}
	if carryState := buildCarryoverState(responseID, conversationID, toolCalls); carryState != nil {
		carryState.Provider = "chatgpt"
		carryState.Model = model
		carryState.LastAssistantSummary = strings.TrimSpace(finalText)
		carryState.LastOutcomeFacts = append([]string(nil), resp.OutcomeFacts...)
		carryState.LastToolNames = toolCallNames(toolCalls)
		carryState.LastRecipeIDs = recipeIDs(resp.OutcomeRecipes)
		resp.CarryoverState = carryState
	}
	return resp
}

func buildCarryoverState(responseID string, conversationID string, toolCalls []ai.ToolCall) *ai.CarryoverState {
	handle := strings.TrimSpace(responseID)
	serverHandle := strings.TrimSpace(conversationID)
	if len(toolCalls) == 0 && handle == "" && serverHandle == "" {
		return nil
	}
	mode := ai.CarryoverModeCompact
	if handle != "" || serverHandle != "" {
		mode = ai.CarryoverModeLive
	}
	state := &ai.CarryoverState{Mode: mode}
	if handle != "" {
		state.ConversationHandle = handle
	}
	if serverHandle != "" {
		state.ConversationID = serverHandle
	}
	if len(toolCalls) == 0 {
		return state
	}
	raw, err := json.Marshal(toolCalls)
	if err != nil {
		return state
	}
	state.EstimatedRawToolTokens = (len(raw) + 3) / 4
	return state
}

func conversationIDFromResponse(response openAIResponsesResponse) string {
	if response.Conversation == nil {
		return ""
	}
	return strings.TrimSpace(response.Conversation.ID)
}

func toolCallNames(toolCalls []ai.ToolCall) []string {
	if len(toolCalls) == 0 {
		return nil
	}
	names := make([]string, 0, len(toolCalls))
	for _, tc := range toolCalls {
		if name := strings.TrimSpace(tc.Name); name != "" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	return names
}

func recipeIDs(recipes []ai.LearnedRecipe) []string {
	if len(recipes) == 0 {
		return nil
	}
	ids := make([]string, 0, len(recipes))
	for _, r := range recipes {
		if id := strings.TrimSpace(r.ID); id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

// ----------------------------------------------------------------------------
// Event emission helpers
// ----------------------------------------------------------------------------

func emitChatGPTOwnedLoopEvent(req ai.ReasoningRequest, eventType string, data any) {
	if req.Streamer == nil || !req.Verbose {
		return
	}
	req.Streamer(eventType, data)
}

func emitChatGPTOwnedLoopAssistantMessages(req ai.ReasoningRequest, response openAIResponsesResponse) {
	for _, message := range openAIResponseAssistantMessages(response) {
		phase := message.Phase
		if phase == "" {
			phase = "final_answer"
		}
		emitChatGPTOwnedLoopEvent(req, ai.ReasoningEventAssistantMessage, ai.BuildAssistantMessageEvent(phase, message.Text))
	}
}

func emitChatGPTOwnedLoopHydration(req ai.ReasoningRequest, resp ai.ReasoningResponse) {
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

// ----------------------------------------------------------------------------
// Tool result unwrapping and argument helpers
// ----------------------------------------------------------------------------

func unwrapChatGPTToolResultEnvelope(rawResult string) (string, *ai.ToolProgressHint) {
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
	result := formatChatGPTEnvelopeToolResult(envelope.ToolResult)
	if strings.TrimSpace(result) == "" {
		result = rawResult
	}
	return result, cloneChatGPTToolProgressHint(envelope.ProgressHint)
}

func formatChatGPTEnvelopeToolResult(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var value any
	if err := json.Unmarshal(raw, &value); err == nil {
		if b, marshalErr := json.Marshal(value); marshalErr == nil {
			return string(b)
		}
	}
	return string(raw)
}

func cloneChatGPTToolArgs(args map[string]any) map[string]any {
	if len(args) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(args))
	for k, v := range args {
		cloned[k] = v
	}
	return cloned
}

func cloneChatGPTToolProgressHint(hint *ai.ToolProgressHint) *ai.ToolProgressHint {
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

// ----------------------------------------------------------------------------
// Textual Function_call fallback parser
//
// Some proxy/stub APIs return a plain assistant text like:
//   Function_call: {"name": "execute_script", "arguments": {...}}
// instead of a native function_call output item. These helpers parse that format.
// ----------------------------------------------------------------------------

func parseChatGPTTextualFunctionCall(text string) (ai.ToolCall, bool, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ai.ToolCall{}, false, nil
	}
	if !strings.HasPrefix(strings.ToLower(trimmed), "function_call") {
		return ai.ToolCall{}, false, nil
	}
	payload := strings.TrimSpace(trimmed[len("Function_call"):])
	payload = strings.TrimLeft(payload, ":\n\r\t ")
	payload = strings.TrimSpace(strings.Trim(payload, "`"))
	if payload == "" {
		return ai.ToolCall{}, true, fmt.Errorf("textual function_call is missing payload")
	}
	for _, candidate := range chatGPTTextualFunctionCallCandidates(payload) {
		if toolCall, err := decodeChatGPTTextualFunctionCallCandidate(candidate); err == nil {
			return toolCall, true, nil
		}
	}
	return ai.ToolCall{}, true, fmt.Errorf("textual function_call payload is invalid: %s", payload)
}

func chatGPTTextualFunctionCallCandidates(payload string) []string {
	candidates := make([]string, 0, 5)
	seen := make(map[string]bool)
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			return
		}
		seen[value] = true
		candidates = append(candidates, value)
	}
	add(payload)
	if unquoted, err := strconv.Unquote(payload); err == nil {
		add(unquoted)
	}
	if strings.Contains(payload, `""`) {
		deduped := strings.ReplaceAll(payload, `""`, `"`)
		add(deduped)
		if unquoted, err := strconv.Unquote(deduped); err == nil {
			add(unquoted)
		}
	}
	if strings.HasPrefix(payload, `"`) && strings.HasSuffix(payload, `"`) {
		add(strings.Trim(payload, `"`))
	}
	return candidates
}

func decodeChatGPTTextualFunctionCallCandidate(candidate string) (ai.ToolCall, error) {
	trimmed := strings.TrimSpace(strings.Trim(strings.TrimSpace(candidate), "`"))
	if strings.Contains(trimmed, `""`) {
		trimmed = strings.ReplaceAll(trimmed, `""`, `"`)
	}
	if strings.HasPrefix(trimmed, `"`) && strings.HasSuffix(trimmed, `"`) {
		trimmed = strings.TrimPrefix(strings.TrimSuffix(trimmed, `"`), `"`)
	}
	var raw struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
		Args      map[string]any `json:"args"`
	}
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return ai.ToolCall{}, err
	}
	args := raw.Arguments
	if len(args) == 0 && len(raw.Args) > 0 {
		args = raw.Args
	}
	name := strings.TrimSpace(raw.Name)
	if name == "" {
		return ai.ToolCall{}, fmt.Errorf("missing tool name")
	}
	return ai.ToolCall{Name: name, Args: args}, nil
}
