package generator

import (
	"bufio"
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

// chatgpt implements the Generator interface for OpenAI's ChatGPT models.
type chatgpt struct {
	apiKey       string
	model        string
	ownedLoop    ai.ReActLoop
	newOwnedLoop func(*chatgpt) ai.ReActLoop
	supportsLive bool
}

func init() {
	Register("chatgpt", func(cfg map[string]any) (ai.Generator, error) {
		apiKey, _ := cfg["api_key"].(string)
		if apiKey == "" {
			apiKey = os.Getenv("LLM_API_KEY")
			if apiKey == "" {
				apiKey = os.Getenv("OPENAI_API_KEY")
			}
		}
		model, _ := cfg["model"].(string)
		if model == "" {
			model = os.Getenv("OPENAI_MODEL")
		}
		if model == "" {
			model = "gpt-4o"
		}
		gen := &chatgpt{apiKey: apiKey, model: model}
		if chatGPTOwnedLoopScaffoldEnabled(cfg) {
			gen.newOwnedLoop = newChatGPTOwnedReActLoop
			gen.supportsLive = true
		}
		return gen, nil
	})
}

func chatGPTOwnedLoopScaffoldEnabled(cfg map[string]any) bool {
	if enabled, ok := cfg["enable_owned_loop_scaffold"].(bool); ok {
		return enabled
	}
	if raw, ok := cfg["enable_owned_loop_scaffold"].(string); ok {
		return parseChatGPTBoolOverride(raw, true)
	}
	if raw := os.Getenv("OPENAI_ENABLE_OWNED_LOOP_SCAFFOLD"); strings.TrimSpace(raw) != "" {
		return parseChatGPTBoolOverride(raw, true)
	}
	return true
}

func parseChatGPTBoolOverride(raw string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

// Name returns the name of the generator.
func (g *chatgpt) Name() string { return "chatgpt" }

func (g *chatgpt) CarryoverCapability() ai.CarryoverCapability {
	return ai.CarryoverCapability{
		Provider:        g.Name(),
		Model:           g.model,
		SupportsCompact: true,
		SupportsLive:    g.supportsLive,
	}
}

func (g *chatgpt) ReActLoop() ai.ReActLoop {
	if g.ownedLoop != nil {
		return g.ownedLoop
	}
	if g.newOwnedLoop != nil {
		return g.newOwnedLoop(g)
	}
	return g.ownedLoop
}

type chatGPTOwnedReActLoop struct {
	generator     *chatgpt
	run           func(context.Context, ai.ReasoningRequest) (ai.ReasoningResponse, error)
	create        func(context.Context, openAIResponsesRequest) (openAIResponsesResponse, error)
	createStream  func(context.Context, openAIResponsesRequest, func(string, any)) (openAIResponsesResponse, error)
	maxIterations int
}

type openAIResponsesRequest struct {
	Model              string                     `json:"model"`
	Instructions       string                     `json:"instructions,omitempty"`
	Input              []openAIResponsesInputItem `json:"input,omitempty"`
	Tools              []openAIResponsesTool      `json:"tools,omitempty"`
	PreviousResponseID string                     `json:"previous_response_id,omitempty"`
	Include            []string                   `json:"include,omitempty"`
	Reasoning          *openAIResponsesReasoning  `json:"reasoning,omitempty"`
	ParallelToolCalls  *bool                      `json:"parallel_tool_calls,omitempty"`
	Store              *bool                      `json:"store,omitempty"`
	Temperature        *float32                   `json:"temperature,omitempty"`
	MaxOutputTokens    int                        `json:"max_output_tokens,omitempty"`
	Stream             *bool                      `json:"stream,omitempty"`
}

type openAIResponsesInputItem struct {
	ID               string `json:"id,omitempty"`
	Role             string `json:"role,omitempty"`
	Content          string `json:"content,omitempty"`
	Type             string `json:"type,omitempty"`
	Name             string `json:"name,omitempty"`
	Arguments        string `json:"arguments,omitempty"`
	CallID           string `json:"call_id,omitempty"`
	Output           string `json:"output,omitempty"`
	Phase            string `json:"phase,omitempty"`
	EncryptedContent string `json:"encrypted_content,omitempty"`
}

type openAIResponsesTool struct {
	Type        string         `json:"type"`
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
	Strict      bool           `json:"strict,omitempty"`
}

type openAIResponsesReasoning struct {
	Effort string `json:"effort,omitempty"`
}

type openAIResponsesResponse struct {
	ID         string                      `json:"id"`
	Status     string                      `json:"status,omitempty"`
	Output     []openAIResponsesOutputItem `json:"output,omitempty"`
	OutputText string                      `json:"output_text,omitempty"`
	Usage      *openAIResponsesUsage       `json:"usage,omitempty"`
	Error      *openAIResponsesError       `json:"error,omitempty"`
}

type openAIResponsesOutputItem struct {
	ID               string                       `json:"id,omitempty"`
	Type             string                       `json:"type,omitempty"`
	Status           string                       `json:"status,omitempty"`
	Role             string                       `json:"role,omitempty"`
	Phase            string                       `json:"phase,omitempty"`
	CallID           string                       `json:"call_id,omitempty"`
	Name             string                       `json:"name,omitempty"`
	Arguments        string                       `json:"arguments,omitempty"`
	EncryptedContent string                       `json:"encrypted_content,omitempty"`
	Content          []openAIResponsesContentItem `json:"content,omitempty"`
	Summary          []openAIResponsesSummaryItem `json:"summary,omitempty"`
}

type openAIResponsesContentItem struct {
	Type string `json:"type,omitempty"`
	Text string `json:"text,omitempty"`
}

type openAIResponsesSummaryItem struct {
	Type string `json:"type,omitempty"`
	Text string `json:"text,omitempty"`
}

type openAIResponsesUsage struct {
	TotalTokens int `json:"total_tokens,omitempty"`
}

type openAIResponsesError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type openAIResponsesStreamEvent struct {
	Type         string                     `json:"type"`
	Response     *openAIResponsesResponse   `json:"response,omitempty"`
	Item         *openAIResponsesOutputItem `json:"item,omitempty"`
	OutputIndex  int                        `json:"output_index,omitempty"`
	ItemID       string                     `json:"item_id,omitempty"`
	ContentIndex int                        `json:"content_index,omitempty"`
	SummaryIndex int                        `json:"summary_index,omitempty"`
	Sequence     int                        `json:"sequence_number,omitempty"`
	Delta        string                     `json:"delta,omitempty"`
	Text         string                     `json:"text,omitempty"`
	Name         string                     `json:"name,omitempty"`
	Arguments    string                     `json:"arguments,omitempty"`
	Message      string                     `json:"message,omitempty"`
}

func newChatGPTOwnedReActLoop(g *chatgpt) ai.ReActLoop {
	return chatGPTOwnedReActLoop{generator: g, maxIterations: 3}
}

func (l chatGPTOwnedReActLoop) Run(ctx context.Context, req ai.ReasoningRequest) (ai.ReasoningResponse, error) {
	if l.run != nil {
		return l.run(ctx, req)
	}
	var tools []ai.ToolDefinition
	if req.Executor != nil {
		listedTools, err := req.Executor.ListTools(ctx)
		if err != nil {
			return ai.ReasoningResponse{}, fmt.Errorf("failed to list tools for chatgpt owned loop scaffold: %w", err)
		}
		tools = listedTools
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
		response, err := l.createResponse(ctx, request, req.Streamer)
		if err != nil {
			return ai.ReasoningResponse{}, err
		}
		emitChatGPTOwnedLoopAssistantMessages(req, response)
		toolCalls, err := openAIResponseToolCalls(response)
		if err != nil {
			return ai.ReasoningResponse{}, err
		}
		if req.Executor == nil || len(toolCalls) == 0 {
			resp := chatGPTOwnedLoopResponse(l.modelName(), openAIResponseFinalText(response), executedToolCalls, ai.SummarizeOutcomeFacts(toolResults), ai.SummarizeOutcomeRecipes(toolResults), response.ID)
			emitChatGPTOwnedLoopHydration(req, ai.MemoryHydrationUpdate{
				FinalText:      resp.FinalText,
				ToolCalls:      resp.ToolCalls,
				OutcomeFacts:   resp.OutcomeFacts,
				OutcomeRecipes: resp.OutcomeRecipes,
				CarryoverState: resp.CarryoverState,
			})
			return resp, nil
		}

		continuationItems := make([]openAIResponsesInputItem, 0, len(toolCalls))
		for _, toolCall := range toolCalls {
			emitChatGPTOwnedLoopEvent(req, ai.ReasoningEventToolCall, ai.BuildToolCallEvent(toolCall.Name, cloneChatGPTToolArgs(toolCall.Args), iteration))
			executedToolCalls = append(executedToolCalls, toolCall)
			toolResult, continuationItem := executeChatGPTOwnedLoopToolCall(ctx, req, iteration, toolCall)
			toolResults = append(toolResults, toolResult)
			continuationItems = append(continuationItems, continuationItem)
		}
		partial := chatGPTOwnedLoopResponse(l.modelName(), "", executedToolCalls, ai.SummarizeOutcomeFacts(toolResults), ai.SummarizeOutcomeRecipes(toolResults), response.ID)
		emitChatGPTOwnedLoopHydration(req, ai.MemoryHydrationUpdate{
			FinalText:      partial.FinalText,
			ToolCalls:      partial.ToolCalls,
			OutcomeFacts:   partial.OutcomeFacts,
			OutcomeRecipes: partial.OutcomeRecipes,
			CarryoverState: partial.CarryoverState,
		})
		request = openAIResponsesRequest{
			Model:              l.modelName(),
			Instructions:       strings.TrimSpace(req.SystemPrompt),
			Input:              buildChatGPTContinuationInput(response, continuationItems),
			Tools:              request.Tools,
			PreviousResponseID: response.ID,
			Include:            []string{"reasoning.encrypted_content"},
			Reasoning:          &openAIResponsesReasoning{Effort: "low"},
			ParallelToolCalls:  request.ParallelToolCalls,
			Store:              request.Store,
			Temperature:        request.Temperature,
			MaxOutputTokens:    request.MaxOutputTokens,
			Stream:             request.Stream,
		}
	}
	return ai.ReasoningResponse{}, fmt.Errorf("chatgpt owned loop exceeded %d iterations for model %q", maxIterations, l.modelName())
}

func (l chatGPTOwnedReActLoop) modelName() string {
	if l.generator == nil {
		return ""
	}
	return l.generator.model
}

func (l chatGPTOwnedReActLoop) createResponse(ctx context.Context, request openAIResponsesRequest, streamer func(string, any)) (openAIResponsesResponse, error) {
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

func buildChatGPTResponsesRequest(req ai.ReasoningRequest, model string, tools []ai.ToolDefinition) (openAIResponsesRequest, error) {
	responsesTools, err := buildChatGPTResponsesTools(tools)
	if err != nil {
		return openAIResponsesRequest{}, err
	}
	parallelToolCalls := true
	store := true
	temperature := float32(0.1)
	request := openAIResponsesRequest{
		Model:        model,
		Instructions: strings.TrimSpace(req.SystemPrompt),
		Input: []openAIResponsesInputItem{{
			Role:    "user",
			Content: chatGPTOwnedLoopPrompt(req),
		}},
		Tools:             responsesTools,
		Include:           []string{"reasoning.encrypted_content"},
		Reasoning:         &openAIResponsesReasoning{Effort: "low"},
		ParallelToolCalls: &parallelToolCalls,
		Store:             &store,
		Temperature:       &temperature,
	}
	if req.Streamer != nil {
		stream := true
		request.Stream = &stream
	}
	if state := req.CarryoverState; state != nil {
		request.PreviousResponseID = strings.TrimSpace(state.ConversationHandle)
	}
	return request, nil
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

func chatGPTOwnedLoopPrompt(req ai.ReasoningRequest) string {
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

func (g *chatgpt) createResponses(ctx context.Context, reqBody openAIResponsesRequest) (openAIResponsesResponse, error) {
	if g == nil {
		return openAIResponsesResponse{}, fmt.Errorf("chatgpt generator is nil")
	}
	if g.apiKey == "" {
		return openAIResponsesResponse{}, fmt.Errorf("missing OpenAI API Key. Please set LLM_API_KEY or OPENAI_API_KEY environment variable")
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return openAIResponsesResponse{}, fmt.Errorf("failed to marshal responses request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/responses", bytes.NewBuffer(jsonBody))
	if err != nil {
		return openAIResponsesResponse{}, fmt.Errorf("failed to create responses request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+g.apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return openAIResponsesResponse{}, fmt.Errorf("openai responses api request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return openAIResponsesResponse{}, fmt.Errorf("openai responses api error (status %d): %s", resp.StatusCode, string(body))
	}
	var response openAIResponsesResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return openAIResponsesResponse{}, fmt.Errorf("failed to unmarshal responses api response: %w", err)
	}
	if response.Error != nil {
		return openAIResponsesResponse{}, fmt.Errorf("openai responses api returned error: %s", response.Error.Message)
	}
	return response, nil
}

func (g *chatgpt) createResponsesStream(ctx context.Context, reqBody openAIResponsesRequest, streamer func(string, any)) (openAIResponsesResponse, error) {
	if g == nil {
		return openAIResponsesResponse{}, fmt.Errorf("chatgpt generator is nil")
	}
	if g.apiKey == "" {
		return openAIResponsesResponse{}, fmt.Errorf("missing OpenAI API Key. Please set LLM_API_KEY or OPENAI_API_KEY environment variable")
	}
	stream := true
	reqBody.Stream = &stream
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return openAIResponsesResponse{}, fmt.Errorf("failed to marshal responses stream request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/responses", bytes.NewBuffer(jsonBody))
	if err != nil {
		return openAIResponsesResponse{}, fmt.Errorf("failed to create responses stream request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+g.apiKey)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return openAIResponsesResponse{}, fmt.Errorf("openai responses stream request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return openAIResponsesResponse{}, fmt.Errorf("openai responses stream api error (status %d): %s", resp.StatusCode, string(body))
	}
	return parseChatGPTResponsesStream(resp.Body, streamer)
}

func parseChatGPTResponsesStream(reader io.Reader, streamer func(string, any)) (openAIResponsesResponse, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 4096), 1024*1024)
	trackedItems := make(map[string]openAIResponsesOutputItem)
	var dataLines []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if len(dataLines) == 0 {
				continue
			}
			response, done, err := handleChatGPTResponsesStreamEvent(strings.Join(dataLines, "\n"), trackedItems, streamer)
			dataLines = nil
			if err != nil {
				return openAIResponsesResponse{}, err
			}
			if done {
				return response, nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		return openAIResponsesResponse{}, fmt.Errorf("failed to read openai responses stream: %w", err)
	}
	if len(dataLines) > 0 {
		response, done, err := handleChatGPTResponsesStreamEvent(strings.Join(dataLines, "\n"), trackedItems, streamer)
		if err != nil {
			return openAIResponsesResponse{}, err
		}
		if done {
			return response, nil
		}
	}
	return openAIResponsesResponse{}, fmt.Errorf("openai responses stream ended without response.completed")
}

func handleChatGPTResponsesStreamEvent(raw string, trackedItems map[string]openAIResponsesOutputItem, streamer func(string, any)) (openAIResponsesResponse, bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "[DONE]" {
		return openAIResponsesResponse{}, false, nil
	}
	var event openAIResponsesStreamEvent
	if err := json.Unmarshal([]byte(trimmed), &event); err != nil {
		return openAIResponsesResponse{}, false, fmt.Errorf("failed to decode openai responses stream event: %w", err)
	}
	switch event.Type {
	case "error":
		return openAIResponsesResponse{}, false, fmt.Errorf("openai responses stream error: %s", strings.TrimSpace(event.Message))
	case "response.output_item.added", "response.output_item.done":
		if event.Item != nil && strings.TrimSpace(event.Item.ID) != "" {
			trackedItems[strings.TrimSpace(event.Item.ID)] = *event.Item
		}
	case "response.output_text.delta":
		if streamer != nil {
			phase := chatGPTTrackedItemPhase(trackedItems, event.ItemID)
			if phase == "" {
				phase = "final_answer"
			}
			streamer(ai.ReasoningEventAssistantMessage, ai.BuildStreamingAssistantMessageEvent(phase, event.Delta, event.ItemID, "assistant_output", "delta"))
		}
	case "response.function_call_arguments.delta":
		if streamer != nil {
			itemID := strings.TrimSpace(event.ItemID)
			item := trackedItems[itemID]
			payload := ai.BuildStreamingToolCallEvent(item.Name, itemID, item.CallID, "delta")
			payload["arguments_delta"] = event.Delta
			streamer(ai.ReasoningEventToolCall, payload)
		}
	case "response.function_call_arguments.done":
		if itemID := strings.TrimSpace(event.ItemID); itemID != "" {
			item := trackedItems[itemID]
			if strings.TrimSpace(event.Name) != "" {
				item.Name = strings.TrimSpace(event.Name)
			}
			item.Arguments = event.Arguments
			trackedItems[itemID] = item
			if streamer != nil {
				payload := ai.BuildStreamingToolCallEvent(item.Name, itemID, item.CallID, "done")
				payload["arguments"] = event.Arguments
				if args := parseChatGPTStreamedToolArgs(event.Arguments); len(args) > 0 {
					payload["args"] = args
				}
				streamer(ai.ReasoningEventToolCall, payload)
			}
		}
	case "response.reasoning_summary_text.delta":
		if streamer != nil {
			payload := ai.BuildStreamingAssistantMessageEvent("commentary", event.Delta, event.ItemID, "reasoning_summary", "delta")
			payload["summary_index"] = event.SummaryIndex
			streamer(ai.ReasoningEventAssistantMessage, payload)
		}
	case "response.reasoning_summary_text.done":
		if streamer != nil {
			payload := ai.BuildStreamingAssistantMessageEvent("commentary", event.Text, event.ItemID, "reasoning_summary", "done")
			payload["summary_index"] = event.SummaryIndex
			streamer(ai.ReasoningEventAssistantMessage, payload)
		}
	case "response.completed":
		if event.Response == nil {
			return openAIResponsesResponse{}, false, fmt.Errorf("openai responses stream completed without response payload")
		}
		return *event.Response, true, nil
	case "response.failed":
		if event.Response != nil && event.Response.Error != nil {
			return openAIResponsesResponse{}, false, fmt.Errorf("openai responses stream failed: %s", event.Response.Error.Message)
		}
		return openAIResponsesResponse{}, false, fmt.Errorf("openai responses stream failed")
	case "response.incomplete":
		return openAIResponsesResponse{}, false, fmt.Errorf("openai responses stream incomplete")
	}
	return openAIResponsesResponse{}, false, nil
}

func chatGPTTrackedItemPhase(trackedItems map[string]openAIResponsesOutputItem, itemID string) string {
	if item, ok := trackedItems[strings.TrimSpace(itemID)]; ok {
		return strings.TrimSpace(item.Phase)
	}
	return ""
}

func openAIResponseToolCalls(response openAIResponsesResponse) ([]ai.ToolCall, error) {
	if len(response.Output) == 0 {
		return nil, nil
	}
	toolCalls := make([]ai.ToolCall, 0)
	for _, item := range response.Output {
		if item.Type != "function_call" {
			continue
		}
		args := map[string]any(nil)
		if strings.TrimSpace(item.Arguments) != "" {
			if err := json.Unmarshal([]byte(item.Arguments), &args); err != nil {
				return nil, fmt.Errorf("invalid OpenAI function_call arguments for %q: %w", item.Name, err)
			}
		}
		toolCalls = append(toolCalls, ai.ToolCall{
			Name:     strings.TrimSpace(item.Name),
			Args:     args,
			NativeID: strings.TrimSpace(item.CallID),
			TransportMeta: map[string]any{
				"response_item_id": strings.TrimSpace(item.ID),
			},
		})
	}
	if len(toolCalls) == 0 {
		return nil, nil
	}
	return toolCalls, nil
}

func openAIResponseFinalText(response openAIResponsesResponse) string {
	if text := strings.TrimSpace(response.OutputText); text != "" {
		return text
	}
	finalParts := make([]string, 0, len(response.Output))
	fallbackParts := make([]string, 0, len(response.Output))
	for _, message := range openAIResponseAssistantMessages(response) {
		text := strings.TrimSpace(message.Text)
		if text == "" {
			continue
		}
		fallbackParts = append(fallbackParts, text)
		if message.Phase == "final_answer" {
			finalParts = append(finalParts, text)
		}
	}
	if len(finalParts) > 0 {
		return strings.Join(finalParts, "\n")
	}
	return strings.Join(fallbackParts, "\n")
}

type openAIResponsesAssistantMessage struct {
	Phase string
	Text  string
}

func openAIResponseAssistantMessages(response openAIResponsesResponse) []openAIResponsesAssistantMessage {
	if len(response.Output) == 0 {
		return nil
	}
	messages := make([]openAIResponsesAssistantMessage, 0, len(response.Output))
	for _, item := range response.Output {
		if item.Type != "message" || item.Role != "assistant" {
			continue
		}
		parts := make([]string, 0, len(item.Content))
		for _, content := range item.Content {
			if content.Type != "output_text" {
				continue
			}
			text := strings.TrimSpace(content.Text)
			if text != "" {
				parts = append(parts, text)
			}
		}
		if len(parts) == 0 {
			continue
		}
		messages = append(messages, openAIResponsesAssistantMessage{
			Phase: strings.TrimSpace(item.Phase),
			Text:  strings.Join(parts, "\n"),
		})
	}
	if len(messages) == 0 {
		return nil
	}
	return messages
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

func buildChatGPTContinuationInput(response openAIResponsesResponse, outputs []openAIResponsesInputItem) []openAIResponsesInputItem {
	if strings.TrimSpace(response.ID) != "" {
		return outputs
	}
	replayed := openAIResponseContinuationReplayItems(response)
	if len(replayed) == 0 {
		return outputs
	}
	combined := make([]openAIResponsesInputItem, 0, len(replayed)+len(outputs))
	combined = append(combined, replayed...)
	combined = append(combined, outputs...)
	return combined
}

func openAIResponseContinuationReplayItems(response openAIResponsesResponse) []openAIResponsesInputItem {
	if len(response.Output) == 0 {
		return nil
	}
	items := make([]openAIResponsesInputItem, 0, len(response.Output))
	for _, item := range response.Output {
		switch item.Type {
		case "message":
			if item.Role != "assistant" {
				continue
			}
			textParts := make([]string, 0, len(item.Content))
			for _, content := range item.Content {
				if content.Type != "output_text" {
					continue
				}
				text := strings.TrimSpace(content.Text)
				if text != "" {
					textParts = append(textParts, text)
				}
			}
			if len(textParts) == 0 {
				continue
			}
			items = append(items, openAIResponsesInputItem{
				ID:      strings.TrimSpace(item.ID),
				Role:    "assistant",
				Content: strings.Join(textParts, "\n"),
				Phase:   strings.TrimSpace(item.Phase),
			})
		case "reasoning":
			replay := openAIResponsesInputItem{
				ID:               strings.TrimSpace(item.ID),
				Type:             "reasoning",
				EncryptedContent: strings.TrimSpace(item.EncryptedContent),
			}
			if replay.ID == "" && replay.EncryptedContent == "" {
				continue
			}
			items = append(items, replay)
		case "function_call":
			items = append(items, openAIResponsesInputItem{
				ID:        strings.TrimSpace(item.ID),
				Type:      "function_call",
				CallID:    strings.TrimSpace(item.CallID),
				Name:      strings.TrimSpace(item.Name),
				Arguments: strings.TrimSpace(item.Arguments),
			})
		}
	}
	if len(items) == 0 {
		return nil
	}
	return items
}

func executeChatGPTOwnedLoopToolCall(ctx context.Context, req ai.ReasoningRequest, iteration int, toolCall ai.ToolCall) (ai.ReActToolResult, openAIResponsesInputItem) {
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
			Output: resultText,
		}
}

func emitChatGPTOwnedLoopEvent(req ai.ReasoningRequest, eventType string, data any) {
	if req.Streamer != nil {
		req.Streamer(eventType, data)
	}
}

func parseChatGPTStreamedToolArgs(raw string) map[string]any {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(trimmed), &args); err != nil {
		return nil
	}
	if len(args) == 0 {
		return nil
	}
	return args
}

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
		if bytes, marshalErr := json.Marshal(value); marshalErr == nil {
			return string(bytes)
		}
	}
	return string(raw)
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

func cloneChatGPTToolArgs(args map[string]any) map[string]any {
	if len(args) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(args))
	for key, value := range args {
		cloned[key] = value
	}
	return cloned
}

func chatGPTOwnedLoopResponse(model string, finalText string, toolCalls []ai.ToolCall, outcomeFacts []string, outcomeRecipes []ai.LearnedRecipe, conversationHandle string) ai.ReasoningResponse {
	resp := ai.ReasoningResponse{
		FinalText:      finalText,
		ToolCalls:      toolCalls,
		OutcomeFacts:   append([]string(nil), outcomeFacts...),
		OutcomeRecipes: append([]ai.LearnedRecipe(nil), outcomeRecipes...),
	}
	if carryState := chatGPTOwnedLoopCarryoverState(conversationHandle, toolCalls); carryState != nil {
		carryState.Provider = "chatgpt"
		carryState.Model = model
		carryState.LastAssistantSummary = strings.TrimSpace(finalText)
		carryState.LastOutcomeFacts = append([]string(nil), outcomeFacts...)
		carryState.LastToolNames = chatGPTOwnedLoopToolNames(toolCalls)
		carryState.LastRecipeIDs = chatGPTOwnedLoopRecipeIDs(outcomeRecipes)
		resp.CarryoverState = carryState
	}
	return resp
}

func chatGPTOwnedLoopToolNames(toolCalls []ai.ToolCall) []string {
	if len(toolCalls) == 0 {
		return nil
	}
	names := make([]string, 0, len(toolCalls))
	for _, toolCall := range toolCalls {
		name := strings.TrimSpace(toolCall.Name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil
	}
	return names
}

func chatGPTOwnedLoopRecipeIDs(recipes []ai.LearnedRecipe) []string {
	if len(recipes) == 0 {
		return nil
	}
	ids := make([]string, 0, len(recipes))
	for _, recipe := range recipes {
		id := strings.TrimSpace(recipe.ID)
		if id == "" {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

func chatGPTOwnedLoopCarryoverState(conversationHandle string, toolCalls []ai.ToolCall) *ai.CarryoverState {
	handle := conversationHandle
	if len(toolCalls) == 0 && handle == "" {
		return nil
	}
	state := &ai.CarryoverState{Mode: ai.CarryoverModeCompact}
	if handle != "" {
		state.ConversationHandle = handle
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

func emitChatGPTOwnedLoopHydration(req ai.ReasoningRequest, update ai.MemoryHydrationUpdate) {
	if req.HydrationSink == nil {
		return
	}
	req.HydrationSink(ai.BuildMemoryHydrationUpdateFromParts(update))
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	Temperature float32         `json:"temperature,omitempty"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		TotalTokens int `json:"total_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

// Generate sends a prompt to the ChatGPT API and returns the generated text.
func (g *chatgpt) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	if g.apiKey == "" {
		return ai.GenOutput{}, fmt.Errorf("missing OpenAI API Key. Please set LLM_API_KEY or OPENAI_API_KEY environment variable")
	}

	url := "https://api.openai.com/v1/chat/completions"

	// Simple prompt-to-messages conversion.
	// Ideally, the interface should support a list of messages, but for now we wrap the prompt.
	messages := []openAIMessage{
		{Role: "user", Content: prompt},
	}

	reqBody := openAIRequest{
		Model:       g.model,
		Messages:    messages,
		MaxTokens:   opts.MaxTokens,
		Temperature: opts.Temperature,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+g.apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("openai api request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return ai.GenOutput{}, fmt.Errorf("openai api error (status %d): %s", resp.StatusCode, string(body))
	}

	var openAIResp openAIResponse
	if err := json.Unmarshal(body, &openAIResp); err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if openAIResp.Error != nil {
		return ai.GenOutput{}, fmt.Errorf("openai api returned error: %s", openAIResp.Error.Message)
	}

	if len(openAIResp.Choices) == 0 {
		return ai.GenOutput{}, fmt.Errorf("no choices returned from openai")
	}

	return ai.GenOutput{
		Text:       openAIResp.Choices[0].Message.Content,
		TokensUsed: openAIResp.Usage.TotalTokens,
		Raw:        openAIResp,
	}, nil
}

// EstimateCost estimates the cost of the generation based on token usage.
func (g *chatgpt) EstimateCost(inTokens, outTokens int) float64 {
	// Rough estimate for GPT-4o
	return float64(inTokens)*0.000005 + float64(outTokens)*0.000015
}
