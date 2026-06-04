package generator

// chatgpt_responses_api.go — OpenAI Responses API transport layer.
//
// Responsibilities:
//   - Sending requests to the Responses API (blocking and streaming).
//   - Parsing the SSE stream into a completed openAIResponsesResponse.
//   - Extracting tool calls and assistant text from a response.
//   - Building the continuation input items for multi-turn tool loops.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	log "log/slog"
	"net/http"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// ----------------------------------------------------------------------------
// Blocking send
// ----------------------------------------------------------------------------

func (g *chatgpt) createResponses(ctx context.Context, reqBody openAIResponsesRequest) (openAIResponsesResponse, error) {
	if g == nil {
		return openAIResponsesResponse{}, fmt.Errorf("chatgpt generator is nil")
	}
	if g.apiKey == "" {
		return openAIResponsesResponse{}, fmt.Errorf("missing OpenAI API Key. Please provide api_key in generator configuration")
	}
	log.Info("ChatGPT Responses API call", "url", g.responsesURL(), "model", reqBody.Model, "tool_choice", reqBody.ToolChoice, "tools", len(reqBody.Tools))

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return openAIResponsesResponse{}, fmt.Errorf("failed to marshal responses request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.responsesURL(), bytes.NewBuffer(jsonBody))
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

// ----------------------------------------------------------------------------
// Streaming send
// ----------------------------------------------------------------------------

func (g *chatgpt) createResponsesStream(ctx context.Context, reqBody openAIResponsesRequest, streamer func(string, any)) (openAIResponsesResponse, error) {
	if g == nil {
		return openAIResponsesResponse{}, fmt.Errorf("chatgpt generator is nil")
	}
	if g.apiKey == "" {
		return openAIResponsesResponse{}, fmt.Errorf("missing OpenAI API Key. Please provide api_key in generator configuration")
	}
	stream := true
	reqBody.Stream = &stream
	log.Info("ChatGPT Responses API call (stream)", "url", g.responsesURL(), "model", reqBody.Model, "tool_choice", reqBody.ToolChoice, "tools", len(reqBody.Tools))

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return openAIResponsesResponse{}, fmt.Errorf("failed to marshal responses stream request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.responsesURL(), bytes.NewBuffer(jsonBody))
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
	return parseChatGPTResponsesStream(ctx, resp.Body, streamer)
}

// ----------------------------------------------------------------------------
// SSE stream parser
// ----------------------------------------------------------------------------

func parseChatGPTResponsesStream(ctx context.Context, reader io.Reader, streamer func(string, any)) (openAIResponsesResponse, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 4096), 1024*1024)
	trackedItems := make(map[string]openAIResponsesOutputItem)
	var dataLines []string

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return openAIResponsesResponse{}, err
		}
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
			continue // SSE keep-alive comment
		}
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := ctx.Err(); err != nil {
		return openAIResponsesResponse{}, err
	}
	if err := scanner.Err(); err != nil {
		return openAIResponsesResponse{}, fmt.Errorf("failed to read openai responses stream: %w", err)
	}
	// Flush any remaining data lines
	if len(dataLines) > 0 {
		if err := ctx.Err(); err != nil {
			return openAIResponsesResponse{}, err
		}
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
		// Raw token deltas are transport detail. The owned loop emits the final
		// assistant message once from the completed response.

	case "response.function_call_arguments.delta":
		// Argument chunks are SSE-internal detail. No bridge event emitted.
		if itemID := strings.TrimSpace(event.ItemID); itemID != "" {
			item := trackedItems[itemID]
			item.Arguments += event.Delta
			trackedItems[itemID] = item
		}

	case "response.function_call_arguments.done":
		// Update the tracked item with final assembled arguments.
		// The tool_call bridge event is emitted once by executeToolCalls, same as non-streaming.
		if itemID := strings.TrimSpace(event.ItemID); itemID != "" {
			item := trackedItems[itemID]
			if strings.TrimSpace(event.Name) != "" {
				item.Name = strings.TrimSpace(event.Name)
			}
			item.Arguments = event.Arguments
			trackedItems[itemID] = item
		}

	case "response.reasoning_summary_text.delta":
		// Reasoning summaries are internal model commentary. Do not surface them to
		// the user-facing stream.

	case "response.reasoning_summary_text.done":
		// Reasoning summaries are internal model commentary. Do not surface them to
		// the user-facing stream.

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

// ----------------------------------------------------------------------------
// Response parsing — extract tool calls and assistant text
// ----------------------------------------------------------------------------

// openAIResponseToolCalls returns all function_call items from a response.
// Falls back to parsing textual Function_call: payloads in assistant messages
// when the API returns a plain text response instead of a native function_call.
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
	// Textual fallback: some proxy/stub APIs return "Function_call: {...}" in assistant text
	if len(toolCalls) == 0 {
		for _, message := range openAIResponseAssistantMessages(response) {
			toolCall, ok, err := parseChatGPTTextualFunctionCall(message.Text)
			if err != nil {
				return nil, err
			}
			if ok {
				toolCalls = append(toolCalls, toolCall)
			}
		}
	}
	if len(toolCalls) == 0 {
		return nil, nil
	}
	return toolCalls, nil
}

// openAIResponseFinalText extracts the final assistant answer text from a response.
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

// openAIResponseAssistantMessages returns all assistant message items with their text content.
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
			if text := strings.TrimSpace(content.Text); text != "" {
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

// ----------------------------------------------------------------------------
// Continuation input — replay prior turn output for stateless multi-turn
// ----------------------------------------------------------------------------

// buildChatGPTContinuationInput builds the input items for the next turn.
// When the response has an ID, PreviousResponseID handles continuity server-side
// so only the tool output items need to be sent. When there is no ID (stateless
// or stub mode) the prior turn's output is replayed inline.
func buildChatGPTContinuationInput(response openAIResponsesResponse, toolOutputs []openAIResponsesInputItem) []openAIResponsesInputItem {
	if strings.TrimSpace(response.ID) != "" {
		return toolOutputs
	}
	replayed := openAIResponseContinuationReplayItems(response)
	if len(replayed) == 0 {
		return toolOutputs
	}
	combined := make([]openAIResponsesInputItem, 0, len(replayed)+len(toolOutputs))
	combined = append(combined, replayed...)
	combined = append(combined, toolOutputs...)
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
				if text := strings.TrimSpace(content.Text); text != "" {
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
