package ai

import "strings"

const (
	ReasoningEventAssistantMessage = "assistant_message"
	ReasoningEventToolCall         = "tool_call"
	ReasoningEventToolResult       = "tool_result"
	ReasoningEventToolError        = "tool_error"
)

func BuildAssistantMessageEvent(phase, text string) map[string]any {
	payload := map[string]any{
		"text": text,
	}
	if phase = strings.TrimSpace(phase); phase != "" {
		payload["phase"] = phase
	}
	return payload
}

func BuildToolCallEvent(tool string, args map[string]any, iteration int) map[string]any {
	payload := map[string]any{
		"tool": strings.TrimSpace(tool),
		"args": args,
	}
	if iteration > 0 {
		payload["iteration"] = iteration
	}
	return payload
}

func BuildToolResultEvent(tool string, args map[string]any, result string, hint *ToolProgressHint, iteration int) map[string]any {
	payload := map[string]any{
		"tool":         strings.TrimSpace(tool),
		"args":         args,
		"result":       result,
		"result_chars": len(result),
	}
	if hint != nil {
		payload["progress_hint"] = hint
	}
	if iteration > 0 {
		payload["iteration"] = iteration
	}
	return payload
}

func BuildToolErrorEvent(tool string, args map[string]any, err error, iteration int) map[string]any {
	message := ""
	if err != nil {
		message = err.Error()
	}
	payload := map[string]any{
		"tool":  strings.TrimSpace(tool),
		"args":  args,
		"error": message,
	}
	if iteration > 0 {
		payload["iteration"] = iteration
	}
	return payload
}

func BuildStreamingAssistantMessageEvent(phase, text, itemID, source, streamState string) map[string]any {
	payload := BuildAssistantMessageEvent(phase, text)
	payload["streaming"] = true
	if itemID = strings.TrimSpace(itemID); itemID != "" {
		payload["item_id"] = itemID
	}
	if source = strings.TrimSpace(source); source != "" {
		payload["source"] = source
	}
	if streamState = strings.TrimSpace(streamState); streamState != "" {
		payload["stream_state"] = streamState
	}
	return payload
}

func BuildStreamingToolCallEvent(tool, itemID, callID, streamState string) map[string]any {
	payload := map[string]any{
		"tool":      strings.TrimSpace(tool),
		"streaming": true,
	}
	if itemID = strings.TrimSpace(itemID); itemID != "" {
		payload["item_id"] = itemID
	}
	if callID = strings.TrimSpace(callID); callID != "" {
		payload["call_id"] = callID
	}
	if streamState = strings.TrimSpace(streamState); streamState != "" {
		payload["stream_state"] = streamState
	}
	return payload
}
