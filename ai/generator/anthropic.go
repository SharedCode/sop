package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/sharedcode/sop/ai"
)

type anthropic struct {
	apiKey string
	model  string
}

func init() {
	Register("anthropic", func(cfg map[string]any) (ai.Generator, error) {
		apiKey, _ := cfg["api_key"].(string)
		model, _ := cfg["model"].(string)
		if model == "" {
			model = "claude-3-5-sonnet-20241022"
		}
		return &anthropic{apiKey: apiKey, model: model}, nil
	})
}

func (g *anthropic) Name() string { return "anthropic" }

func (g *anthropic) CarryoverCapability() ai.CarryoverCapability {
	return ai.CarryoverCapability{
		Provider:        g.Name(),
		Model:           g.model,
		SupportsCompact: true,
		SupportsLive:    false,
	}
}

// supportsTemperature checks if the model supports the temperature parameter.
// Claude Opus 4.7+ has deprecated temperature (verified by API error).
func (g *anthropic) supportsTemperature() bool {
	modelLower := strings.ToLower(strings.TrimSpace(g.model))
	// Only exclude temperature for models we know have deprecated it
	// Currently confirmed: Claude Opus 4.7+
	if strings.Contains(modelLower, "opus-4") {
		return false
	}
	// Default to supporting temperature for all other models
	return true
}

// anthropicContentBlock represents a content block in a message
type anthropicContentBlock struct {
	Type         string                 `json:"type"` // "text", "tool_use", "tool_result"
	Text         string                 `json:"text,omitempty"`
	ID           string                 `json:"id,omitempty"`            // For tool_use
	Name         string                 `json:"name,omitempty"`          // For tool_use
	Input        map[string]any         `json:"input,omitempty"`         // For tool_use
	ToolUseID    string                 `json:"tool_use_id,omitempty"`   // For tool_result
	Content      any                    `json:"content,omitempty"`       // For tool_result
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"` // For prompt caching
}

// anthropicCacheControl marks content for caching (saves up to 90% on input costs)
type anthropicCacheControl struct {
	Type string `json:"type"` // "ephemeral"
}

// anthropicMessage represents a message in the conversation
type anthropicMessage struct {
	Role    string `json:"role"`    // "user" or "assistant"
	Content any    `json:"content"` // string or []anthropicContentBlock
}

// anthropicTool represents a tool definition
type anthropicTool struct {
	Name         string                 `json:"name"`
	Description  string                 `json:"description"`
	InputSchema  map[string]any         `json:"input_schema"`
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"` // For caching tools
}

type anthropicRequest struct {
	Model       string                  `json:"model"`
	Messages    []anthropicMessage      `json:"messages"`
	MaxTokens   int                     `json:"max_tokens"`
	Temperature *float32                `json:"temperature,omitempty"` // Pointer to allow omission for newer models
	System      []anthropicContentBlock `json:"system,omitempty"`      // System prompt (supports cache_control)
	Tools       []anthropicTool         `json:"tools,omitempty"`
}

type anthropicResponse struct {
	ID           string                  `json:"id"`
	Type         string                  `json:"type"`
	Role         string                  `json:"role"`
	Content      []anthropicContentBlock `json:"content"`
	Model        string                  `json:"model"`
	StopReason   string                  `json:"stop_reason"` // "end_turn", "tool_use", etc.
	StopSequence string                  `json:"stop_sequence,omitempty"`
	Usage        struct {
		InputTokens              int `json:"input_tokens"`
		OutputTokens             int `json:"output_tokens"`
		CacheCreationInputTokens int `json:"cache_creation_input_tokens,omitempty"` // Tokens written to cache
		CacheReadInputTokens     int `json:"cache_read_input_tokens,omitempty"`     // Tokens read from cache (90% savings)
	} `json:"usage"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (g *anthropic) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	if g.apiKey == "" {
		return ai.GenOutput{}, fmt.Errorf("missing Anthropic API Key")
	}

	url := "https://api.anthropic.com/v1/messages"

	// Build messages array with tool call continuations if provided
	messages := g.buildMessages(prompt, opts)

	maxTokens := opts.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096 // Anthropic requires max_tokens
	}

	reqBody := anthropicRequest{
		Model:     g.model,
		Messages:  messages,
		MaxTokens: maxTokens,
	}

	// Only set temperature for models that support it
	// Claude Opus 4+ and newer models have deprecated temperature
	if g.supportsTemperature() {
		temp := opts.Temperature
		reqBody.Temperature = &temp
	}

	// Add system prompt if provided (explicit parameter from opts)
	// Enable caching for system prompt (typically 1000s of tokens of instructions)
	if opts.SystemPrompt != "" {
		reqBody.System = []anthropicContentBlock{
			{
				Type:         "text",
				Text:         opts.SystemPrompt,
				CacheControl: &anthropicCacheControl{Type: "ephemeral"}, // Cache system prompt
			},
		}
	}

	// Add tools if provided (explicit parameter from opts, NOT from Context!)
	// Enable caching for tools (typically reused across many requests)
	if len(opts.Tools) > 0 {
		reqBody.Tools = g.convertToolsWithCaching(opts.Tools)
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
	req.Header.Set("x-api-key", g.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("anthropic-beta", "prompt-caching-2024-07-31") // Enable prompt caching

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("anthropic api request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return ai.GenOutput{}, fmt.Errorf("anthropic api error (status %d): %s", resp.StatusCode, string(body))
	}

	var anthropicResp anthropicResponse
	if err := json.Unmarshal(body, &anthropicResp); err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if anthropicResp.Error != nil {
		return ai.GenOutput{}, fmt.Errorf("anthropic error: %s", anthropicResp.Error.Message)
	}

	// Extract text and tool calls from response
	var text strings.Builder
	var toolCalls []ai.ToolCall

	for _, block := range anthropicResp.Content {
		switch block.Type {
		case "text":
			if block.Text != "" {
				text.WriteString(block.Text)
			}
		case "tool_use":
			toolCalls = append(toolCalls, ai.ToolCall{
				Name:     block.Name,
				Args:     block.Input,
				NativeID: block.ID,
			})
		}
	}

	return ai.GenOutput{
		Text:       text.String(),
		TokensUsed: anthropicResp.Usage.InputTokens + anthropicResp.Usage.OutputTokens,
		ToolCalls:  toolCalls,
		Raw:        anthropicResp,
	}, nil
}

// buildMessages constructs the messages array with tool call continuations
// Parameters passed explicitly, NOT extracted from Context
func (g *anthropic) buildMessages(prompt string, opts ai.GenOptions) []anthropicMessage {
	messages := []anthropicMessage{}

	// Add tool call continuations if provided (explicit parameter from opts)
	if len(opts.ToolCallContinuations) > 0 {
		for _, cont := range opts.ToolCallContinuations {
			// Assistant message with tool use
			assistantContent := []anthropicContentBlock{
				{
					Type:  "tool_use",
					ID:    cont.ToolCall.NativeID,
					Name:  cont.ToolCall.Name,
					Input: cont.ToolCall.Args,
				},
			}
			messages = append(messages, anthropicMessage{
				Role:    "assistant",
				Content: assistantContent,
			})

			// User message with tool result
			resultContent, _ := json.Marshal(cont.Response)
			userContent := []anthropicContentBlock{
				{
					Type:      "tool_result",
					ToolUseID: cont.ToolCall.NativeID,
					Content:   string(resultContent),
				},
			}
			messages = append(messages, anthropicMessage{
				Role:    "user",
				Content: userContent,
			})
		}
	}

	// Add current user prompt (explicit parameter)
	messages = append(messages, anthropicMessage{
		Role:    "user",
		Content: prompt, // Simple string content
	})

	return messages
}

// convertTools converts SOP tool definitions to Anthropic format
// Parameters passed explicitly, NOT extracted from Context
func (g *anthropic) convertTools(tools []ai.ToolDefinition) []anthropicTool {
	anthropicTools := make([]anthropicTool, 0, len(tools))

	for _, t := range tools {
		var inputSchema map[string]any
		if t.Schema != "" && strings.HasPrefix(strings.TrimSpace(t.Schema), "{") {
			json.Unmarshal([]byte(t.Schema), &inputSchema)
		} else if t.Schema != "" {
			inputSchema = map[string]any{"type": "object"}
		} else {
			inputSchema = map[string]any{"type": "object"}
		}

		anthropicTools = append(anthropicTools, anthropicTool{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: inputSchema,
		})
	}

	return anthropicTools
}

// convertToolsWithCaching converts tools and marks the LAST tool for caching
// Anthropic caches everything up to and including the marked block
func (g *anthropic) convertToolsWithCaching(tools []ai.ToolDefinition) []anthropicTool {
	anthropicTools := g.convertTools(tools)

	// Mark the last tool with cache_control to cache all tools
	// (Anthropic caches from start to the marked breakpoint)
	if len(anthropicTools) > 0 {
		anthropicTools[len(anthropicTools)-1].CacheControl = &anthropicCacheControl{Type: "ephemeral"}
	}

	return anthropicTools
}

// EstimateCost estimates the cost of the generation based on token usage.
func (g *anthropic) EstimateCost(inTokens, outTokens int) float64 {
	// Rough estimate for Claude 3.5 Sonnet
	return float64(inTokens)*0.000003 + float64(outTokens)*0.000015
}
