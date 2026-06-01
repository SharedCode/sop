package generator

// chatgpt.go — Core ChatGPT generator: struct, registration, Name, Generate, EstimateCost.
//
// Everything else lives in dedicated files:
//   - chatgpt_types.go          Wire types for both APIs (Responses + Chat Completions)
//   - chatgpt_responses_api.go  HTTP transport for the Responses API (blocking + streaming)
//   - chatgpt_react_loop.go     Owned ReAct loop, request building, tool execution

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

// chatgpt implements the Generator interface for OpenAI's ChatGPT models.
type chatgpt struct {
	apiKey       string
	model        string
	apiURL       string
	ownedLoop    ai.ReActLoop
	newOwnedLoop func(*chatgpt) ai.ReActLoop
	supportsLive bool
}

func init() {
	Register("chatgpt", func(cfg map[string]any) (ai.Generator, error) {
		apiKey, _ := cfg["api_key"].(string)
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI_API_KEY")
		}
		model, _ := cfg["model"].(string)
		if model == "" {
			model = os.Getenv("OPENAI_MODEL")
		}
		if model == "" {
			model = "gpt-4o"
		}
		rawAPIURL, hasAPIURL := cfg["api_url"]
		apiURL, _ := rawAPIURL.(string)
		if !hasAPIURL {
			apiURL = os.Getenv("OPENAI_API_BASE_URL")
		}
		gen := &chatgpt{apiKey: apiKey, model: model, apiURL: strings.TrimSpace(apiURL)}
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

// ----------------------------------------------------------------------------
// Generator interface
// ----------------------------------------------------------------------------

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

// Generate sends a single prompt to the Chat Completions API and returns the response.
// For multi-turn tool-using workflows, use the Responses API via the ReAct loop.
func (g *chatgpt) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	if g.apiKey == "" {
		return ai.GenOutput{}, fmt.Errorf("missing OpenAI API Key. Please set OPENAI_API_KEY environment variable")
	}
	messages := make([]openAIMessage, 0, 2)
	if systemPrompt := strings.TrimSpace(opts.SystemPrompt); systemPrompt != "" {
		messages = append(messages, openAIMessage{Role: "system", Content: systemPrompt})
	}
	messages = append(messages, openAIMessage{Role: "user", Content: prompt})

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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.chatCompletionsURL(), bytes.NewBuffer(jsonBody))
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
	var response openAIResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}
	if response.Error != nil {
		return ai.GenOutput{}, fmt.Errorf("openai api returned error: %s", response.Error.Message)
	}
	if len(response.Choices) == 0 {
		return ai.GenOutput{}, fmt.Errorf("no choices returned from openai")
	}
	return ai.GenOutput{
		Text:       response.Choices[0].Message.Content,
		TokensUsed: response.Usage.TotalTokens,
		Raw:        response,
	}, nil
}

// EstimateCost returns a rough cost estimate based on token counts (GPT-4o pricing).
func (g *chatgpt) EstimateCost(inTokens, outTokens int) float64 {
	return float64(inTokens)*0.000005 + float64(outTokens)*0.000015
}

// ----------------------------------------------------------------------------
// URL helpers
// ----------------------------------------------------------------------------

func (g *chatgpt) responsesURL() string {
	if g == nil || strings.TrimSpace(g.apiURL) == "" {
		return "https://api.openai.com/v1/responses"
	}
	return strings.TrimRight(strings.TrimSpace(g.apiURL), "/") + "/responses"
}

func (g *chatgpt) chatCompletionsURL() string {
	if g == nil || strings.TrimSpace(g.apiURL) == "" {
		return "https://api.openai.com/v1/chat/completions"
	}
	return strings.TrimRight(strings.TrimSpace(g.apiURL), "/") + "/chat/completions"
}
