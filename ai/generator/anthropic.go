package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/sharedcode/sop/ai"
)

type anthropic struct {
	apiKey string
	model  string
}

func init() {
	Register("anthropic", func(cfg map[string]any) (ai.Generator, error) {
		apiKey, _ := cfg["api_key"].(string)
		if apiKey == "" {
			apiKey = os.Getenv("ANTHROPIC_API_KEY")
			if apiKey == "" {
				apiKey = os.Getenv("LLM_API_KEY")
			}
		}
		model, _ := cfg["model"].(string)
		if model == "" {
			model = os.Getenv("ANTHROPIC_MODEL")
		}
		if model == "" {
			model = "claude-3-5-sonnet-20240620"
		}
		return &anthropic{apiKey: apiKey, model: model}, nil
	})
}

func (g *anthropic) Name() string { return "anthropic" }

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicRequest struct {
	Model       string             `json:"model"`
	Messages    []anthropicMessage `json:"messages"`
	MaxTokens   int                `json:"max_tokens"`
	Temperature float32            `json:"temperature,omitempty"`
}

type anthropicResponse struct {
	Content []struct {
		Text string `json:"text"`
		Type string `json:"type"`
	} `json:"content"`
	Usage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
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

	messages := []anthropicMessage{
		{Role: "user", Content: prompt},
	}

	maxTokens := opts.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096 // Anthropic requires max_tokens
	}

	reqBody := anthropicRequest{
		Model:       g.model,
		Messages:    messages,
		MaxTokens:   maxTokens,
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
	req.Header.Set("x-api-key", g.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

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

	var text string
	if len(anthropicResp.Content) > 0 {
		text = anthropicResp.Content[0].Text
	}

	return ai.GenOutput{
		Text:       text,
		TokensUsed: anthropicResp.Usage.InputTokens + anthropicResp.Usage.OutputTokens,
		Raw:        anthropicResp,
	}, nil
}

// EstimateCost estimates the cost of the generation based on token usage.
func (g *anthropic) EstimateCost(inTokens, outTokens int) float64 {
	// Rough estimate for Claude 3.5 Sonnet
	return float64(inTokens)*0.000003 + float64(outTokens)*0.000015
}
