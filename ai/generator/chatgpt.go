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

// chatgpt implements the Generator interface for OpenAI's ChatGPT models.
type chatgpt struct {
	apiKey string
	model  string
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
		return &chatgpt{apiKey: apiKey, model: model}, nil
	})
}

// Name returns the name of the generator.
func (g *chatgpt) Name() string { return "chatgpt" }

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
		return ai.GenOutput{}, fmt.Errorf("missing OpenAI API Key")
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
