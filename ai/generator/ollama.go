package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/sharedcode/sop/ai"
)

// ollama implements the Generator interface for local Ollama models.
type ollama struct {
	baseURL string
	model   string
}

func init() {
	Register("ollama", func(cfg map[string]any) (ai.Generator, error) {
		baseURL, _ := cfg["base_url"].(string)
		if baseURL == "" {
			baseURL = "http://localhost:11434"
		}
		model, _ := cfg["model"].(string)
		if model == "" {
			model = "llama3"
		}
		return &ollama{baseURL: baseURL, model: model}, nil
	})
}

// Name returns the name of the generator.
func (g *ollama) Name() string { return "ollama" }

type ollamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type ollamaResponse struct {
	Response string `json:"response"`
	Done     bool   `json:"done"`
}

// Generate sends a prompt to the Ollama API and returns the generated text.
func (g *ollama) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	url := fmt.Sprintf("%s/api/generate", g.baseURL)

	reqBody := ollamaRequest{
		Model:  g.model,
		Prompt: prompt,
		Stream: false,
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("ollama api request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return ai.GenOutput{}, fmt.Errorf("ollama api error (status %d): %s", resp.StatusCode, string(body))
	}

	var ollamaResp ollamaResponse
	if err := json.Unmarshal(body, &ollamaResp); err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return ai.GenOutput{
		Text:       ollamaResp.Response,
		TokensUsed: len(prompt) / 4, // Rough estimate
	}, nil
}

// EstimateCost estimates the cost of the generation. For local models, this is typically zero.
func (g *ollama) EstimateCost(inTokens, outTokens int) float64 {
	return 0 // Free!
}
