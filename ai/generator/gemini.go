package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

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
			apiKey = os.Getenv("GEMINI_API_KEY")
		}
		model, _ := cfg["model"].(string)
		if model == "" {
			model = os.Getenv("GEMINI_MODEL")
		}
		if model == "" {
			model = "gemini-pro"
		}
		return &gemini{apiKey: apiKey, model: model}, nil
	})
}

// Name returns the name of the generator.
func (g *gemini) Name() string { return "gemini" }

type geminiRequest struct {
	Contents []geminiContent `json:"contents"`
}

type geminiContent struct {
	Parts []geminiPart `json:"parts"`
}

type geminiPart struct {
	Text string `json:"text"`
}

type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Generate sends a prompt to the Gemini API and returns the generated text.
func (g *gemini) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	if g.apiKey == "" || g.apiKey == "YOUR_API_KEY" {
		return ai.GenOutput{
			Text: fmt.Sprintf("[Gemini Stub] Missing API Key. Would send: %q", prompt),
		}, nil
	}

	// URL encode the API key to be safe
	safeKey := url.QueryEscape(g.apiKey)
	apiURL := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s", g.model, safeKey)

	reqBody := geminiRequest{
		Contents: []geminiContent{
			{Parts: []geminiPart{{Text: prompt}}},
		},
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

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

	if geminiResp.Error != nil {
		return ai.GenOutput{}, fmt.Errorf("gemini api returned error: %s", geminiResp.Error.Message)
	}

	if len(geminiResp.Candidates) == 0 || len(geminiResp.Candidates[0].Content.Parts) == 0 {
		return ai.GenOutput{}, fmt.Errorf("no candidates returned from gemini")
	}

	text := geminiResp.Candidates[0].Content.Parts[0].Text
	return ai.GenOutput{
		Text:       text,
		TokensUsed: len(prompt) / 4, // Rough estimate
	}, nil
}

// EstimateCost estimates the cost of the generation based on token usage.
func (g *gemini) EstimateCost(inTokens, outTokens int) float64 {
	// Placeholder pricing
	return float64(inTokens)*0.0001 + float64(outTokens)*0.0002
}
