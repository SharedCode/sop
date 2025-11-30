package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// OllamaEmbedder uses a local Ollama instance to generate semantic embeddings.
type OllamaEmbedder struct {
	baseURL string
	model   string
}

// NewOllama creates a new Ollama embedder.
func NewOllama(baseURL, model string) *OllamaEmbedder {
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}
	if model == "" {
		model = "nomic-embed-text" // A good default embedding model
	}
	return &OllamaEmbedder{baseURL: baseURL, model: model}
}

func (e *OllamaEmbedder) Name() string { return "ollama-" + e.model }
func (e *OllamaEmbedder) Dim() int     { return 768 } // Typical for nomic-embed-text/bert, but dynamic check is better

type ollamaEmbedRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type ollamaEmbedResponse struct {
	Embedding []float64 `json:"embedding"`
}

// EmbedTexts generates embeddings for the given texts using the Ollama API.
func (e *OllamaEmbedder) EmbedTexts(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	url := fmt.Sprintf("%s/api/embeddings", e.baseURL)

	for i, text := range texts {
		reqBody := ollamaEmbedRequest{
			Model:  e.model,
			Prompt: text,
		}

		jsonBody, err := json.Marshal(reqBody)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("ollama api request failed: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("ollama api error (status %d): %s", resp.StatusCode, string(body))
		}

		var embedResp ollamaEmbedResponse
		if err := json.NewDecoder(resp.Body).Decode(&embedResp); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		// Convert float64 to float32
		vec := make([]float32, len(embedResp.Embedding))
		for j, v := range embedResp.Embedding {
			vec[j] = float32(v)
		}
		out[i] = vec
	}

	return out, nil
}
