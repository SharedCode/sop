package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// OpenAIEmbedder uses the OpenAI embeddings API or a compatible relay.
type OpenAIEmbedder struct {
	apiKey  string
	model   string
	baseURL string
}

// NewOpenAI creates a new OpenAI-compatible embedder.
func NewOpenAI(apiKey, model, baseURL string) *OpenAIEmbedder {
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	if model == "" {
		model = "text-embedding-3-small"
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = os.Getenv("OPENAI_API_BASE_URL")
	}
	return &OpenAIEmbedder{
		apiKey:  apiKey,
		model:   model,
		baseURL: strings.TrimSpace(baseURL),
	}
}

func (e *OpenAIEmbedder) Name() string { return "openai-" + e.model }

func (e *OpenAIEmbedder) Dim() int {
	if strings.Contains(strings.ToLower(e.model), "large") {
		return 3072
	}
	return 1536
}

func (e *OpenAIEmbedder) embeddingsURL() string {
	if strings.TrimSpace(e.baseURL) == "" {
		return "https://api.openai.com/v1/embeddings"
	}
	return strings.TrimRight(strings.TrimSpace(e.baseURL), "/") + "/embeddings"
}

type openAIEmbeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type openAIEmbeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type,omitempty"`
	} `json:"error,omitempty"`
}

// EmbedTexts generates embeddings using the OpenAI embeddings API.
func (e *OpenAIEmbedder) EmbedTexts(ctx context.Context, texts []string) ([][]float32, error) {
	if e.apiKey == "" {
		return nil, fmt.Errorf("openai api key is missing")
	}
	if len(texts) == 0 {
		return nil, nil
	}

	requestBody := openAIEmbeddingRequest{
		Model: e.model,
		Input: make([]string, len(texts)),
	}
	copy(requestBody.Input, texts)
	for index, text := range requestBody.Input {
		if text == "" {
			requestBody.Input[index] = " "
		}
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.embeddingsURL(), bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai embeddings api request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotImplemented {
			lowerBody := strings.ToLower(string(body))
			if strings.Contains(lowerBody, "embedding") || strings.Contains(lowerBody, "embeddings_not_supported") {
				return nil, fmt.Errorf("openai-compatible endpoint does not support embeddings; choose a different embedder provider or base URL")
			}
		}
		return nil, fmt.Errorf("openai embeddings api error (status %d): %s", resp.StatusCode, string(body))
	}

	var embedResp openAIEmbeddingResponse
	if err := json.Unmarshal(body, &embedResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	if embedResp.Error != nil {
		return nil, fmt.Errorf("openai embeddings api returned error: %s", embedResp.Error.Message)
	}
	if len(embedResp.Data) != len(texts) {
		return nil, fmt.Errorf("openai embedding count mismatch: expected %d, got %d", len(texts), len(embedResp.Data))
	}

	out := make([][]float32, len(embedResp.Data))
	for index, item := range embedResp.Data {
		vector := make([]float32, len(item.Embedding))
		for j, value := range item.Embedding {
			vector[j] = float32(value)
		}
		out[index] = vector
	}

	return out, nil
}
