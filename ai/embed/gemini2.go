package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// GeminiEmbedder connects to the Google Generative Language API to generate semantic embeddings.
type GeminiEmbedder struct {
	apiKey               string
	model                string
	outputDimensionality int
}

// NewGemini creates a new Google Gemini embedder.
func NewGemini(apiKey, model string) *GeminiEmbedder {
	model = sanitizeModelName(model)
	outputDimensionality := 0
	if strings.Contains(strings.ToLower(model), "embedding-2") || strings.Contains(strings.ToLower(model), "embedding-001") {
		outputDimensionality = 768
	}
	return &GeminiEmbedder{apiKey: apiKey, model: model, outputDimensionality: outputDimensionality}
}

func sanitizeModelName(model string) string {
	model = strings.TrimSpace(model)
	lower := strings.ToLower(model)
	if strings.HasPrefix(lower, "models/") {
		model = lower[len("models/"):]
	}
	if model == "" {
		return "gemini-embedding-2"
	}
	return strings.ToLower(model)
}

func normalizedModelName(model string) string {
	return "models/" + sanitizeModelName(model)
}

func (e *GeminiEmbedder) Name() string { return "gemini-" + e.model }
func (e *GeminiEmbedder) Dim() int     { return 768 } // Gemini embedding is 768d

type geminiPart struct {
	Text string `json:"text"`
}

type geminiContent struct {
	Parts []geminiPart `json:"parts"`
}

type geminiContentRequest struct {
	Model                string        `json:"model"`
	Content              geminiContent `json:"content"`
	TaskType             string        `json:"taskType,omitempty"`
	OutputDimensionality int           `json:"outputDimensionality,omitempty"`
}

type geminiBatchEmbedRequest struct {
	Requests []geminiContentRequest `json:"requests"`
}

type geminiValue struct {
	Values []float64 `json:"values"`
}

type geminiBatchEmbedResponse struct {
	Embeddings []geminiValue `json:"embeddings"`
}

// EmbedTexts generates embeddings for a batch of strings via Google's Gemini API.
func (e *GeminiEmbedder) EmbedTexts(ctx context.Context, texts []string) ([][]float32, error) {
	if e.apiKey == "" {
		return nil, fmt.Errorf("gemini api key is missing")
	}

	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/%s:batchEmbedContents?key=%s", normalizedModelName(e.model), e.apiKey)

	out := make([][]float32, 0, len(texts))
	chunkSize := 100 // Gemini has a limit of 100 requests per batch
	for i := 0; i < len(texts); i += chunkSize {
		end := i + chunkSize
		if end > len(texts) {
			end = len(texts)
		}
		chunk := texts[i:end]

		// Prepare batch request payload
		reqs := make([]geminiContentRequest, len(chunk))
		for j, text := range chunk {
			if text == "" {
				text = " "
			}
			reqs[j] = geminiContentRequest{
				Model:                normalizedModelName(e.model),
				Content:              geminiContent{Parts: []geminiPart{{Text: text}}},
				TaskType:             "RETRIEVAL_DOCUMENT",
				OutputDimensionality: e.outputDimensionality,
			}
		}

		batchReq := geminiBatchEmbedRequest{Requests: reqs}
		jsonBody, err := json.Marshal(batchReq)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}

		var resp *http.Response
		for attempt := 0; attempt < 5; attempt++ {
			req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
			if err != nil {
				return nil, fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err = http.DefaultClient.Do(req)
			if err != nil {
				return nil, fmt.Errorf("gemini api request failed: %w", err)
			}

			if resp.StatusCode == 429 {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				fmt.Printf("\n[Gemini Embedder] Rate limit 429 reached. Response: %s\nSleeping 30 seconds...\n", string(body))
				time.Sleep(30 * time.Second)
				continue
			}
			break
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("gemini api error (status %d): %s", resp.StatusCode, string(body))
		}

		var embedResp geminiBatchEmbedResponse
		if err := json.NewDecoder(resp.Body).Decode(&embedResp); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		if len(embedResp.Embeddings) != len(chunk) {
			return nil, fmt.Errorf("gemini embedding count mismatch: expected %d, got %d", len(chunk), len(embedResp.Embeddings))
		}

		for _, emb := range embedResp.Embeddings {
			vec := make([]float32, len(emb.Values))
			for k, val := range emb.Values {
				vec[k] = float32(val)
			}
			out = append(out, vec)
		}
	}

	return out, nil
}
