package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// GeminiEmbedder connects to the Google Generative Language API to generate semantic embeddings.
type GeminiEmbedder struct {
	apiKey string
	model  string
}

// NewGemini creates a new Google Gemini embedder.
func NewGemini(apiKey, model string) *GeminiEmbedder {
	if apiKey == "" {
		apiKey = os.Getenv("GEMINI_API_KEY")
	}
	if model == "" {
		model = "gemini-embedding-001" // Typically 768 dimensions
	}
	return &GeminiEmbedder{apiKey: apiKey, model: model}
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
	Model   string        `json:"model"`
	Content geminiContent `json:"content"`
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

	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:batchEmbedContents?key=%s", e.model, e.apiKey)

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
				Model: fmt.Sprintf("models/%s", e.model),
				Content: geminiContent{
					Parts: []geminiPart{{Text: text}},
				},
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
				resp.Body.Close()
				fmt.Println("\n[Gemini Embedder] Rate limit 429 reached. Sleeping 30 seconds...")
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
