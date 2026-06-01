package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sharedcode/sop/ai"
)

type llmConnectionTestRequest struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
	APIKey   string `json:"api_key"`
	URL      string `json:"url"`
}

type embedderConnectionTestRequest struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
	APIKey   string `json:"api_key"`
	URL      string `json:"url"`
}

const defaultAIConnectionTestTimeout = 45 * time.Second

func aiConnectionTestTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv("SOP_AI_CONNECTION_TEST_TIMEOUT"))
	if raw == "" {
		return defaultAIConnectionTestTimeout
	}
	timeout, err := time.ParseDuration(raw)
	if err != nil || timeout <= 0 {
		return defaultAIConnectionTestTimeout
	}
	return timeout
}

func handleTestLLMConnection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req llmConnectionTestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	settings := llmSettings{
		Provider: strings.TrimSpace(req.Provider),
		Model:    strings.TrimSpace(req.Model),
		APIKey:   strings.TrimSpace(req.APIKey),
		URL:      strings.TrimSpace(req.URL),
	}
	if settings.Provider == "" {
		http.Error(w, "Provider is required", http.StatusBadRequest)
		return
	}

	gen, err := newConfiguredLLM(settings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), aiConnectionTestTimeout())
	defer cancel()

	output, err := gen.Generate(ctx, "Reply with OK only.", ai.GenOptions{MaxTokens: 16})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"ok":       true,
		"provider": settings.Provider,
		"model":    settings.Model,
		"response": strings.TrimSpace(output.Text),
	})
}

func handleTestEmbedderConnection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req embedderConnectionTestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	settings := embedderSettings{
		Provider: strings.TrimSpace(req.Provider),
		Model:    strings.TrimSpace(req.Model),
		APIKey:   strings.TrimSpace(req.APIKey),
		URL:      strings.TrimSpace(req.URL),
	}
	if settings.Provider == "" {
		http.Error(w, "Provider is required", http.StatusBadRequest)
		return
	}

	embedder, err := newConfiguredEmbedder(settings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), aiConnectionTestTimeout())
	defer cancel()

	vectors, err := embedder.EmbedTexts(ctx, []string{"connection test"})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	dimension := 0
	if len(vectors) > 0 {
		dimension = len(vectors[0])
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"ok":        true,
		"provider":  settings.Provider,
		"model":     settings.Model,
		"dimension": dimension,
	})
}
