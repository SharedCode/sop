package main

import (
	"net/http"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/embed"
)

// GetConfiguredEmbedder returns an AI embedder based on the server configuration.
func GetConfiguredEmbedder(r *http.Request) ai.Embeddings {
	if !config.ProductionMode {
		return embed.NewSimple("mock_embedder", 384, nil)
	}

	provider := config.EmbedderProvider
	apiKey := config.EmbedderAPIKey
	url := config.EmbedderURL
	model := config.EmbedderModel

	if r != nil {
		if h := r.Header.Get("X-Embedder-Provider"); h != "" {
			provider = h
		}
		if h := r.Header.Get("X-Embedder-API-Key"); h != "" {
			apiKey = h
		}
		if h := r.Header.Get("X-Embedder-URL"); h != "" {
			url = h
		}
		if h := r.Header.Get("X-Embedder-Model"); h != "" {
			model = h
		}
	}

	if provider == "" {
		if config.OllamaEmbedderURL != "" {
			provider = "ollama"
		} else {
			provider = "gemini"
		}
	}

	switch provider {
	case "gemini":
		if apiKey == "" {
			apiKey = config.LLMApiKey
		}
		if model == "" {
			model = "gemini-embedding-2"
		}
		return embed.NewGemini(apiKey, model)
	case "ollama":
		if url == "" {
			url = config.OllamaEmbedderURL
		}
		if model == "" {
			model = "nomic-embed-text"
		}
		return embed.NewOllama(url, model)
	default:
		return embed.NewSimple("mock_embedder", 384, nil)
	}
}
