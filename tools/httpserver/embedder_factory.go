package main

import (
	"fmt"
	"net/http"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/embed"
)

type embedderSettings struct {
	Provider string
	APIKey   string
	URL      string
	Model    string
}

func resolveEmbedderSettings(r *http.Request) embedderSettings {
	settings := embedderSettings{
		Provider: config.EmbedderProvider,
		APIKey:   config.EmbedderAPIKey,
		URL:      config.EmbedderURL,
		Model:    config.EmbedderModel,
	}

	if r != nil {
		if h := r.Header.Get("X-Embedder-Provider"); h != "" {
			settings.Provider = h
		}
		if h := r.Header.Get("X-Embedder-API-Key"); h != "" {
			settings.APIKey = h
		}
		if h := r.Header.Get("X-Embedder-URL"); h != "" {
			settings.URL = h
		}
		if h := r.Header.Get("X-Embedder-Model"); h != "" {
			settings.Model = h
		}
	}

	if settings.Provider == "" {
		if config.OllamaEmbedderURL != "" {
			settings.Provider = "ollama"
		} else {
			settings.Provider = "gemini"
		}
	}

	return settings
}

func newConfiguredEmbedder(settings embedderSettings) (ai.Embeddings, error) {
	switch settings.Provider {
	case "gemini":
		apiKey := settings.APIKey
		if apiKey == "" {
			apiKey = config.LLMApiKey
		}
		model := settings.Model
		if model == "" {
			model = "gemini-embedding-2"
		}
		return embed.NewGemini(apiKey, model), nil
	case "openai":
		apiKey := settings.APIKey
		if apiKey == "" {
			apiKey = config.EmbedderAPIKey
		}
		if apiKey == "" {
			apiKey = config.LLMApiKey
		}
		model := settings.Model
		if model == "" {
			model = "text-embedding-3-small"
		}
		return embed.NewOpenAI(apiKey, model, settings.URL), nil
	case "ollama":
		url := settings.URL
		if url == "" {
			url = config.OllamaEmbedderURL
		}
		model := settings.Model
		if model == "" {
			model = "nomic-embed-text"
		}
		return embed.NewOllama(url, model), nil
	case "simple":
		return embed.NewSimple("mock_embedder", 384, nil), nil
	default:
		return nil, fmt.Errorf("unsupported embedder provider %q", settings.Provider)
	}
}

// GetConfiguredEmbedder returns an AI embedder based on the server configuration.
func GetConfiguredEmbedder(r *http.Request) ai.Embeddings {
	if !config.ProductionMode {
		return embed.NewSimple("mock_embedder", 384, nil)
	}

	embedder, err := newConfiguredEmbedder(resolveEmbedderSettings(r))
	if err != nil {
		return embed.NewSimple("mock_embedder", 384, nil)
	}
	return embedder
}
