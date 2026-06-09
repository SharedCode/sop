package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/generator"
)

type llmSettings struct {
	Provider string
	APIKey   string
	URL      string
	Model    string
}

func resolveLLMSettings(r *http.Request) llmSettings {
	settings := llmSettings{
		Provider: firstNonEmpty(config.BrainProvider, "openai"),
		APIKey:   config.BrainAPIKey,
		URL:      config.BrainURL,
		Model:    firstNonEmpty(config.BrainModel, "gpt-5.4"),
	}

	if r != nil {
		if h := r.Header.Get("X-LLM-Provider"); h != "" {
			settings.Provider = h
		}
		if h := r.Header.Get("X-LLM-API-Key"); h != "" {
			settings.APIKey = h
		}
		if h := r.Header.Get("X-LLM-URL"); h != "" {
			settings.URL = h
		}
		if h := r.Header.Get("X-LLM-Model"); h != "" {
			settings.Model = h
		}
	}

	return settings
}

func newConfiguredLLM(settings llmSettings) (ai.Generator, error) {
	provider := settings.Provider
	options := make(map[string]any)

	switch provider {
	case "gemini":
		options["api_key"] = settings.APIKey
		if settings.APIKey == "" {
			options["api_key"] = config.LLMApiKey
		}
		options["model"] = settings.Model
		if settings.URL != "" {
			options["api_url"] = settings.URL
		}
	case "ollama":
		options["base_url"] = settings.URL
		if settings.URL == "" {
			options["base_url"] = config.BrainURL
		}
		options["model"] = settings.Model
	case "chatgpt", "openai":
		provider = "chatgpt"
		options["api_key"] = settings.APIKey
		if settings.APIKey == "" {
			options["api_key"] = config.LLMApiKey
		}
		options["model"] = settings.Model
		if settings.URL != "" {
			options["api_url"] = settings.URL
		}
	case "anthropic":
		options["api_key"] = settings.APIKey
		if settings.APIKey == "" {
			options["api_key"] = config.LLMApiKey
		}
		options["model"] = settings.Model
	default:
		return nil, fmt.Errorf("unsupported provider %q", provider)
	}

	return generator.New(provider, options)
}

// GetConfiguredLLM returns an AI Generator based on the server configuration.
// It checks the HTTP request headers first, then falls back to server config defaults.
func GetConfiguredLLM(r *http.Request) ai.Generator {
	if !config.ProductionMode {
		mockGen, _ := generator.New("perceptron", nil)
		return mockGen
	}

	gen, err := newConfiguredLLM(resolveLLMSettings(r))
	if err != nil {
		mockGen, _ := generator.New("perceptron", nil)
		return mockGen
	}

	return gen
}

// summarizerLLMClient is an adapter that makes ai.Generator implement LLMClient
type summarizerLLMClient struct {
	gen ai.Generator
}

func (c *summarizerLLMClient) Generate(prompt string) (string, error) {
	out, err := c.gen.Generate(context.Background(), prompt, ai.GenOptions{})
	if err != nil {
		return "", err
	}
	return out.Text, nil
}

// GetConfiguredLLMClient returns an LLMClient for the Summarizer
func GetConfiguredLLMClient(r *http.Request) LLMClient {
	gen := GetConfiguredLLM(r)
	if gen == nil {
		return nil
	}
	return &summarizerLLMClient{gen: gen}
}
