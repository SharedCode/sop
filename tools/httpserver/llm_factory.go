package main

import (
	"context"
	"net/http"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/generator"
)

// GetConfiguredLLM returns an AI Generator based on the server configuration.
func GetConfiguredLLM(r *http.Request) ai.Generator {
	if !config.ProductionMode {
		mockGen, _ := generator.New("perceptron", nil)
		return mockGen
	}

	provider := config.BrainProvider
	apiKey := config.BrainAPIKey
	url := config.BrainURL
	model := config.BrainModel

	if r != nil {
		if h := r.Header.Get("X-LLM-Provider"); h != "" {
			provider = h
		}
		if h := r.Header.Get("X-LLM-API-Key"); h != "" {
			apiKey = h
		}
		if h := r.Header.Get("X-LLM-URL"); h != "" {
			url = h
		}
		if h := r.Header.Get("X-LLM-Model"); h != "" {
			model = h
		}
	}

	if provider == "" {
		provider = "gemini"
	}

	options := make(map[string]any)
	switch provider {
	case "gemini":
		options["apiKey"] = apiKey
		if options["apiKey"] == "" {
			options["apiKey"] = config.LLMApiKey
		}
		options["model"] = model
	case "ollama":
		options["baseURL"] = url
		if options["baseURL"] == "" {
			options["baseURL"] = config.BrainURL
		}
		options["model"] = model
	case "chatgpt":
		options["apiKey"] = apiKey
		if options["apiKey"] == "" {
			options["apiKey"] = config.LLMApiKey
		}
		options["model"] = model
	}

	gen, err := generator.New(provider, options)
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
