package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSaveConfig_OmitsSensitiveAISelectionFields(t *testing.T) {
	defer func() {
		config = Config{}
	}()

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.json")
	config = Config{
		ConfigFile:       configFile,
		Port:             8080,
		LLMApiKey:        "legacy-secret",
		BrainProvider:    "chatgpt",
		BrainModel:       "gpt-5.4",
		BrainURL:         "https://example.test/brain",
		BrainAPIKey:      "brain-secret",
		EmbedderProvider: "gemini",
		EmbedderModel:    "gemini-embedding-2",
		EmbedderURL:      "https://example.test/embedder",
		EmbedderAPIKey:   "embedder-secret",
	}

	if err := saveConfig(); err != nil {
		t.Fatalf("saveConfig returned error: %v", err)
	}

	data, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatalf("read saved config: %v", err)
	}

	var saved map[string]any
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("unmarshal saved config: %v", err)
	}

	for _, key := range []string{
		"llm_api_key",
		"brain_provider", "brain_model", "brain_url", "brain_api_key",
		"embedder_provider", "embedder_model", "embedder_url", "embedder_api_key",
	} {
		if _, ok := saved[key]; ok {
			t.Fatalf("expected %q to be omitted from config.json, but it was present", key)
		}
	}

	if got := saved["port"]; got != float64(8080) {
		t.Fatalf("expected non-sensitive config fields to remain, got %v", got)
	}
}
