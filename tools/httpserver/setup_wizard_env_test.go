package main

import "testing"

func TestReadSetupWizardAIConfigFromEnv(t *testing.T) {
	t.Run("no api keys from env", func(t *testing.T) {
		cfg := readSetupWizardAIConfigFromEnv()

		// API keys are no longer read from environment variables
		if cfg.BrainProvider != "" {
			t.Fatalf("expected no provider from env, got %q", cfg.BrainProvider)
		}
		if cfg.BrainAPIKey != "" {
			t.Fatalf("expected no api key from env, got %q", cfg.BrainAPIKey)
		}
	})

	t.Run("ollama host detection still works", func(t *testing.T) {
		t.Setenv("OLLAMA_HOST", "http://localhost:11434")

		cfg := readSetupWizardAIConfigFromEnv()

		if cfg.BrainProvider != "ollama" {
			t.Fatalf("expected ollama provider, got %q", cfg.BrainProvider)
		}
		if cfg.BrainURL != "http://localhost:11434" {
			t.Fatalf("expected ollama url, got %q", cfg.BrainURL)
		}
	})
}
