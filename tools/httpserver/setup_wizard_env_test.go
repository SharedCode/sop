package main

import "testing"

func TestReadSetupWizardAIConfigFromEnv(t *testing.T) {
	t.Run("provider specific openai env vars", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "openai-key")
		t.Setenv("OPENAI_API_BASE_URL", "http://127.0.0.1:3030/v1")
		t.Setenv("OPENAI_MODEL", "gpt-5.4")
		t.Setenv("GEMINI_API_KEY", "")
		t.Setenv("GEMINI_MODEL", "")
		t.Setenv("GEMINI_API_BASE_URL", "")
		t.Setenv("ANTHROPIC_API_KEY", "")
		t.Setenv("ANTHROPIC_MODEL", "")
		t.Setenv("ANTHROPIC_API_BASE_URL", "")

		cfg := readSetupWizardAIConfigFromEnv()

		if cfg.BrainProvider != "openai" {
			t.Fatalf("expected openai provider, got %q", cfg.BrainProvider)
		}
		if cfg.BrainAPIKey != "openai-key" {
			t.Fatalf("expected openai api key, got %q", cfg.BrainAPIKey)
		}
		if cfg.BrainURL != "http://127.0.0.1:3030/v1" {
			t.Fatalf("expected openai base url, got %q", cfg.BrainURL)
		}
		if cfg.BrainModel != "gpt-5.4" {
			t.Fatalf("expected openai model, got %q", cfg.BrainModel)
		}
	})

	t.Run("all configured providers are exposed as env options", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "openai-key")
		t.Setenv("OPENAI_API_BASE_URL", "http://127.0.0.1:3030/v1")
		t.Setenv("OPENAI_MODEL", "gpt-5.4")
		t.Setenv("ANTHROPIC_API_KEY", "anthropic-key")
		t.Setenv("ANTHROPIC_API_BASE_URL", "http://127.0.0.1:3030/v1")
		t.Setenv("ANTHROPIC_MODEL", "claude-code-sonnet-4.6")
		t.Setenv("GEMINI_API_KEY", "gemini-key")
		t.Setenv("GEMINI_API_BASE_URL", "http://127.0.0.1:3030/v1")
		t.Setenv("GEMINI_MODEL", "gemini-3.5-flash")

		cfg := readSetupWizardAIConfigFromEnv()

		if cfg.BrainProvider != "gemini" {
			t.Fatalf("expected default env provider to remain gemini, got %q", cfg.BrainProvider)
		}
		if len(cfg.EnvProviders) != 3 {
			t.Fatalf("expected 3 env providers, got %d", len(cfg.EnvProviders))
		}

		want := []struct {
			provider string
			model    string
		}{
			{provider: "gemini", model: "gemini-3.5-flash"},
			{provider: "openai", model: "gpt-5.4"},
			{provider: "anthropic", model: "claude-code-sonnet-4.6"},
		}

		for i, item := range want {
			if cfg.EnvProviders[i].Provider != item.provider {
				t.Fatalf("expected env provider %d to be %q, got %q", i, item.provider, cfg.EnvProviders[i].Provider)
			}
			if cfg.EnvProviders[i].Model != item.model {
				t.Fatalf("expected env model %d to be %q, got %q", i, item.model, cfg.EnvProviders[i].Model)
			}
		}
	})

	t.Run("provider defaults do not depend on generic llm env vars", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "")
		t.Setenv("OPENAI_API_BASE_URL", "")
		t.Setenv("OPENAI_MODEL", "")
		t.Setenv("ANTHROPIC_API_KEY", "")
		t.Setenv("ANTHROPIC_API_BASE_URL", "")
		t.Setenv("ANTHROPIC_MODEL", "")
		t.Setenv("GEMINI_API_KEY", "")
		t.Setenv("GEMINI_API_BASE_URL", "http://127.0.0.1:3030/v1beta")
		t.Setenv("GEMINI_MODEL", "gemini-3.5-flash")

		cfg := readSetupWizardAIConfigFromEnv()

		if cfg.BrainProvider != "gemini" {
			t.Fatalf("expected gemini provider from model/url detection, got %q", cfg.BrainProvider)
		}
		if cfg.BrainAPIKey != "" {
			t.Fatalf("expected no api key without provider-specific env vars, got %q", cfg.BrainAPIKey)
		}
		if cfg.BrainURL != "http://127.0.0.1:3030/v1beta" {
			t.Fatalf("expected gemini base url, got %q", cfg.BrainURL)
		}
		if cfg.BrainModel != "gemini-3.5-flash" {
			t.Fatalf("expected gemini model, got %q", cfg.BrainModel)
		}
	})
}
