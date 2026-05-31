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
		t.Setenv("LLM_API_KEY", "")

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

	t.Run("gemini falls back to legacy llm api key", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "")
		t.Setenv("OPENAI_API_BASE_URL", "")
		t.Setenv("OPENAI_MODEL", "")
		t.Setenv("ANTHROPIC_API_KEY", "")
		t.Setenv("ANTHROPIC_API_BASE_URL", "")
		t.Setenv("ANTHROPIC_MODEL", "")
		t.Setenv("GEMINI_API_KEY", "")
		t.Setenv("GEMINI_API_BASE_URL", "http://127.0.0.1:3030/v1beta")
		t.Setenv("GEMINI_MODEL", "gemini-3.5-flash")
		t.Setenv("LLM_API_KEY", "legacy-gemini-key")

		cfg := readSetupWizardAIConfigFromEnv()

		if cfg.BrainProvider != "gemini" {
			t.Fatalf("expected gemini provider, got %q", cfg.BrainProvider)
		}
		if cfg.BrainAPIKey != "legacy-gemini-key" {
			t.Fatalf("expected llm api key fallback, got %q", cfg.BrainAPIKey)
		}
		if cfg.BrainURL != "http://127.0.0.1:3030/v1beta" {
			t.Fatalf("expected gemini base url, got %q", cfg.BrainURL)
		}
		if cfg.BrainModel != "gemini-3.5-flash" {
			t.Fatalf("expected gemini model, got %q", cfg.BrainModel)
		}
	})

	t.Run("gemini falls back to documented sop llm api key", func(t *testing.T) {
		t.Setenv("OPENAI_API_KEY", "")
		t.Setenv("OPENAI_API_BASE_URL", "")
		t.Setenv("OPENAI_MODEL", "")
		t.Setenv("ANTHROPIC_API_KEY", "")
		t.Setenv("ANTHROPIC_API_BASE_URL", "")
		t.Setenv("ANTHROPIC_MODEL", "")
		t.Setenv("GEMINI_API_KEY", "")
		t.Setenv("GEMINI_API_BASE_URL", "")
		t.Setenv("GEMINI_MODEL", "gemini-3.5-flash")
		t.Setenv("LLM_API_KEY", "")
		t.Setenv("SOP_LLM_API_KEY", "documented-key")

		cfg := readSetupWizardAIConfigFromEnv()

		if cfg.BrainProvider != "gemini" {
			t.Fatalf("expected gemini provider, got %q", cfg.BrainProvider)
		}
		if cfg.BrainAPIKey != "documented-key" {
			t.Fatalf("expected sop llm api key fallback, got %q", cfg.BrainAPIKey)
		}
		if cfg.BrainModel != "gemini-3.5-flash" {
			t.Fatalf("expected gemini model, got %q", cfg.BrainModel)
		}
	})
}
