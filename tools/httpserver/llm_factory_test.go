package main

import "testing"

func TestResolveLLMSettingsDefaultsToPreferredProviders(t *testing.T) {
	config = Config{}

	settings := resolveLLMSettings(nil)

	if settings.Provider != "openai" {
		t.Fatalf("expected default provider %q, got %q", "openai", settings.Provider)
	}

	if settings.Model != "gpt-5.4" {
		t.Fatalf("expected default model %q, got %q", "gpt-5.4", settings.Model)
	}
}

func TestResolveEmbedderSettingsDefaultsToKelindar(t *testing.T) {
	config = Config{}

	settings := resolveEmbedderSettings(nil)

	if settings.Provider != "local" {
		t.Fatalf("expected default embedder provider %q, got %q", "local", settings.Provider)
	}

	if settings.Model != "kelindar" {
		t.Fatalf("expected default embedder model %q, got %q", "kelindar", settings.Model)
	}
}
