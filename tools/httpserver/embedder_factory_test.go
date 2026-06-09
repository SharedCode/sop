package main

import (
	"context"
	"testing"

	"github.com/sharedcode/sop/ai/embed"
)

func TestShouldForceLocalBuiltinEmbedder(t *testing.T) {
	tests := []struct {
		name        string
		spaceName   string
		preloadPath string
		want        bool
	}{
		{name: "sop preload uses local override", spaceName: "SOP", preloadPath: "ai/sop_base_knowledge.json", want: true},
		{name: "medical preload uses local override", spaceName: "medical", preloadPath: "medical.json", want: true},
		{name: "custom preload path does not override", spaceName: "SOP", preloadPath: "custom.json", want: false},
		{name: "empty preload path still uses local override for built-in SOP space", spaceName: "SOP KB", preloadPath: "", want: true},
		{name: "empty preload path still uses local override for built-in medical space", spaceName: "Medical", preloadPath: "", want: true},
		{name: "custom space keeps default path", spaceName: "customer-space", preloadPath: "custom.json", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldForceLocalBuiltinEmbedder(tt.spaceName, tt.preloadPath); got != tt.want {
				t.Fatalf("shouldForceLocalBuiltinEmbedder(%q, %q) = %v, want %v", tt.spaceName, tt.preloadPath, got, tt.want)
			}
		})
	}
}

func TestResolveEmbedderSettingsDefaultsToLocalEmbedder(t *testing.T) {
	oldCfg := config
	config = Config{}
	defer func() { config = oldCfg }()

	settings := resolveEmbedderSettings(nil)
	if settings.Provider != "local" {
		t.Fatalf("expected local embedder to be the default provider, got %q", settings.Provider)
	}
}

func TestGetConfiguredEmbedderForSpaceUsesRealEmbedderForBuiltinSpaces(t *testing.T) {
	oldCfg := config
	config = Config{ProductionMode: false}
	defer func() { config = oldCfg }()

	embedder := GetConfiguredEmbedderForSpace(nil, "SOP", "")
	if embedder == nil {
		t.Fatal("expected a built-in embedder instance, got nil")
	}

	if _, ok := embedder.(*embed.Simple); ok {
		t.Fatal("expected built-in SOP vectorization to bypass the mock embedder fallback")
	}
}

func TestPinnedInternalKBSettings(t *testing.T) {
	embedder, readOnly := pinnedInternalKBSettings("Medical")
	if !readOnly {
		t.Fatal("expected Medical to be pinned read-only")
	}
	if embedder != "kelindar:nomic-embed-text-v1.5-q8_0" {
		t.Fatalf("expected pinned internal KB embedder to be %q, got %q", "kelindar:nomic-embed-text-v1.5-q8_0", embedder)
	}

	embedder, readOnly = pinnedInternalKBSettings("Custom")
	if readOnly {
		t.Fatal("expected non-internal KB to remain editable")
	}
	if embedder != "" {
		t.Fatalf("expected non-internal KB embedder override to be empty, got %q", embedder)
	}
}

func TestNormalizeProviderAndModelTreatsKelindarAsLocal(t *testing.T) {
	provider, model := normalizeProviderAndModel("kelindar", "nomic-embed-text-v1.5-q8_0")
	if provider != "local" {
		t.Fatalf("expected legacy kelindar provider to normalize to local, got %q", provider)
	}
	if model != "nomic-embed-text-v1.5-q8_0" {
		t.Fatalf("expected model path to remain intact, got %q", model)
	}
}

func TestNewConfiguredEmbedderSupportsLegacyKelindarProvider(t *testing.T) {
	embedder, err := newConfiguredEmbedder(embedderSettings{Provider: "kelindar", Model: "kelindar"})
	if err != nil {
		t.Fatalf("expected legacy kelindar provider to initialize, got error: %v", err)
	}
	if embedder == nil {
		t.Fatal("expected a non-nil embedder for the legacy kelindar provider")
	}
}

func TestConfiguredLocalKelindarEmbedderGeneratesVectors(t *testing.T) {
	oldCfg := config
	config = Config{ProductionMode: true}
	defer func() { config = oldCfg }()

	embedder, err := newConfiguredEmbedder(embedderSettings{Provider: "local", Model: "kelindar", URL: "kelindar"})
	if err != nil {
		t.Fatalf("newConfiguredEmbedder returned an unexpected error: %v", err)
	}

	vectors, err := embedder.EmbedTexts(context.Background(), []string{"hello kelindar"})
	if err != nil {
		t.Fatalf("EmbedTexts returned an unexpected error: %v", err)
	}
	if len(vectors) != 1 {
		t.Fatalf("expected 1 vector, got %d", len(vectors))
	}
	if len(vectors[0]) == 0 {
		t.Fatal("expected kelindar embedder to generate a non-empty vector")
	}
}
