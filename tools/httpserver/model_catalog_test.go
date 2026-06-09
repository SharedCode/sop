package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadModelCatalogSeedsDefaultFile(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")
	modelCatalogPath := filepath.Join(tempDir, modelCatalogFilename)

	seededDefaults, err := loadModelCatalog(configPath)
	if err != nil {
		t.Fatalf("loadModelCatalog returned error: %v", err)
	}
	if !seededDefaults {
		t.Fatal("expected loadModelCatalog to seed default model catalog file")
	}
	if len(modelCatalog.LLM) == 0 {
		t.Fatal("expected default llm catalog entries")
	}
	if len(modelCatalog.Embedder) == 0 {
		t.Fatal("expected default embedder catalog entries")
	}
	if got := modelCatalog.Embedder[len(modelCatalog.Embedder)-1].Options[len(modelCatalog.Embedder[len(modelCatalog.Embedder)-1].Options)-1].Value; got != "kelindar:nomic-embed-text-v1.5-q8_0" {
		t.Fatalf("expected default embedder catalog to include the registry-backed local kelindar option, got %q", got)
	}
	if modelCatalog.LLM[0].Options[0].Value != "gemini:gemini-3.1-pro-preview" {
		t.Fatalf("unexpected first default llm option: %q", modelCatalog.LLM[0].Options[0].Value)
	}
	data, err := os.ReadFile(modelCatalogPath)
	if err != nil {
		t.Fatalf("expected seeded model catalog file: %v", err)
	}
	var persisted ModelCatalog
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal seeded model catalog: %v", err)
	}
	if len(persisted.LLM) == 0 || persisted.LLM[0].Options[0].Value != "gemini:gemini-3.1-pro-preview" {
		t.Fatal("expected seeded file to contain default llm catalog")
	}
}

func TestLoadModelCatalogPreservesConfiguredFile(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")
	modelCatalogPath := filepath.Join(tempDir, modelCatalogFilename)

	contents := `{
		"llm": [{"label": "Vendor", "options": [{"value": "vendor:model-a", "label": "Model A"}]}],
		"embedder": [{"label": "Vendor", "options": [{"value": "vendor:embed-a", "label": "Embed A"}]}]
	}`
	if err := os.WriteFile(modelCatalogPath, []byte(contents), 0o644); err != nil {
		t.Fatalf("write model catalog: %v", err)
	}

	seededDefaults, err := loadModelCatalog(configPath)
	if err != nil {
		t.Fatalf("loadModelCatalog returned error: %v", err)
	}
	if seededDefaults {
		t.Fatal("expected configured model catalog to remain source of truth")
	}
	if got := modelCatalog.LLM[0].Options[0].Value; got != "vendor:model-a" {
		t.Fatalf("expected configured llm option, got %q", got)
	}
	if got := modelCatalog.Embedder[0].Options[0].Value; got != "vendor:embed-a" {
		t.Fatalf("expected configured embedder option, got %q", got)
	}
}

func TestSaveConfigFileSeedsModelCatalogSidecar(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")
	modelCatalogPath := filepath.Join(tempDir, modelCatalogFilename)

	config = Config{ConfigFile: configPath, Port: 8080}
	modelCatalog = defaultModelCatalog()

	saveConfigFile()

	if _, err := os.Stat(modelCatalogPath); err != nil {
		t.Fatalf("expected model catalog sidecar to be created: %v", err)
	}
	data, err := os.ReadFile(modelCatalogPath)
	if err != nil {
		t.Fatalf("read model catalog sidecar: %v", err)
	}
	var persisted ModelCatalog
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal model catalog sidecar: %v", err)
	}
	if len(persisted.LLM) == 0 || persisted.LLM[0].Options[0].Value != "gemini:gemini-3.1-pro-preview" {
		t.Fatal("expected sidecar to contain default llm catalog")
	}
}

func TestLoadModelCatalogFallsBackToParentOfWorkingDirectory(t *testing.T) {
	tempDir := t.TempDir()
	toolsDir := filepath.Join(tempDir, "tools")
	if err := os.MkdirAll(toolsDir, 0o755); err != nil {
		t.Fatalf("mkdir tools dir: %v", err)
	}

	catalogPath := filepath.Join(tempDir, modelCatalogFilename)
	contents := `{
		"llm": [{"label": "Vendor", "options": [{"value": "vendor:model-parent", "label": "Model Parent"}]}],
		"embedder": [{"label": "Vendor", "options": [{"value": "vendor:embed-parent", "label": "Embed Parent"}]}]
	}`
	if err := os.WriteFile(catalogPath, []byte(contents), 0o644); err != nil {
		t.Fatalf("write model catalog: %v", err)
	}

	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer os.Chdir(originalWD)

	if err := os.Chdir(toolsDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	seededDefaults, err := loadModelCatalog("")
	if err != nil {
		t.Fatalf("loadModelCatalog returned error: %v", err)
	}
	if seededDefaults {
		t.Fatal("expected existing parent catalog to remain source of truth")
	}
	if got := modelCatalog.LLM[0].Options[0].Value; got != "vendor:model-parent" {
		t.Fatalf("expected parent catalog llm option, got %q", got)
	}
	if got := modelCatalog.Embedder[0].Options[0].Value; got != "vendor:embed-parent" {
		t.Fatalf("expected parent catalog embedder option, got %q", got)
	}
}
