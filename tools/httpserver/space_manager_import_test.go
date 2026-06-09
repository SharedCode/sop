package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/memory"
)

type stubConfigSyncKB struct {
	cfg      *memory.KnowledgeBaseConfig
	setCalls int
}

func (s *stubConfigSyncKB) GetConfig(context.Context) (*memory.KnowledgeBaseConfig, error) {
	return s.cfg, nil
}

func (s *stubConfigSyncKB) SetConfig(_ context.Context, cfg *memory.KnowledgeBaseConfig) error {
	s.cfg = cfg
	s.setCalls++
	return nil
}

type stubEmbedder struct {
	name string
	dim  int
}

func (s stubEmbedder) Name() string { return s.name }
func (s stubEmbedder) Dim() int     { return s.dim }
func (s stubEmbedder) EmbedTexts(context.Context, []string) ([][]float32, error) {
	return nil, nil
}

func TestSyncKnowledgeBaseEmbedderConfig_UpdatesConfigWhenNeeded(t *testing.T) {
	kb := &stubConfigSyncKB{cfg: &memory.KnowledgeBaseConfig{Embedder: "old", EmbedderDimension: 1}}
	if err := syncKnowledgeBaseEmbedderConfig(context.Background(), kb, stubEmbedder{name: "new", dim: 768}); err != nil {
		t.Fatalf("syncKnowledgeBaseEmbedderConfig() error = %v", err)
	}
	if kb.cfg.Embedder != "new" {
		t.Fatalf("expected embedder to be updated to %q, got %q", "new", kb.cfg.Embedder)
	}
	if kb.cfg.EmbedderDimension != 768 {
		t.Fatalf("expected embedder dimension to be updated to %d, got %d", 768, kb.cfg.EmbedderDimension)
	}
	if kb.setCalls != 1 {
		t.Fatalf("expected one config update call, got %d", kb.setCalls)
	}
}

func TestSyncKnowledgeBaseEmbedderConfig_SkipsWhenUnchanged(t *testing.T) {
	kb := &stubConfigSyncKB{cfg: &memory.KnowledgeBaseConfig{Embedder: "same", EmbedderDimension: 768}}
	if err := syncKnowledgeBaseEmbedderConfig(context.Background(), kb, stubEmbedder{name: "same", dim: 768}); err != nil {
		t.Fatalf("syncKnowledgeBaseEmbedderConfig() error = %v", err)
	}
	if kb.setCalls != 0 {
		t.Fatalf("expected no config update calls when values already match, got %d", kb.setCalls)
	}
}

var _ ai.Embeddings = stubEmbedder{}

func TestRunIngestSpace_PropagatesDecodeError(t *testing.T) {
	oldConfig := config
	defer func() { config = oldConfig }()

	config = Config{
		Databases: []DatabaseConfig{{Name: "testdb", Path: t.TempDir(), Mode: "standalone"}},
	}

	request := IngestSpaceRequest{
		DatabaseName: "testdb",
		SpaceName:    "medical",
		CustomData:   json.RawMessage(`{"items":[{"id":"broken"`),
	}

	err := runIngestSpace(context.Background(), request, nil, &MockGenerator{}, nil, nil)
	if err == nil {
		t.Fatal("expected ingest helper to return decode error")
	}
	if !strings.Contains(err.Error(), "failed to decode Space item") {
		t.Fatalf("expected decode error, got %v", err)
	}
}
