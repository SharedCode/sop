package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestShouldUseImportPathForPreload(t *testing.T) {
	tests := []struct {
		name        string
		templateID  string
		preloadPath string
		want        bool
	}{
		{name: "sop uses import path", templateID: "SOP", preloadPath: "ai/sop_base_knowledge.json", want: true},
		{name: "medical stays on ingest path", templateID: "medical", preloadPath: "medical.json", want: false},
		{name: "medical kb alias stays on ingest path", templateID: "Medical KB", preloadPath: "medical.json", want: false},
		{name: "custom template stays on ingest path", templateID: "custom", preloadPath: "custom.json", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldUseImportPathForPreload(tt.templateID, tt.preloadPath); got != tt.want {
				t.Fatalf("shouldUseImportPathForPreload(%q, %q) = %v, want %v", tt.templateID, tt.preloadPath, got, tt.want)
			}
		})
	}
}

func TestShouldForceLocalBuiltinEmbedderForInternalSpaces(t *testing.T) {
	if !shouldForceLocalBuiltinEmbedder("SOP", "ai/sop_base_knowledge.json") {
		t.Fatal("expected SOP to force the built-in local embedder")
	}
	if !shouldForceLocalBuiltinEmbedder("Medical", "medical.json") {
		t.Fatal("expected Medical KB to force the built-in local embedder")
	}
}

func TestShouldForceLocalBuiltinEmbedderSkipsCustomSpaces(t *testing.T) {
	if shouldForceLocalBuiltinEmbedder("Custom", "custom.json") {
		t.Fatal("expected custom spaces to keep the configured embedder")
	}
}

func TestBuildImportSpaceRequestFromPreloadUsesMultipartPayload(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "space_export.json")
	if err := os.WriteFile(filePath, []byte(`{"items":[]}`), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	req, err := buildImportSpaceRequest(context.Background(), filePath, "test-db", "SOP")
	if err != nil {
		t.Fatalf("buildImportSpaceRequest returned error: %v", err)
	}

	if req.Method != http.MethodPost {
		t.Fatalf("expected POST method, got %q", req.Method)
	}

	if err := req.ParseMultipartForm(32 << 20); err != nil {
		t.Fatalf("ParseMultipartForm returned error: %v", err)
	}

	if got := req.FormValue("database"); got != "test-db" {
		t.Fatalf("expected database form value %q, got %q", "test-db", got)
	}
	if got := req.FormValue("name"); got != "SOP" {
		t.Fatalf("expected name form value %q, got %q", "SOP", got)
	}

	fh, header, err := req.FormFile("file")
	if err != nil {
		t.Fatalf("FormFile returned error: %v", err)
	}
	defer fh.Close()
	if got := header.Filename; got != filepath.Base(filePath) {
		t.Fatalf("expected uploaded filename %q, got %q", filepath.Base(filePath), got)
	}
}
