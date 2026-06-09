package embed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type stubVectorizer struct {
	closed bool
	texts  []string
	vec    []float32
	err    error
}

func (s *stubVectorizer) EmbedText(text string) ([]float32, error) {
	s.texts = append(s.texts, text)
	if s.err != nil {
		return nil, s.err
	}
	return append([]float32(nil), s.vec...), nil
}

func (s *stubVectorizer) Close() error {
	s.closed = true
	return nil
}

func TestLocalEmbedderEmbedTextsUsesInjectedModel(t *testing.T) {
	stub := &stubVectorizer{vec: []float32{1, 2, 3}}
	embedder := &Local{
		modelPath: "models/test.gguf",
		name:      "local-test.gguf",
		model:     stub,
	}

	vectors, err := embedder.EmbedTexts(context.Background(), []string{"alpha", "beta"})
	if err != nil {
		t.Fatalf("EmbedTexts returned an unexpected error: %v", err)
	}
	if len(vectors) != 2 {
		t.Fatalf("expected 2 vectors, got %d", len(vectors))
	}
	if got := embedder.Dim(); got != 3 {
		t.Fatalf("expected dim 3, got %d", got)
	}
	if len(stub.texts) != 2 || stub.texts[0] != "alpha" || stub.texts[1] != "beta" {
		t.Fatalf("unexpected texts passed to model: %+v", stub.texts)
	}
}

func TestLoadEmbeddingProfileUsesJSONContract(t *testing.T) {
	tmpDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmpDir, "models"), 0o755); err != nil {
		t.Fatal(err)
	}
	profileJSON := `{
	  "nomic-embed-text-v1.5-q8_0": {
	    "display_name": "Nomic Embed Text v1.5 (Q8)",
	    "max_context_tokens": 8192,
	    "supports_matryoshka": true,
	    "dimensions": {
	      "routing": 128,
	      "document": 256
	    },
	    "prefixes": {
	      "routing_search": "classification: ",
	      "doc_storage": "search_document: ",
	      "doc_search": "search_query: "
	    }
	  }
	}`
	if err := os.WriteFile(filepath.Join(tmpDir, "models", "embedder_profiles.json"), []byte(profileJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(oldWd) }()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}

	profile := loadEmbeddingProfile("models/nomic-embed-text-v1.5-q8_0.gguf")
	if profile.RoutingDim != 128 || profile.DocumentDim != 256 {
		t.Fatalf("expected JSON contract dimensions to be used, got %+v", profile)
	}
	if profile.RoutingPrefix != "classification: " || profile.DocStorePrefix != "search_document: " || profile.DocQueryPrefix != "search_query: " {
		t.Fatalf("expected JSON contract prefixes to be used, got %+v", profile)
	}
}

func TestLoadEmbeddingProfileSupportsModelTypeRegistry(t *testing.T) {
	tmpDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmpDir, "models"), 0o755); err != nil {
		t.Fatal(err)
	}
	profileJSON := `{
	  "nomic-embed-text-v1.5-q8_0": {
	    "display_name": "Nomic Embed Text v1.5 (Q8)",
	    "max_context_tokens": 8192,
	    "supports_matryoshka": true,
	    "dimensions": {
	      "routing": 128,
	      "document": 256
	    },
	    "prefixes": {
	      "routing_search": "classification: ",
	      "doc_storage": "search_document: ",
	      "doc_search": "search_query: "
	    }
	  },
	  "bge-small-en-v1.5-q8_0": {
	    "display_name": "BAAI General Embedding Small v1.5 (Q8)",
	    "max_context_tokens": 512,
	    "supports_matryoshka": false,
	    "dimensions": {
	      "routing": 384,
	      "document": 384
	    },
	    "prefixes": {
	      "routing_search": "",
	      "doc_storage": "",
	      "doc_search": "Represent this sentence for searching relevant passages: "
	    }
	  }
	}`
	if err := os.WriteFile(filepath.Join(tmpDir, "models", "embedder_profiles.json"), []byte(profileJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(oldWd) }()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}

	profile := loadEmbeddingProfile("models/nomic-embed-text-v1.5-q8_0.gguf")
	if profile.DisplayName != "Nomic Embed Text v1.5 (Q8)" {
		t.Fatalf("expected display name from model registry, got %q", profile.DisplayName)
	}
	if profile.MaxContextTokens != 8192 || !profile.SupportsMatryoshka {
		t.Fatalf("expected registry metadata to be loaded, got %+v", profile)
	}
}

func TestLoadEmbeddingProfileSupportsKelindarSecondaryModel(t *testing.T) {
	tmpDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmpDir, "models"), 0o755); err != nil {
		t.Fatal(err)
	}
	profileJSON := `{
	  "nomic-embed-text-v1.5-q8_0": {
	    "display_name": "Nomic Embed Text v1.5 (Q8)",
	    "max_context_tokens": 8192,
	    "supports_matryoshka": true,
	    "dimensions": {"routing": 256, "document": 768},
	    "prefixes": {"routing_search": "classification: ", "doc_storage": "search_document: ", "doc_search": "search_query: "}
	  },
	  "bge-small-en-v1.5-q8_0": {
	    "display_name": "BAAI General Embedding Small v1.5 (Q8)",
	    "max_context_tokens": 512,
	    "supports_matryoshka": false,
	    "dimensions": {"routing": 384, "document": 384},
	    "prefixes": {"routing_search": "", "doc_storage": "", "doc_search": "Represent this sentence for searching relevant passages: "}
	  }
	}`
	if err := os.WriteFile(filepath.Join(tmpDir, "models", "embedder_profiles.json"), []byte(profileJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(oldWd) }()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}

	profile := loadEmbeddingProfile("bge-small-en-v1.5-q8_0")
	if profile.DisplayName != "BAAI General Embedding Small v1.5 (Q8)" {
		t.Fatalf("expected secondary Kelindar model entry to load, got %q", profile.DisplayName)
	}
	if profile.RoutingDim != 384 || profile.DocumentDim != 384 || profile.MaxContextTokens != 512 {
		t.Fatalf("expected secondary Kelindar dimensions and metadata, got %+v", profile)
	}
}

func TestLocalEmbedderCategoryVectorsAreNormalizedAfterSlicing(t *testing.T) {
	stub := &stubVectorizer{vec: []float32{1, 2, 3, 4, 5}}
	e := &Local{
		model: stub,
		profile: EmbeddingProfile{
			RoutingDim:         3,
			DocumentDim:        5,
			RoutingPrefix:      "classification: ",
			SupportsMatryoshka: true,
		},
	}

	catVecs, err := e.EmbedCategoryTexts(context.Background(), []string{"alpha"})
	if err != nil {
		t.Fatalf("EmbedCategoryTexts returned an unexpected error: %v", err)
	}
	if len(catVecs) != 1 || len(catVecs[0]) != 3 {
		t.Fatalf("expected one 3-dimensional normalized routing vector, got %+v", catVecs)
	}

	norm := 0.0
	for _, val := range catVecs[0] {
		norm += float64(val * val)
	}
	if norm < 0.999999 || norm > 1.000001 {
		t.Fatalf("expected normalized routing vector with unit norm, got %v", norm)
	}
}

func TestLocalEmbedderNormalizationIsLimitedToMatryoshkaModels(t *testing.T) {
	stub := &stubVectorizer{vec: []float32{1, 2, 3, 4, 5}}
	e := &Local{
		model: stub,
		profile: EmbeddingProfile{
			RoutingPrefix:      "classification: ",
			RoutingDim:         3,
			SupportsMatryoshka: false,
		},
	}

	vectors, err := e.EmbedTexts(context.Background(), []string{"classification: alpha"})
	if err != nil {
		t.Fatalf("EmbedTexts returned an unexpected error: %v", err)
	}
	if len(vectors) != 1 || len(vectors[0]) != 5 {
		t.Fatalf("expected unnormalized 5-d vector for non-matryoshka model, got %+v", vectors)
	}
}

func TestLocalEmbedderModeSpecificPathsApplyKelindarContract(t *testing.T) {
	stub := &stubVectorizer{vec: []float32{1, 2, 3, 4, 5}}
	e := &Local{
		model: stub,
		profile: EmbeddingProfile{
			RoutingDim:     3,
			DocumentDim:    5,
			RoutingPrefix:  "classification: ",
			DocStorePrefix: "search_document: ",
			DocQueryPrefix: "search_query: ",
		},
	}

	catVecs, err := e.EmbedCategoryTexts(context.Background(), []string{"alpha"})
	if err != nil {
		t.Fatalf("EmbedCategoryTexts returned an unexpected error: %v", err)
	}
	if len(catVecs) != 1 || len(catVecs[0]) != 3 {
		t.Fatalf("expected one 3-dimensional routing vector, got %+v", catVecs)
	}
	if got := stub.texts[0]; got != "classification: alpha" {
		t.Fatalf("expected classification prefix, got %q", got)
	}

	stub.texts = nil
	docVecs, err := e.EmbedDocumentTexts(context.Background(), []string{"delta"})
	if err != nil {
		t.Fatalf("EmbedDocumentTexts returned an unexpected error: %v", err)
	}
	if got := stub.texts[0]; got != "search_document: delta" {
		t.Fatalf("expected search_document prefix, got %q", got)
	}
	if len(docVecs[0]) != 5 {
		t.Fatalf("expected full document vector length 5, got %d", len(docVecs[0]))
	}

	stub.texts = nil
	queryVecs, err := e.EmbedQueryTexts(context.Background(), []string{"gamma"})
	if err != nil {
		t.Fatalf("EmbedQueryTexts returned an unexpected error: %v", err)
	}
	if got := stub.texts[0]; got != "search_query: gamma" {
		t.Fatalf("expected search_query prefix, got %q", got)
	}
	if len(queryVecs[0]) != 5 {
		t.Fatalf("expected full query vector length 5, got %d", len(queryVecs[0]))
	}
}

func TestLocalEmbedderEmbedTextsRespectsCancellation(t *testing.T) {
	e := &Local{model: &stubVectorizer{}, gate: make(chan struct{}, 1)}
	e.gate <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := e.EmbedTexts(ctx, []string{"alpha"})
	if err == nil {
		t.Fatal("expected cancellation to be reported when the slot is unavailable")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation error, got %v", err)
	}
}

func TestLocalEmbedderReturnsErrorForMissingModel(t *testing.T) {
	_, err := newLocal("", 0, func(string, int) (localVectorizer, error) {
		return nil, errors.New("boom")
	})
	if err == nil {
		t.Fatal("expected an error when the model path is empty")
	}
}

func TestLocalEmbedderCloseDelegatesToModel(t *testing.T) {
	stub := &stubVectorizer{}
	e := &Local{model: stub}
	if err := e.Close(); err != nil {
		t.Fatalf("Close returned an error: %v", err)
	}
	if !stub.closed {
		t.Fatal("expected Close to delegate to the underlying model")
	}
}

func TestLocalEmbedderFallbackProviderWorksWithoutNativeLibrary(t *testing.T) {
	e, err := NewLocalWithProvider("kelindar", "kelindar", 0)
	if err != nil {
		t.Fatalf("NewLocalWithProvider returned an unexpected error: %v", err)
	}
	if e == nil {
		t.Fatal("expected a local embedder instance")
	}
	vectors, err := e.EmbedTexts(context.Background(), []string{"connection test"})
	if err != nil {
		t.Fatalf("EmbedTexts returned an unexpected error: %v", err)
	}
	if len(vectors) != 1 || len(vectors[0]) == 0 {
		t.Fatalf("expected a non-empty embedding vector, got %+v", vectors)
	}
}

func TestLocalEmbedderProviderSelection(t *testing.T) {
	providerName := "stub-provider"
	RegisterLocalEmbedder(providerName, func(modelPath string, gpuLayers int) (localVectorizer, error) {
		return &stubVectorizer{vec: []float32{1, 2, 3}}, nil
	})
	defer delete(localEmbedderFactories, providerName)

	e, err := NewLocalWithProvider(providerName, "models/test.gguf", 0)
	if err != nil {
		t.Fatalf("NewLocalWithProvider returned an unexpected error: %v", err)
	}
	if got := e.Name(); got == "" {
		t.Fatal("expected the embedder to have a name")
	}

	vectors, err := e.EmbedTexts(context.Background(), []string{"alpha"})
	if err != nil {
		t.Fatalf("EmbedTexts returned an unexpected error: %v", err)
	}
	if len(vectors) != 1 || len(vectors[0]) != 3 {
		t.Fatalf("expected one 3-dimensional vector, got %+v", vectors)
	}
}

func TestAvailableLocalEmbeddersIncludesRegisteredProvider(t *testing.T) {
	providerName := "available-provider"
	RegisterLocalEmbedder(providerName, func(modelPath string, gpuLayers int) (localVectorizer, error) {
		return &stubVectorizer{}, nil
	})
	defer delete(localEmbedderFactories, providerName)

	found := false
	for _, name := range AvailableLocalEmbedders() {
		if name == providerName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected %q to be listed among available local embedders", providerName)
	}
}

func TestEnsureLocalModelPathUsesExistingFile(t *testing.T) {
	modelPath := filepath.Join(t.TempDir(), "existing.gguf")
	if err := os.WriteFile(modelPath, []byte("model"), 0o644); err != nil {
		t.Fatal(err)
	}

	resolved, err := ensureLocalModelPath(modelPath)
	if err != nil {
		t.Fatalf("ensureLocalModelPath returned an unexpected error: %v", err)
	}
	if resolved != modelPath {
		t.Fatalf("expected existing path %q, got %q", modelPath, resolved)
	}
}

func TestEnsureLocalModelPathPrefersBundledModelCopy(t *testing.T) {
	oldHome := os.Getenv("HOME")
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	if err := os.MkdirAll(filepath.Join(tmpDir, "models"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "models", defaultLocalModelFileName), []byte("bundled"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(oldWd); os.Setenv("HOME", oldHome) }()

	originalDownloader := downloadLocalModelFile
	defer func() { downloadLocalModelFile = originalDownloader }()

	downloaded := false
	downloadLocalModelFile = func(dst, modelURL string) error {
		downloaded = true
		return nil
	}

	resolved, err := ensureLocalModelPath("kelindar")
	if err != nil {
		t.Fatalf("ensureLocalModelPath returned an unexpected error: %v", err)
	}
	if downloaded {
		t.Fatal("expected bundled model copy to be used without downloading")
	}
	if resolved != filepath.Join("models", defaultLocalModelFileName) {
		t.Fatalf("expected bundled model path %q, got %q", filepath.Join("models", defaultLocalModelFileName), resolved)
	}
}

func TestEnsureLocalModelPathDownloadsToCache(t *testing.T) {
	oldHome := os.Getenv("HOME")
	t.Setenv("HOME", t.TempDir())
	defer os.Setenv("HOME", oldHome)

	originalDownloader := downloadLocalModelFile
	defer func() { downloadLocalModelFile = originalDownloader }()

	downloaded := false
	downloadLocalModelFile = func(dst, modelURL string) error {
		downloaded = true
		if modelURL != defaultLocalModelDownloadURL {
			t.Fatalf("unexpected model URL: %s", modelURL)
		}
		return os.WriteFile(dst, []byte("cached"), 0o644)
	}

	resolved, err := ensureLocalModelPath("models/missing.gguf")
	if err != nil {
		t.Fatalf("ensureLocalModelPath returned an unexpected error: %v", err)
	}
	if !downloaded {
		t.Fatal("expected the model download helper to be called")
	}

	if _, err := os.Stat(resolved); err != nil {
		t.Fatalf("expected cached model file to exist: %v", err)
	}
}
