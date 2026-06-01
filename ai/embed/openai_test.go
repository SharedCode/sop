package embed

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAIEmbedderReportsUnsupportedEmbeddings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = w.Write([]byte(`{"error":{"message":"Embeddings are not supported by Copilot.","type":"not_implemented","code":"embeddings_not_supported"}}`))
	}))
	defer server.Close()

	embedder := NewOpenAI("anything", "text-embedding-3-small", server.URL+"/v1")

	_, err := embedder.EmbedTexts(context.Background(), []string{"connection test"})
	if err == nil {
		t.Fatal("expected error for unsupported embeddings endpoint")
	}
	if !strings.Contains(err.Error(), "does not support embeddings") {
		t.Fatalf("expected unsupported embeddings message, got %q", err.Error())
	}
}
