package embed

import "testing"

func TestNewKelindarEmbedderReturnsEmbeddings(t *testing.T) {
	embedder, err := NewKelindarEmbedder("kelindar", 0)
	if err != nil {
		t.Fatalf("NewKelindarEmbedder failed: %v", err)
	}
	if embedder == nil {
		t.Fatal("expected non-nil embedder")
	}
	if got := embedder.Name(); got == "" {
		t.Fatal("expected embedder name")
	}
	if got := embedder.Dim(); got <= 0 {
		t.Fatalf("expected a positive dimension, got %d", got)
	}
}
