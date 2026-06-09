package embed

import (
	"context"
	"testing"
)

type fakeEmbedder struct {
	inputs []string
	vecs   [][]float32
}

func (f *fakeEmbedder) Name() string { return "fake" }
func (f *fakeEmbedder) Dim() int     { return 768 }
func (f *fakeEmbedder) EmbedTexts(_ context.Context, texts []string) ([][]float32, error) {
	f.inputs = append([]string(nil), texts...)
	return f.vecs, nil
}

func TestProfiledEmbedder_UsesModeSpecificPrefixesAndTruncation(t *testing.T) {
	base := &fakeEmbedder{vecs: [][]float32{{1, 2, 3, 4, 5, 6}}}
	modeled := NewProfiledEmbedder(base, EmbeddingProfile{
		RoutingDim:     4,
		DocumentDim:    768,
		RoutingPrefix:  "classification: ",
		DocStorePrefix: "search_document: ",
		DocQueryPrefix: "search_query: ",
	})

	catVecs, err := modeled.(interface {
		EmbedCategoryTexts(context.Context, []string) ([][]float32, error)
	}).EmbedCategoryTexts(context.Background(), []string{"alpha"})
	if err != nil {
		t.Fatalf("EmbedCategoryTexts returned error: %v", err)
	}
	if got := base.inputs[0]; got != "classification: alpha" {
		t.Fatalf("expected classification prefix, got %q", got)
	}
	if len(catVecs[0]) != 4 {
		t.Fatalf("expected routed vector to be truncated to 4 dims, got %d", len(catVecs[0]))
	}

	base.inputs = nil
	docVecs, err := modeled.(interface {
		EmbedDocumentTexts(context.Context, []string) ([][]float32, error)
	}).EmbedDocumentTexts(context.Background(), []string{"beta"})
	if err != nil {
		t.Fatalf("EmbedDocumentTexts returned error: %v", err)
	}
	if got := base.inputs[0]; got != "search_document: beta" {
		t.Fatalf("expected document prefix, got %q", got)
	}
	if len(docVecs[0]) != 6 {
		t.Fatalf("expected full document vector length to remain 6, got %d", len(docVecs[0]))
	}

	base.inputs = nil
	queryVecs, err := modeled.(interface {
		EmbedQueryTexts(context.Context, []string) ([][]float32, error)
	}).EmbedQueryTexts(context.Background(), []string{"gamma"})
	if err != nil {
		t.Fatalf("EmbedQueryTexts returned error: %v", err)
	}
	if got := base.inputs[0]; got != "search_query: gamma" {
		t.Fatalf("expected query prefix, got %q", got)
	}
	if len(queryVecs[0]) != 6 {
		t.Fatalf("expected full query vector length to remain 6, got %d", len(queryVecs[0]))
	}
}
