package embed

import (
	"context"
	"reflect"
	"testing"

	"github.com/sharedcode/sop/ai"
)

type modeAwareFake struct {
	categoryCalls int
	documentCalls int
	queryCalls    int
}

func (f *modeAwareFake) Name() string { return "mode-aware-fake" }
func (f *modeAwareFake) Dim() int     { return 3 }

func (f *modeAwareFake) EmbedTexts(_ context.Context, texts []string) ([][]float32, error) {
	return [][]float32{{float32(len(texts))}}, nil
}

func (f *modeAwareFake) EmbedCategoryTexts(_ context.Context, texts []string) ([][]float32, error) {
	f.categoryCalls++
	return [][]float32{{1, 2, 3}}, nil
}

func (f *modeAwareFake) EmbedDocumentTexts(_ context.Context, texts []string) ([][]float32, error) {
	f.documentCalls++
	return [][]float32{{4, 5, 6}}, nil
}

func (f *modeAwareFake) EmbedQueryTexts(_ context.Context, texts []string) ([][]float32, error) {
	f.queryCalls++
	return [][]float32{{7, 8, 9}}, nil
}

func TestResilientEmbedderPreservesEmbeddingModeSupport(t *testing.T) {
	base := &modeAwareFake{}
	r := NewResilientEmbedder(base)

	if _, ok := any(r).(ai.EmbeddingModeSupport); !ok {
		t.Fatal("expected resilient embedder to preserve EmbeddingModeSupport")
	}

	catVecs, err := r.EmbedCategoryTexts(context.Background(), []string{"alpha"})
	if err != nil {
		t.Fatalf("EmbedCategoryTexts returned error: %v", err)
	}
	if !reflect.DeepEqual(catVecs, [][]float32{{1, 2, 3}}) {
		t.Fatalf("EmbedCategoryTexts returned unexpected vectors: %v", catVecs)
	}

	docVecs, err := r.EmbedDocumentTexts(context.Background(), []string{"beta"})
	if err != nil {
		t.Fatalf("EmbedDocumentTexts returned error: %v", err)
	}
	if !reflect.DeepEqual(docVecs, [][]float32{{4, 5, 6}}) {
		t.Fatalf("EmbedDocumentTexts returned unexpected vectors: %v", docVecs)
	}

	queryVecs, err := r.EmbedQueryTexts(context.Background(), []string{"gamma"})
	if err != nil {
		t.Fatalf("EmbedQueryTexts returned error: %v", err)
	}
	if !reflect.DeepEqual(queryVecs, [][]float32{{7, 8, 9}}) {
		t.Fatalf("EmbedQueryTexts returned unexpected vectors: %v", queryVecs)
	}

	if base.categoryCalls != 1 || base.documentCalls != 1 || base.queryCalls != 1 {
		t.Fatalf("expected each mode-aware method to be delegated exactly once, got category=%d document=%d query=%d", base.categoryCalls, base.documentCalls, base.queryCalls)
	}
}
