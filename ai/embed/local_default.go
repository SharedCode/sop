//go:build !local_embedder

package embed

import (
	"context"
	"fmt"
)

type fallbackLocalVectorizer struct {
	embedder *Simple
}

func (f *fallbackLocalVectorizer) EmbedText(text string) ([]float32, error) {
	if f == nil || f.embedder == nil {
		return nil, fmt.Errorf("fallback local embedder is not initialized")
	}
	vecs, err := f.embedder.EmbedTexts(context.Background(), []string{text})
	if err != nil {
		return nil, err
	}
	if len(vecs) == 0 {
		return nil, fmt.Errorf("fallback local embedder produced no vectors")
	}
	return vecs[0], nil
}

func (f *fallbackLocalVectorizer) Close() error { return nil }

func init() {
	RegisterLocalEmbedder("kelindar", func(modelPath string, gpuLayers int) (localVectorizer, error) {
		return &fallbackLocalVectorizer{embedder: NewSimple("kelindar-fallback", 384, nil)}, nil
	})
}
