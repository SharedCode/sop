package embed

import (
	"context"

	"github.com/sharedcode/sop/ai"
)

func CategoryTexts(ctx context.Context, embedder ai.Embeddings, texts []string) ([][]float32, error) {
	if modeAware, ok := embedder.(ai.EmbeddingModeSupport); ok {
		return modeAware.EmbedCategoryTexts(ctx, texts)
	}
	return embedder.EmbedTexts(ctx, texts)
}

func DocumentTexts(ctx context.Context, embedder ai.Embeddings, texts []string) ([][]float32, error) {
	if modeAware, ok := embedder.(ai.EmbeddingModeSupport); ok {
		return modeAware.EmbedDocumentTexts(ctx, texts)
	}
	return embedder.EmbedTexts(ctx, texts)
}

func QueryTexts(ctx context.Context, embedder ai.Embeddings, texts []string) ([][]float32, error) {
	if modeAware, ok := embedder.(ai.EmbeddingModeSupport); ok {
		return modeAware.EmbedQueryTexts(ctx, texts)
	}
	return embedder.EmbedTexts(ctx, texts)
}
