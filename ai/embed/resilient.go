package embed

import (
	"context"
	"fmt"

	"github.com/sethvargo/go-retry"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
)

// ResilientEmbedder wraps an existing Embeddings model automatically
// backing off and retrying in case of network or API limit errors.
type ResilientEmbedder struct {
	base ai.Embeddings
}

// NewResilientEmbedder creates a ResilientEmbedder wrapping base Embeddings.
func NewResilientEmbedder(base ai.Embeddings) *ResilientEmbedder {
	return &ResilientEmbedder{base: base}
}

func (r *ResilientEmbedder) Name() string { return fmt.Sprintf("resilient-%s", r.base.Name()) }
func (r *ResilientEmbedder) Dim() int     { return r.base.Dim() }

func (r *ResilientEmbedder) EmbedTexts(ctx context.Context, texts []string) ([][]float32, error) {
	var result [][]float32
	err := sop.Retry(ctx, func(rctx context.Context) error {
		res, rerr := r.base.EmbedTexts(rctx, texts)
		if rerr != nil {
			if sop.ShouldRetry(rerr) {
				return retry.RetryableError(rerr)
			}
			return rerr // permanent
		}
		result = res
		return nil
	}, nil)

	return result, err
}
