package embed

import (
	"context"

	"github.com/sharedcode/sop/ai"
)

// EmbeddingProfile captures the model-specific routing/document contract.
type EmbeddingProfile struct {
	ModelName          string
	DisplayName        string
	MaxContextTokens   int
	SupportsMatryoshka bool
	RoutingDim         int
	DocumentDim        int
	RoutingPrefix      string
	DocStorePrefix     string
	DocQueryPrefix     string
}

// ProfiledEmbedder wraps any ai.Embeddings with explicit category/document/query modes.
type ProfiledEmbedder struct {
	base    ai.Embeddings
	profile EmbeddingProfile
}

func NewProfiledEmbedder(base ai.Embeddings, profile EmbeddingProfile) ai.Embeddings {
	if base == nil {
		return nil
	}
	return &ProfiledEmbedder{base: base, profile: profile}
}

func (p *ProfiledEmbedder) Name() string { return p.base.Name() }

func (p *ProfiledEmbedder) Dim() int {
	if p.profile.DocumentDim > 0 {
		return p.profile.DocumentDim
	}
	return p.base.Dim()
}

func (p *ProfiledEmbedder) EmbedTexts(ctx context.Context, texts []string) ([][]float32, error) {
	return p.base.EmbedTexts(ctx, texts)
}

func (p *ProfiledEmbedder) EmbedCategoryTexts(ctx context.Context, texts []string) ([][]float32, error) {
	return p.embedWithPrefix(ctx, texts, p.profile.RoutingPrefix, p.profile.RoutingDim)
}

func (p *ProfiledEmbedder) EmbedDocumentTexts(ctx context.Context, texts []string) ([][]float32, error) {
	return p.embedWithPrefix(ctx, texts, p.profile.DocStorePrefix, p.profile.DocumentDim)
}

func (p *ProfiledEmbedder) EmbedQueryTexts(ctx context.Context, texts []string) ([][]float32, error) {
	return p.embedWithPrefix(ctx, texts, p.profile.DocQueryPrefix, p.profile.DocumentDim)
}

func (p *ProfiledEmbedder) embedWithPrefix(ctx context.Context, texts []string, prefix string, targetDim int) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	prefixed := make([]string, 0, len(texts))
	for _, text := range texts {
		prefixed = append(prefixed, prefix+text)
	}

	vecs, err := p.base.EmbedTexts(ctx, prefixed)
	if err != nil {
		return nil, err
	}

	out := make([][]float32, 0, len(vecs))
	for _, vec := range vecs {
		candidate := vec
		if targetDim > 0 && len(vec) > targetDim {
			candidate = make([]float32, targetDim)
			copy(candidate, vec[:targetDim])
		}
		out = append(out, candidate)
	}
	return out, nil
}
