package embed

import (
	"fmt"

	"github.com/sharedcode/sop/ai"
)

// NewKelindarEmbedder creates a reusable local embedder for the Kelindar/Nomic stack.
//
// The helper is intentionally small and reusable from client code because the
// broader KnowledgeBase flow expects any ai.Embeddings implementation.
func NewKelindarEmbedder(modelPath string, gpuLayers int) (ai.Embeddings, error) {
	if modelPath == "" {
		modelPath = "kelindar"
	}
	local, err := NewLocalWithProvider("kelindar", modelPath, gpuLayers)
	if err != nil {
		return nil, fmt.Errorf("create kelindar embedder: %w", err)
	}
	return local, nil
}
