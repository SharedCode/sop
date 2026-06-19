package embed

import (
	"fmt"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// NewFromName creates an embedder using the configured provider/model name.
// It is intentionally lightweight and self-contained so callers can construct
// a concrete embedder without needing a separate factory callback.
func NewFromName(name string, dim int) (ai.Embeddings, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		trimmed = "simple"
	}

	normalized := strings.ToLower(trimmed)
	switch {
	case normalized == "simple" || normalized == "mock" || normalized == "default":
		if dim <= 0 {
			dim = 384
		}
		return NewSimple(trimmed, dim, nil), nil
	case strings.Contains(normalized, "local-kelindar"):
		return NewLocalWithProvider("kelindar", trimmed, 0)
	case strings.Contains(normalized, "gemini"):
		return NewGemini("", trimmed), nil
	case strings.Contains(normalized, "openai") || strings.Contains(normalized, "text-embedding"):
		return NewOpenAI("", trimmed, ""), nil
	case strings.Contains(normalized, "ollama") || strings.Contains(normalized, "nomic"):
		return NewOllama("", trimmed), nil
	default:
		if dim <= 0 {
			dim = 384
		}
		return nil, fmt.Errorf("unsupported embedder name %q", name)
	}
}
