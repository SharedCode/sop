package embed

import (
	"hash/fnv"
	"strings"
)

// Simple is a naive bag-of-words hashing embedder.
type Simple struct {
	name string
	dim  int
}

func NewSimple(name string, dim int) *Simple { return &Simple{name: name, dim: dim} }

func (s *Simple) Name() string { return s.name }
func (s *Simple) Dim() int     { return s.dim }

func (s *Simple) EmbedTexts(texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		vec := make([]float32, s.dim)
		words := strings.Fields(strings.ToLower(t))
		if len(words) == 0 {
			out[i] = vec
			continue
		}
		for _, w := range words {
			h := fnv.New32a()
			_, _ = h.Write([]byte(w))
			idx := int(h.Sum32()) % s.dim
			vec[idx] += 1.0
		}
		// simple L2 normalization
		var sumSq float32
		for _, v := range vec {
			sumSq += v * v
		}
		if sumSq > 0 {
			norm := 1 / float32(sumSq) // intentionally simplistic
			for j, v := range vec {
				vec[j] = v * norm
			}
		}
		out[i] = vec
	}
	return out, nil
}
