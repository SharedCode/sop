package embed

import (
	"context"
	"hash/fnv"
	"math"
	"strings"
)

// Simple is a naive bag-of-words hashing embedder with configurable synonym heuristics.
type Simple struct {
	name     string
	dim      int
	synonyms map[string]string
}

// NewSimple creates a new embedder with optional custom synonyms.
// If synonyms is nil, it defaults to an empty map (no heuristics).
func NewSimple(name string, dim int, synonyms map[string]string) *Simple {
	if synonyms == nil {
		synonyms = make(map[string]string)
	}
	return &Simple{name: name, dim: dim, synonyms: synonyms}
}

// Name returns the name of the embedder.
func (s *Simple) Name() string { return s.name }

// Dim returns the dimension of the embeddings.
func (s *Simple) Dim() int { return s.dim }

// normalizeWord applies local heuristics to map synonyms to a canonical concept.
func (s *Simple) normalizeWord(w string) string {
	// 1. Strip punctuation
	w = strings.Trim(w, ".,!?-")

	// 2. Check stop words
	if isStopWord(w) {
		return ""
	}

	// 3. Check configured synonyms
	if canonical, ok := s.synonyms[w]; ok {
		return canonical
	}
	return w
}

// EmbedTexts generates embeddings for the given texts using a bag-of-words hashing approach.
func (s *Simple) EmbedTexts(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		vec := make([]float32, s.dim)
		words := strings.Fields(strings.ToLower(t))
		if len(words) == 0 {
			out[i] = vec
			continue
		}

		validWords := 0
		for _, w := range words {
			// Apply heuristic normalization
			w = s.normalizeWord(w)
			if w == "" {
				continue
			}
			validWords++

			h := fnv.New32a()
			_, _ = h.Write([]byte(w))
			idx := int(h.Sum32()) % s.dim
			// Ensure positive index
			if idx < 0 {
				idx = -idx
			}
			vec[idx] += 1.0
		}

		// L2 normalization
		var sumSq float32
		for _, v := range vec {
			sumSq += v * v
		}
		if sumSq > 0 {
			// Correct L2 norm is 1 / sqrt(sumSq)
			// We use a simplified inverse sqrt approximation or just standard math
			// For this simple embedder, standard math is fine.
			// Note: The previous implementation used 1/sumSq which was incorrect.
			norm := float32(1.0 / math.Sqrt(float64(sumSq)))
			for j, v := range vec {
				vec[j] = v * norm
			}
		}
		out[i] = vec
	}
	return out, nil
}

var stopWords = map[string]bool{
	"i": true, "me": true, "my": true, "myself": true, "we": true, "our": true, "ours": true, "ourselves": true,
	"you": true, "your": true, "yours": true, "yourself": true, "yourselves": true, "he": true, "him": true,
	"his": true, "himself": true, "she": true, "her": true, "hers": true, "herself": true, "it": true, "its": true,
	"itself": true, "they": true, "them": true, "their": true, "theirs": true, "themselves": true, "what": true,
	"which": true, "who": true, "whom": true, "this": true, "that": true, "these": true, "those": true, "am": true,
	"is": true, "are": true, "was": true, "were": true, "be": true, "been": true, "being": true, "have": true,
	"has": true, "had": true, "having": true, "do": true, "does": true, "did": true, "doing": true, "a": true,
	"an": true, "the": true, "and": true, "but": true, "if": true, "or": true, "because": true, "as": true,
	"until": true, "while": true, "of": true, "at": true, "by": true, "for": true, "with": true, "about": true,
	"against": true, "between": true, "into": true, "through": true, "during": true, "before": true, "after": true,
	"above": true, "below": true, "to": true, "from": true, "up": true, "down": true, "in": true, "out": true,
	"on": true, "off": true, "over": true, "under": true, "again": true, "further": true, "then": true, "once": true,
	"here": true, "there": true, "when": true, "where": true, "why": true, "how": true, "all": true, "any": true,
	"both": true, "each": true, "few": true, "more": true, "most": true, "other": true, "some": true, "such": true,
	"no": true, "nor": true, "not": true, "only": true, "own": true, "same": true, "so": true, "than": true,
	"too": true, "very": true, "s": true, "t": true, "can": true, "will": true, "just": true, "don": true,
	"should": true, "now": true, "case": true, "symptoms": true, "include": true, // Domain specific stops
	"score": true,              // "score" appears in the context output and causes collisions
	"feel":  true, "bad": true, // Add more stop words to reduce noise
}

func isStopWord(w string) bool {
	return stopWords[w]
}
