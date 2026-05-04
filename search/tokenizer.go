package search

import (
	"strings"
	"unicode"
)

// Tokenizer defines the interface for splitting text into terms.
type Tokenizer interface {
	Tokenize(text string) []string
}

// SimpleTokenizer splits text by whitespace and punctuation, converting to lowercase.
type SimpleTokenizer struct{}

func (t *SimpleTokenizer) Tokenize(text string) []string {
	f := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	fields := strings.FieldsFunc(text, f)
	var tokens []string
	for _, field := range fields {
		token := strings.ToLower(field)
		if !DefaultStopWords[token] && len(token) > 0 {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// DefaultStopWords contains common English words to filter out (similar to Lucene's default stop words).
var DefaultStopWords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true, "be": true, "but": true,
	"by": true, "for": true, "if": true, "in": true, "into": true, "is": true, "it": true,
	"no": true, "not": true, "of": true, "on": true, "or": true, "such": true, "that": true,
	"the": true, "their": true, "then": true, "there": true, "these": true,
	"they": true, "this": true, "to": true, "was": true, "will": true, "with": true,
}
