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
		tokens = append(tokens, strings.ToLower(field))
	}
	return tokens
}

// Posting represents a single occurrence of a term in a document.
type Posting struct {
	DocID     string
	Frequency int
	Positions []int
}
