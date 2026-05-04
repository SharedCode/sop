package main

import (
	"context"
	"fmt"
	"strings"
)

type LLMClient interface {
	Generate(prompt string) (string, error)
}

type Summarizer interface {
	Summarize(ctx context.Context, text string, maxChunks int) ([]string, error)
}

type sentenceSummarizer struct{}

func (s *sentenceSummarizer) Summarize(ctx context.Context, text string, maxChunks int) ([]string, error) {
	sentences := strings.Split(text, ".")
	var chunks []string
	for _, sentence := range sentences {
		clean := strings.TrimSpace(sentence)
		if len(clean) > 5 {
			chunks = append(chunks, clean+".")
		}
		if len(chunks) >= maxChunks {
			break
		}
	}
	if len(chunks) == 0 && len(strings.TrimSpace(text)) > 0 {
		chunks = append(chunks, strings.TrimSpace(text))
	}
	return chunks, nil
}

type llmSummarizer struct {
	client LLMClient
}

func (s *llmSummarizer) Summarize(ctx context.Context, text string, maxChunks int) ([]string, error) {
	if s.client == nil {
		return defaultSummarizer.Summarize(ctx, text, maxChunks)
	}

	prompt := fmt.Sprintf("Summarize the following text into at most %d distinct, highly dense semantic key point sentences.\nReturn each point on a new line. Do not include bullet points or numbering.\nText: %s", maxChunks, text)

	resp, err := s.client.Generate(prompt)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(resp, "\n")
	var chunks []string
	for _, line := range lines {
		clean := strings.TrimSpace(line)
		clean = strings.TrimPrefix(clean, "- ")
		clean = strings.TrimPrefix(clean, "* ")

		if len(clean) > 5 {
			chunks = append(chunks, clean)
		}
		if len(chunks) >= maxChunks {
			break
		}
	}

	if len(chunks) == 0 {
		return defaultSummarizer.Summarize(ctx, text, maxChunks)
	}
	return chunks, nil
}

var defaultSummarizer Summarizer = &sentenceSummarizer{}

func GetSummarizer(productionMode bool, client LLMClient) Summarizer {
	if productionMode && client != nil {
		return &llmSummarizer{client: client}
	}
	return &sentenceSummarizer{}
}
