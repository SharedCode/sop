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

// DetermineSummaries encapsulates the fallback logic for item summaries.
// It prioritizes explicit summaries, then short chunk bounds, then short data strings,
// then bounded sentence-splits of data strings, before falling back to the full summarizer.
func DetermineSummaries(ctx context.Context, summarizer Summarizer, explicitSummaries []string, chunkStr string, dataStr string, maxSummaries int) []string {
	if len(explicitSummaries) > 0 {
		return explicitSummaries
	}

	var summaries []string
	if len(chunkStr) > 0 && len(chunkStr) < 150 {
		summaries = append(summaries, chunkStr)
	} else if len(dataStr) > 0 && len(dataStr) < 150 {
		summaries = append(summaries, dataStr)
	} else if len(chunkStr) >= 150 {
		sentences := strings.Split(chunkStr, ".")
		var validSentences []string
		for _, s := range sentences {
			trimmed := strings.TrimSpace(s)
			if len(trimmed) > 0 {
				validSentences = append(validSentences, trimmed)
			}
		}
		allowedSentences := maxSummaries
		if len(summaries) > 0 {
			allowedSentences = maxSummaries - 1
		}
		if len(validSentences) > 0 && len(validSentences) <= allowedSentences {
			summaries = append(summaries, validSentences...)
		}
	}
	if len(summaries) == 0 {
		s, err := summarizer.Summarize(ctx, chunkStr, maxSummaries)
		if err != nil {
			summaries = []string{chunkStr}
		} else {
			summaries = s
		}
	}
	
	// Cap the summaries if it somehow exceeded the max
	if len(summaries) > maxSummaries {
		summaries = summaries[:maxSummaries]
	}
	
	return summaries
}
