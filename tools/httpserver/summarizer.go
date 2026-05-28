package main

import (
	"context"
	"fmt"
	"strings"
)

const MAX_PARAGRAPH_LENGTH = 500

type LLMClient interface {
	Generate(prompt string) (string, error)
}

type Summarizer interface {
	Summarize(ctx context.Context, text string, maxChunks int) ([]string, error)
}

type sentenceSummarizer struct{}

func (s *sentenceSummarizer) Summarize(ctx context.Context, text string, maxChunks int) ([]string, error) {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	paragraphs := strings.Split(text, "\n\n")
	var chunks []string

	for _, paragraph := range paragraphs {
		clean := strings.TrimSpace(paragraph)
		if len(clean) > 5 {
			if len(clean) <= MAX_PARAGRAPH_LENGTH {
				chunks = append(chunks, clean)
			} else {
				subsentences := strings.Split(clean, ".")
				var currentChunk string
				for _, sub := range subsentences {
					subClean := strings.TrimSpace(sub)
					if len(subClean) == 0 {
						continue
					}
					if currentChunk == "" {
						currentChunk = subClean + "."
					} else if len(currentChunk)+len(subClean)+1 <= MAX_PARAGRAPH_LENGTH {
						currentChunk += " " + subClean + "."
					} else {
						chunks = append(chunks, currentChunk)
						currentChunk = subClean + "."
					}
				}
				if currentChunk != "" {
					chunks = append(chunks, currentChunk)
				}
			}
		}
		if len(chunks) >= maxChunks {
			break
		}
	}

	if len(chunks) > maxChunks {
		chunks = chunks[:maxChunks]
	}

	if len(chunks) == 0 && len(strings.TrimSpace(text)) > 0 {
		cleanText := strings.TrimSpace(text)
		if len(cleanText) > MAX_PARAGRAPH_LENGTH {
			cleanText = cleanText[:MAX_PARAGRAPH_LENGTH]
		}
		chunks = append(chunks, cleanText)
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

func determineSummaries(chunkStr string, dataStr string, maxSummaries int) []string {
	chunkStr = strings.TrimSpace(chunkStr)
	dataStr = strings.TrimSpace(dataStr)

	f := func(str string, isFallback bool, currentSummCount int) []string {
		var result []string
		sentences := strings.Split(str, ".")
		var validSentences []string
		for _, s := range sentences {
			trimmed := strings.TrimSpace(s)
			if len(trimmed) > 0 {
				validSentences = append(validSentences, trimmed)
			}
		}

		allowedSentences := maxSummaries
		if currentSummCount > 0 {
			allowedSentences = maxSummaries - currentSummCount
		}
		if allowedSentences <= 0 {
			return result
		}

		if isFallback {
			if len(validSentences) > 0 {
				if len(validSentences) > allowedSentences {
					validSentences = validSentences[:allowedSentences]
				}
				result = append(result, validSentences...)
			}
		} else {
			if len(validSentences) > 0 && len(validSentences) <= allowedSentences {
				result = append(result, validSentences...)
			}
		}
		return result
	}

	var summaries []string
	var a, b bool
	if len(chunkStr) > 0 && len(chunkStr) < 150 {
		summaries = append(summaries, chunkStr)
		a = true
	}
	if len(dataStr) > 0 && len(dataStr) < 150 {
		summaries = append(summaries, dataStr)
		b = true
	}

	if len(summaries) < 2 {
		if !a {
			summaries = append(summaries, f(chunkStr, false, len(summaries))...)
		}
		if len(summaries) < maxSummaries && !b {
			summaries = append(summaries, f(dataStr, false, len(summaries))...)
		}
		if len(summaries) == 0 {
			summaries = append(summaries, f(chunkStr, true, len(summaries))...)
			if len(summaries) < maxSummaries {
				summaries = append(summaries, f(dataStr, true, len(summaries))...)
			}
		}
	}

	// Filter out any empty/whitespace-only items one last time to be completely robust
	var finalSummaries []string
	for _, s := range summaries {
		s = strings.TrimSpace(s)
		if s != "" {
			finalSummaries = append(finalSummaries, s)
		}
	}

	// Cap the summaries if it somehow exceeded the max
	if len(finalSummaries) > maxSummaries {
		finalSummaries = finalSummaries[:maxSummaries]
	}
	return finalSummaries
}

// DetermineSummaries encapsulates the fallback logic for item summaries.
// It prioritizes explicit summaries, then short chunk bounds, then short data strings,
// then bounded sentence-splits of data strings, before falling back to the full summarizer.
func DetermineSummaries(ctx context.Context, summarizer Summarizer, explicitSummaries []string, chunkStr string, dataStr string, maxSummaries int) []string {
	if len(explicitSummaries) > 0 {
		return explicitSummaries
	}

	summaries := determineSummaries(chunkStr, dataStr, maxSummaries)

	if len(summaries) == 0 {
		s, err := summarizer.Summarize(ctx, dataStr, maxSummaries)
		if err != nil {
			summaries = []string{dataStr}
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
