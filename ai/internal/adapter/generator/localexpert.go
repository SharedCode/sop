package generator

import (
	"context"
	"fmt"
	"strings"

	"github.com/sharedcode/sop/ai/internal/port"
)

// LocalExpert is a heuristic-based generator that simulates an expert's reasoning
// by analyzing the prompt's context and query overlap.
type LocalExpert struct {
	name string
}

func init() {
	Register("local-expert", func(cfg map[string]any) (port.Generator, error) {
		return &LocalExpert{name: "local-expert"}, nil
	})
}

func (g *LocalExpert) Name() string { return g.name }

func (g *LocalExpert) Generate(ctx context.Context, prompt string, opts port.GenOptions) (port.GenOutput, error) {
	// 1. Parse the Prompt to extract Context and Query
	// We assume the prompt format: "Knowledge Base:\n{{context}}\n\nQuery: {{query}}"
	// But to be robust, we'll look for the standard separator used in main.go

	// Default separator if not found
	separator := "Query:"
	if strings.Contains(prompt, "Patient Symptoms:") {
		separator = "Patient Symptoms:"
	}

	parts := strings.Split(prompt, separator)
	if len(parts) < 2 {
		return port.GenOutput{Text: "I need more information to make a diagnosis/assessment."}, nil
	}

	contextPart := parts[0]
	queryPart := parts[1]

	// Clean up context
	contextLines := strings.Split(contextPart, "\n")
	var candidates []string
	for _, line := range contextLines {
		line = strings.TrimSpace(line)
		// Filter out headers and empty lines
		if line != "" && strings.Contains(line, ":") && !strings.HasPrefix(line, "Knowledge Base") && !strings.HasPrefix(line, "Medical Knowledge") {
			candidates = append(candidates, line)
		}
	}

	// Analyze Query (Simple Keyword Matching)
	query := strings.ToLower(queryPart)
	queryWords := strings.Fields(query)

	// Format user input for display
	rawItems := strings.Split(queryPart, "|")
	var displayItems []string
	for _, item := range rawItems {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if strings.Contains(item, " ") {
			displayItems = append(displayItems, fmt.Sprintf("\"%s\"", item))
		} else {
			displayItems = append(displayItems, item)
		}
	}
	displayInput := strings.Join(displayItems, ", ")

	bestScore := 0
	var bestCandidates []string

	// If no context provided (RAG failed or empty), we can't diagnose
	if len(candidates) == 0 {
		return port.GenOutput{Text: "I don't have enough knowledge about this topic yet."}, nil
	}

	for _, cand := range candidates {
		candLower := strings.ToLower(cand)
		score := 0
		for _, word := range queryWords {
			if len(word) > 3 && strings.Contains(candLower, word) {
				score++
			}
		}

		if score > bestScore {
			bestScore = score
			bestCandidates = []string{cand}
		} else if score == bestScore && score > 0 {
			bestCandidates = append(bestCandidates, cand)
		}
	}

	var response string
	if bestScore > 0 {
		if len(bestCandidates) == 1 {
			// Extract Condition/Issue Name
			condition := strings.Split(bestCandidates[0], ":")[0]
			response = fmt.Sprintf("Based on your input (%s), the most likely result is **%s**.\n\nReference: %s",
				displayInput, condition, bestCandidates[0])
		} else {
			response = fmt.Sprintf("Based on your input (%s), there are multiple potential matches with similar likelihood:\n", displayInput)
			for _, cand := range bestCandidates {
				condition := strings.Split(cand, ":")[0]
				response += fmt.Sprintf("\n- **%s**\n  Reference: %s", condition, cand)
			}
		}
	} else {
		response = "The details provided are too vague or do not match any specific entry in my current knowledge base. Could you describe them in more detail?"
		if len(candidates) > 0 {
			response += "\n\nPossible entries considered:\n"
			for _, c := range candidates {
				response += "- " + strings.Split(c, ":")[0] + "\n"
			}
		}
	}

	return port.GenOutput{
		Text:       response,
		TokensUsed: len(prompt),
	}, nil
}

func (g *LocalExpert) EstimateCost(inTokens, outTokens int) float64 {
	return 0
}
