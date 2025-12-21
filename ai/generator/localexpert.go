package generator

import (
	"context"
	"fmt"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// LocalExpert is a heuristic-based generator that simulates an expert's reasoning
// by analyzing the prompt's context and query overlap.
type LocalExpert struct {
	name string
}

func init() {
	Register("local-expert", func(cfg map[string]any) (ai.Generator, error) {
		return &LocalExpert{name: "local-expert"}, nil
	})
}

// Name returns the name of the generator.
func (g *LocalExpert) Name() string { return g.name }

// Generate produces a response by analyzing the prompt and matching it against a knowledge base.
// It uses simple keyword matching and scoring to find the best candidate.
func (g *LocalExpert) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	// 1. Parse the Prompt to extract Context and Query
	// We assume the prompt format: "Knowledge Base:\n{{context}}\n\nQuery: {{query}}"
	// But to be robust, we'll look for the standard separator used in main.go

	// Default separator if not found
	separator := "Query:"
	if strings.Contains(prompt, "Patient Symptoms:") {
		separator = "Patient Symptoms:"
	} else if strings.Contains(prompt, "User Query:") {
		separator = "User Query:"
	}

	parts := strings.Split(prompt, separator)
	if len(parts) < 2 {
		return ai.GenOutput{Text: "I need more information to make a diagnosis/assessment."}, nil
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

	// Check for SQL intent
	isSQL := false
	sqlKeywords := []string{"select", "insert", "update", "delete", "create", "drop", "show", "list"}
	for _, kw := range sqlKeywords {
		if strings.Contains(query, kw) {
			isSQL = true
			break
		}
	}

	if isSQL {
		// Simple SQL Heuristic
		if strings.Contains(query, "select") || strings.Contains(query, "show") || strings.Contains(query, "list") {
			// Check for "select * from <table>" pattern
			// Regex: select .* from (\w+)
			// We'll use a simple split for now to avoid heavy regex import if not needed, but regex is cleaner.
			// Let's just look for the word after "from"
			words := strings.Fields(strings.ToLower(query))
			var storeName string
			var limit int = -1 // Default to -1 (Unlimited/All) if not specified

			for i, w := range words {
				if w == "from" && i+1 < len(words) {
					storeName = words[i+1]
					// Strip trailing punctuation
					storeName = strings.TrimRight(storeName, ";.,")
				}
				if w == "limit" && i+1 < len(words) {
					fmt.Sscanf(words[i+1], "%d", &limit)
				}
			}

			if storeName != "" {
				return ai.GenOutput{Text: fmt.Sprintf(`{"tool": "select", "args": {"store": "%s", "limit": %d}}`, storeName, limit)}, nil
			}

			// Fallback to list_stores if no table specified
			return ai.GenOutput{Text: `{"tool": "list_stores", "args": {}}`}, nil
		}
		if strings.Contains(query, "delete") || strings.Contains(query, "remove") {
			return ai.GenOutput{Text: "```sql\nDELETE FROM items WHERE id = '...';\n```\n\n(Local Expert: I detected a delete operation. Please specify the ID.)"}, nil
		}
	}

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
		return ai.GenOutput{Text: "I don't have enough knowledge about this topic yet."}, nil
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
			candidate := bestCandidates[0]
			condition := extractConditionName(candidate)
			response = fmt.Sprintf("Based on your input (%s), the most likely result is **%s**.\n\nReference: %s",
				displayInput, condition, candidate)
		} else {
			response = fmt.Sprintf("Based on your input (%s), there are multiple potential matches with similar likelihood:\n", displayInput)
			for _, cand := range bestCandidates {
				condition := extractConditionName(cand)
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

	return ai.GenOutput{
		Text:       response,
		TokensUsed: len(prompt),
	}, nil
}

// EstimateCost estimates the cost of the generation.
func (g *LocalExpert) EstimateCost(inTokens, outTokens int) float64 {
	return 0
}

func extractConditionName(candidate string) string {
	// fmt.Printf("DEBUG: Extracting from '%s'\n", candidate)
	condition := strings.Split(candidate, ":")[0]
	if strings.HasPrefix(candidate, "Condition: ") {
		afterPrefix := strings.TrimPrefix(candidate, "Condition: ")
		if idx := strings.IndexAny(afterPrefix, ".:"); idx != -1 {
			condition = strings.TrimSpace(afterPrefix[:idx])
		} else {
			condition = afterPrefix
		}
	}
	return condition
}
