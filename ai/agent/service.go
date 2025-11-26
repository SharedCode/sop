package agent

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// Service is a generic agent service that operates on any Domain.
type Service struct {
	domain    ai.Domain
	generator ai.Generator // The LLM (Gemini, etc.)
}

// NewService creates a new agent service for a specific domain.
func NewService(domain ai.Domain, generator ai.Generator) *Service {
	return &Service{
		domain:    domain,
		generator: generator,
	}
}

// Search performs a semantic search in the domain's knowledge base.
// It enforces policies and uses the domain's embedder.
func (s *Service) Search(ctx context.Context, query string, limit int) ([]ai.Hit, error) {
	// 1. Policy Check (Input)
	if pol := s.domain.Policies(); pol != nil {
		classifier := s.domain.Classifier()
		if classifier != nil {
			sample := ai.ContentSample{Text: query}
			labels, err := classifier.Classify(sample)
			if err != nil {
				return nil, fmt.Errorf("classification failed: %w", err)
			}
			decision, err := pol.Evaluate("input", sample, labels)
			if err != nil {
				return nil, fmt.Errorf("policy evaluation failed: %w", err)
			}
			if decision.Action == "block" {
				return nil, fmt.Errorf("request blocked by policy: %v", decision.Reasons)
			}
		}
	}

	// 2. Embed
	emb := s.domain.Embedder()
	if emb == nil {
		return nil, fmt.Errorf("domain %s has no embedder configured", s.domain.ID())
	}
	vecs, err := emb.EmbedTexts([]string{query})
	if err != nil {
		return nil, fmt.Errorf("embedding failed: %w", err)
	}

	// 3. Query Index
	idx := s.domain.Index()
	if idx == nil {
		return nil, fmt.Errorf("domain %s has no index configured", s.domain.ID())
	}
	hits, err := idx.Query(vecs[0], limit, nil)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	return hits, nil
}

// Ask performs a RAG (Retrieval-Augmented Generation) request.
func (s *Service) Ask(ctx context.Context, query string) (string, error) {
	// 1. Search for context
	hits, err := s.Search(ctx, query, 5)
	if err != nil {
		return "", fmt.Errorf("retrieval failed: %w", err)
	}

	// 2. Construct Prompt
	contextText := s.formatContext(hits)
	systemPrompt, _ := s.domain.Prompt("system")

	fullPrompt := fmt.Sprintf("%s\n\nContext:\n%s\n\nUser Query: %s", systemPrompt, contextText, query)

	// 3. Generate Answer
	if s.generator == nil {
		// Fallback: If no generator is configured, return the retrieved context directly.
		// This allows agents to act as "Search Services" or "Translators" without an LLM.
		return contextText, nil
	}

	output, err := s.generator.Generate(ctx, fullPrompt, ai.GenOptions{
		MaxTokens:   1024,
		Temperature: 0.7,
	})
	if err != nil {
		return "", fmt.Errorf("generation failed: %w", err)
	}

	return output.Text, nil
}

// RunLoop starts an interactive Read-Eval-Print Loop (REPL) for the agent.
// It reads user input from r, processes it (using RAG if a generator is available,
// or simple search if not), and writes the response to w.
// The loop continues until the user enters "exit" or the input stream ends.
func (s *Service) RunLoop(ctx context.Context, r io.Reader, w io.Writer) error {
	scanner := bufio.NewScanner(r)

	for {
		fmt.Fprint(w, "\nPatient> ")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "exit" {
			break
		}
		if input == "reset" {
			fmt.Fprint(w, "\033[H\033[2J")
			continue
		}
		if input == "" {
			continue
		}

		query := input

		// If we have a generator, try to generate a response (RAG)
		if s.generator != nil {
			answer, err := s.Ask(ctx, query)
			if err == nil {
				fmt.Fprintf(w, "\nAI Doctor: %s\n", answer)
				continue
			}
			// If generation fails, fall back to search results
			// We suppress the error to avoid confusing the user if the LLM is offline
		}

		// Fallback to simple search if no generator is configured or if generation failed
		hits, err := s.Search(ctx, query, 3)
		if err != nil {
			fmt.Fprintf(w, "Error: %v\n", err)
			// Check if the error is a policy block that should terminate the session
			if strings.Contains(err.Error(), "Session terminated") {
				return nil
			}
			continue
		}

		fmt.Fprintf(w, "Found %d relevant entries for '%s':\n", len(hits), query)
		for i, hit := range hits {
			text, _ := hit.Meta["text"].(string)
			desc, _ := hit.Meta["description"].(string)

			if text != "" && desc != "" {
				// Apply display formatting: quote text if it contains spaces
				displayText := text
				if strings.Contains(text, " ") {
					displayText = fmt.Sprintf("\"%s\"", text)
				}
				fmt.Fprintf(w, "[%d] %s\n    %s (Score: %.2f)\n", i+1, displayText, desc, hit.Score)
			} else if desc != "" {
				fmt.Fprintf(w, "[%d] %s (Score: %.2f)\n", i+1, desc, hit.Score)
			} else {
				fmt.Fprintf(w, "[%d] %s (Score: %.2f)\n", i+1, text, hit.Score)
			}
		}
	}
	return scanner.Err()
}

func (s *Service) formatContext(hits []ai.Hit) string {
	var sb strings.Builder
	for i, hit := range hits {
		// Generic handling of metadata
		sb.WriteString(fmt.Sprintf("[%d] ", i+1))
		if desc, ok := hit.Meta["description"].(string); ok {
			sb.WriteString(desc)
		} else if text, ok := hit.Meta["text"].(string); ok {
			sb.WriteString(text)
		} else {
			sb.WriteString(fmt.Sprintf("%v", hit.Meta))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}
