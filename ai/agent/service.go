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
	domain    ai.Domain[map[string]any]
	generator ai.Generator // The LLM (Gemini, etc.)
	pipeline  []PipelineStep
	registry  map[string]ai.Agent[map[string]any]
}

// NewService creates a new agent service for a specific domain.
func NewService(domain ai.Domain[map[string]any], generator ai.Generator, pipeline []PipelineStep, registry map[string]ai.Agent[map[string]any]) *Service {
	return &Service{
		domain:    domain,
		generator: generator,
		pipeline:  pipeline,
		registry:  registry,
	}
}

// Domain returns the underlying domain of the service.
func (s *Service) Domain() ai.Domain[map[string]any] {
	return s.domain
}

// evaluateInputPolicy checks the input against the domain's policies.
func (s *Service) evaluateInputPolicy(ctx context.Context, input string) error {
	if pol := s.domain.Policies(); pol != nil {
		classifier := s.domain.Classifier()
		if classifier != nil {
			sample := ai.ContentSample{Text: input}
			labels, err := classifier.Classify(ctx, sample)
			if err != nil {
				return fmt.Errorf("classification failed: %w", err)
			}
			decision, err := pol.Evaluate(ctx, "input", sample, labels)
			if err != nil {
				return fmt.Errorf("policy evaluation failed: %w", err)
			}
			if decision.Action == "block" {
				return fmt.Errorf("request blocked by policy: %v", decision.Reasons)
			}
		}
	}
	return nil
}

// Search performs a semantic search in the domain's knowledge base.
// It enforces policies and uses the domain's embedder.
func (s *Service) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	// 1. Policy Check (Input)
	if err := s.evaluateInputPolicy(ctx, query); err != nil {
		return nil, err
	}

	// 2. Embed
	emb := s.domain.Embedder()
	if emb == nil {
		return nil, fmt.Errorf("domain %s has no embedder configured", s.domain.ID())
	}
	vecs, err := emb.EmbedTexts(ctx, []string{query})
	if err != nil {
		return nil, fmt.Errorf("embedding failed: %w", err)
	}

	// 3. Query Index
	idx := s.domain.Index()
	if idx == nil {
		return nil, fmt.Errorf("domain %s has no index configured", s.domain.ID())
	}
	hits, err := idx.Query(ctx, vecs[0], limit, nil)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	return hits, nil
}

// RunPipeline executes the configured chain of agents.
func (s *Service) RunPipeline(ctx context.Context, input string) (string, error) {
	// Note: We do NOT call evaluateInputPolicy here anymore.
	// Policies should be explicitly added as steps in the pipeline if desired.
	// This allows for more flexible policy application (e.g. input, output, intermediate).

	currentInput := input

	for _, step := range s.pipeline {
		agent, ok := s.registry[step.Agent.ID]
		if !ok {
			return "", fmt.Errorf("pipeline agent '%s' not found in registry", step.Agent.ID)
		}

		output, err := agent.Ask(ctx, currentInput)
		if err != nil {
			return "", fmt.Errorf("pipeline step '%s' failed: %w", step.Agent.ID, err)
		}

		if step.OutputTo == "context" {
			// Append context to the input for the next step so it's available
			currentInput = fmt.Sprintf("%s\n\nContext from %s:\n%s", currentInput, step.Agent.ID, output)
		} else {
			// Default or "next_step": The output becomes the input for the next agent
			currentInput = output
		}
	}
	return currentInput, nil
}

// Ask performs a RAG (Retrieval-Augmented Generation) request.
func (s *Service) Ask(ctx context.Context, query string) (string, error) {
	// 0. Pipeline Execution (if configured)
	if len(s.pipeline) > 0 {
		return s.RunPipeline(ctx, query)
	}

	// 1. Search for context
	hits, err := s.Search(ctx, query, 10)
	if err != nil {
		return "", fmt.Errorf("retrieval failed: %w", err)
	}

	// 2. Construct Prompt
	contextText := s.formatContext(hits)
	systemPrompt, _ := s.domain.Prompt(ctx, "system")

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

		// If we have a generator or a pipeline, try to generate a response (RAG or Pipeline)
		if s.generator != nil || len(s.pipeline) > 0 {
			answer, err := s.Ask(ctx, query)
			if err == nil {
				fmt.Fprintf(w, "\nAI Doctor: %s\n", answer)
				continue
			}
			// If generation/pipeline fails, fall back to search results
			// We suppress the error to avoid confusing the user if the LLM is offline
			// But for pipeline errors, we might want to show them?
			// For now, let's print them if it's a pipeline error
			if len(s.pipeline) > 0 {
				fmt.Fprintf(w, "Pipeline Error: %v\n", err)
			}
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
			text, _ := hit.Payload["text"].(string)
			desc, _ := hit.Payload["description"].(string)

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

func (s *Service) formatContext(hits []ai.Hit[map[string]any]) string {
	var sb strings.Builder
	for i, hit := range hits {
		// Generic handling of metadata
		sb.WriteString(fmt.Sprintf("[%d] ", i+1))

		text, hasText := hit.Payload["text"].(string)
		desc, hasDesc := hit.Payload["description"].(string)

		if hasText && hasDesc {
			sb.WriteString(fmt.Sprintf("%s: %s (Score: %.2f)", text, desc, hit.Score))
		} else if hasDesc {
			sb.WriteString(fmt.Sprintf("%s (Score: %.2f)", desc, hit.Score))
		} else if hasText {
			sb.WriteString(fmt.Sprintf("%s (Score: %.2f)", text, hit.Score))
		} else {
			sb.WriteString(fmt.Sprintf("%v (Score: %.2f)", hit.Payload, hit.Score))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}
