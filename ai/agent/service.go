package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/obfuscation"
	"github.com/sharedcode/sop/search"
)

// Service is a generic agent service that operates on any Domain.
type Service struct {
	domain            ai.Domain[map[string]any]
	generator         ai.Generator // The LLM (Gemini, etc.)
	pipeline          []PipelineStep
	registry          map[string]ai.Agent[map[string]any]
	EnableObfuscation bool
}

// NewService creates a new agent service for a specific domain.
func NewService(domain ai.Domain[map[string]any], generator ai.Generator, pipeline []PipelineStep, registry map[string]ai.Agent[map[string]any], enableObfuscation bool) *Service {
	return &Service{
		domain:            domain,
		generator:         generator,
		pipeline:          pipeline,
		registry:          registry,
		EnableObfuscation: enableObfuscation,
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
	tx, err := s.domain.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	idx, err := s.domain.Index(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("domain %s has no index configured: %w", s.domain.ID(), err)
	}

	// Vector Search
	vectorHits, err := idx.Query(ctx, vecs[0], limit, nil)
	if err != nil {
		return nil, fmt.Errorf("vector query failed: %w", err)
	}

	// Text Search
	textIdx, err := s.domain.TextIndex(ctx, tx)
	var textHits []search.TextSearchResult
	if err == nil && textIdx != nil {
		textHits, err = textIdx.Search(ctx, query)
		if err != nil {
			// Log error but continue with vector results?
			// For now, let's treat it as non-fatal if text index is missing or fails,
			// but maybe we should log it.
			// fmt.Printf("Text search failed: %v\n", err)
		}
	}

	// Hybrid Fusion (RRF)
	k := 60.0
	scores := make(map[string]float64)
	payloads := make(map[string]map[string]any)

	// Process Vector Hits
	for rank, hit := range vectorHits {
		scores[hit.ID] += 1.0 / (k + float64(rank+1))
		payloads[hit.ID] = hit.Payload
	}

	// Process Text Hits
	for rank, hit := range textHits {
		scores[hit.DocID] += 1.0 / (k + float64(rank+1))
		// If payload missing, we need to fetch it
		if _, ok := payloads[hit.DocID]; !ok {
			item, err := idx.Get(ctx, hit.DocID)
			if err == nil && item != nil {
				payloads[hit.DocID] = item.Payload
			}
		}
	}

	// Construct Final Results
	var results []ai.Hit[map[string]any]
	for id, score := range scores {
		if payload, ok := payloads[id]; ok {
			results = append(results, ai.Hit[map[string]any]{
				ID:      id,
				Score:   float32(score),
				Payload: payload,
			})
		}
	}

	// Sort by Score Descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Limit
	if len(results) > limit {
		results = results[:limit]
	}

	return results, nil
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
	// Obfuscate Input
	// We ONLY obfuscate known resource names (Database, Store) that have been registered.
	// We do NOT obfuscate the entire text blindly, but ObfuscateText only replaces known keys.
	// If the user says "select from Python Complex DB", and "Python Complex DB" is registered,
	// it becomes "select from DB_123". This is correct.
	// The LLM sees "select from DB_123".

	// Obfuscate Query if enabled
	if s.EnableObfuscation {
		query = obfuscation.GlobalObfuscator.ObfuscateText(query)
	}

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

	// If obfuscation is enabled, we should obfuscate the context too.
	// This ensures that if the vector store returns real names, they are hidden from the LLM.
	if s.EnableObfuscation {
		contextText = obfuscation.GlobalObfuscator.ObfuscateText(contextText)
	}

	fullPrompt := fmt.Sprintf("%s\n\nContext:\n%s\n\nUser Query: %s", systemPrompt, contextText, query)
	if s.EnableObfuscation {
	}

	// 3. Determine Generator
	gen := s.generator

	// Check for override in context
	if provider, ok := ctx.Value(ai.CtxKeyProvider).(string); ok && provider != "" {
		// Only override if the requested provider is different from the current one
		// (We assume s.generator.Name() matches the provider string, e.g. "gemini", "ollama")
		if gen == nil || gen.Name() != provider {
			// Create a temporary generator instance
			// We rely on the generator package to pick up API keys from Env Vars
			overriddenGen, err := generator.New(provider, nil)
			if err == nil {
				gen = overriddenGen
			} else {
				// Log warning? For now, just fall back to default
				fmt.Printf("Warning: Failed to initialize requested provider '%s': %v. Falling back to default.\n", provider, err)
			}
		}
	}

	// 4. Generate Answer
	if gen == nil {
		// Fallback: If no generator is configured, return the retrieved context directly.
		// This allows agents to act as "Search Services" or "Translators" without an LLM.
		if s.EnableObfuscation {
			return obfuscation.GlobalObfuscator.DeobfuscateText(contextText), nil
		}
		return contextText, nil
	}

	output, err := gen.Generate(ctx, fullPrompt, ai.GenOptions{
		MaxTokens:   1024,
		Temperature: 0.7,
	})
	if err != nil {
		return "", fmt.Errorf("generation failed: %w", err)
	}
	if s.EnableObfuscation {
	}

	// 5. Check for Tool Execution (Agent -> App)
	// If the generator returns a JSON tool call, and we have an executor, run it.
	if executor, ok := ctx.Value(ai.CtxKeyExecutor).(ai.ToolExecutor); ok && executor != nil {
		// Simple heuristic: If output looks like a JSON tool call
		text := strings.TrimSpace(output.Text)
		// Remove markdown code blocks if present
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)

		if strings.HasPrefix(text, "{") && strings.Contains(text, "\"tool\"") {
			// De-obfuscate Tool Arguments
			// We need to parse, de-obfuscate, and re-serialize (or just return the de-obfuscated JSON)
			// Since we return text, we should return the de-obfuscated JSON so the caller can execute it.

			// 1. Parse JSON FIRST to get the exact values the LLM returned
			var toolCall struct {
				Tool string         `json:"tool"`
				Args map[string]any `json:"args"`
			}

			// We try to unmarshal the text directly.
			// If the LLM returned valid JSON (even with obfuscated values), this will succeed.
			if err := json.Unmarshal([]byte(text), &toolCall); err == nil {
				// 2. Sanitize Args
				for k, v := range toolCall.Args {
					if val, ok := v.(string); ok {
						// a. Remove Markdown bold/italics/code wrappers from the value itself
						// LLM might return: "database": "**DB_123**" or "`DB_123`"
						val = strings.Trim(val, "*_`")

						// b. Replace NBSP with space and Trim whitespace
						val = strings.ReplaceAll(val, "\u00a0", " ")
						val = strings.TrimSpace(val)

						toolCall.Args[k] = val
					}
				}
				// Re-serialize
				if b, err := json.Marshal(toolCall); err == nil {
					cleanJSON := string(b)
					return cleanJSON, nil
				}
			}

			// Fallback: If JSON parsing failed (maybe invalid JSON), return as is
			return text, nil
		}
	}

	// De-obfuscate Output Text
	if s.EnableObfuscation {
		return obfuscation.GlobalObfuscator.DeobfuscateText(output.Text), nil
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
