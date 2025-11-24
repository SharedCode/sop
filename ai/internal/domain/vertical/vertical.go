package vertical

import (
	"context"
	"fmt"
	"strings"

	"github.com/sharedcode/sop/ai/internal/port"
)

type Vertical struct {
	id          string
	embedder    port.Embeddings
	index       port.VectorIndex
	policies    port.PolicyEngine
	classifiers []port.Classifier
	generator   port.Generator
	prompts     map[string]string
}

func New(id string, embedder port.Embeddings, index port.VectorIndex, policies port.PolicyEngine, generator port.Generator, classifiers ...port.Classifier) *Vertical {
	return &Vertical{
		id:          id,
		embedder:    embedder,
		index:       index,
		policies:    policies,
		generator:   generator,
		classifiers: classifiers,
		prompts:     make(map[string]string),
	}
}

func (v *Vertical) ID() string {
	return v.id
}

func (v *Vertical) Embedder() port.Embeddings {
	return v.embedder
}

func (v *Vertical) Index() port.VectorIndex {
	return v.index
}

func (v *Vertical) Policies() port.PolicyEngine {
	return v.policies
}

func (v *Vertical) Prompt(kind string) (string, error) {
	if p, ok := v.prompts[kind]; ok {
		return p, nil
	}
	return "", fmt.Errorf("prompt kind %q not found in vertical %q", kind, v.id)
}

func (v *Vertical) SetPrompt(kind, prompt string) {
	v.prompts[kind] = prompt
}

// Process handles a user query by embedding it, retrieving relevant context,
// constructing a prompt, and generating a response.
func (v *Vertical) Process(ctx context.Context, query string) (string, error) {
	// 0. Input Guardrails
	if err := v.checkSafety("input", query); err != nil {
		return "", err
	}

	// 1. Embed the query
	vecs, err := v.embedder.EmbedTexts([]string{query})
	if err != nil {
		return "", fmt.Errorf("embedding failed: %w", err)
	}
	if len(vecs) == 0 {
		return "", fmt.Errorf("no embedding generated")
	}
	queryVec := vecs[0]

	// 2. Retrieve relevant context
	// We assume a default top-k of 3 for now.
	hits, err := v.index.Query(queryVec, 3, nil)
	if err != nil {
		return "", fmt.Errorf("index query failed: %w", err)
	}

	// 3. Construct context string
	var contextStr string
	for _, hit := range hits {
		if text, ok := hit.Meta["text"].(string); ok {
			contextStr += text + "\n"
		}
	}

	// 4. Construct Prompt
	// We look for a "rag" prompt template, or default to a simple one.
	tmpl, err := v.Prompt("rag")
	if err != nil {
		// Default template if not found
		tmpl = "Context:\n{{context}}\n\nQuestion: {{query}}\n\nAnswer:"
	}

	// Simple template replacement (in a real app, use text/template)
	prompt := strings.ReplaceAll(tmpl, "{{context}}", contextStr)
	prompt = strings.ReplaceAll(prompt, "{{query}}", query)
	prompt = strings.ReplaceAll(prompt, "{{engine}}", v.generator.Name())

	// 5. Generate Response
	out, err := v.generator.Generate(ctx, prompt, port.GenOptions{})
	if err != nil {
		return "", fmt.Errorf("generation failed: %w", err)
	}

	// 6. Output Guardrails
	if err := v.checkSafety("output", out.Text); err != nil {
		return "", err
	}

	return out.Text, nil
}

func (v *Vertical) checkSafety(stage, text string) error {
	sample := port.ContentSample{Text: text}
	var allLabels []port.Label
	for _, c := range v.classifiers {
		labels, err := c.Classify(sample)
		if err != nil {
			return fmt.Errorf("classifier %q failed: %w", c.Name(), err)
		}
		allLabels = append(allLabels, labels...)
	}

	decision, err := v.policies.Evaluate(stage, sample, allLabels)
	if err != nil {
		return fmt.Errorf("policy evaluation failed: %w", err)
	}
	if decision.Action == "block" {
		return fmt.Errorf("content blocked: %v", decision.Reasons)
	}
	return nil
}
