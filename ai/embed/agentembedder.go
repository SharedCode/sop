package embed

import (
	"context"
	"fmt"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// AgentEmbedder wraps an AI Agent to use its understanding for embedding.
// It asks the agent to "summarize" or "extract concepts" from the text,
// and then embeds those concepts using a base embedder (like Simple).
type AgentEmbedder struct {
	agent       ai.Agent      // The agent that "understands" the text
	base        ai.Embeddings // The embedder that turns concepts into vectors
	instruction string        // e.g., "Extract the medical condition from this text:"
}

// NewAgentEmbedder creates a new embedder that uses an agent to preprocess text.
func NewAgentEmbedder(agent ai.Agent, base ai.Embeddings, instruction string) *AgentEmbedder {
	if instruction == "" {
		instruction = "Extract the core concepts from this text:"
	}
	return &AgentEmbedder{
		agent:       agent,
		base:        base,
		instruction: instruction,
	}
}

// Name returns the name of the embedder.
func (ae *AgentEmbedder) Name() string {
	return fmt.Sprintf("agent-enhanced-%s", ae.base.Name())
}

// Dim returns the dimension of the embeddings.
func (ae *AgentEmbedder) Dim() int {
	return ae.base.Dim()
}

// EmbedTexts generates embeddings for the given texts.
// It first enhances the texts using the agent, then embeds the enhanced texts using the base embedder.
func (ae *AgentEmbedder) EmbedTexts(texts []string) ([][]float32, error) {
	// 1. Preprocess texts using the Agent
	enhancedTexts := make([]string, len(texts))
	for i, text := range texts {
		// We use the agent's Ask method to get a conceptual summary
		// Note: In a real high-throughput system, we'd want a batch API for the agent.
		prompt := fmt.Sprintf("%s\n\nInput: %s", ae.instruction, text)

		// We use a background context here, but ideally this should be passed in.
		// Since EmbedTexts signature doesn't have context, we have to improvise or change the interface.
		// For this MVP, we'll assume the agent is fast enough.
		concept, err := ae.agent.Ask(context.Background(), prompt)
		if err != nil {
			// Fallback to original text if agent fails
			enhancedTexts[i] = text
		} else {
			// Combine original text with concepts for richer embedding
			// or just use concepts depending on strategy.
			// Here we append concepts to give them weight.
			enhancedTexts[i] = fmt.Sprintf("%s %s", text, strings.TrimSpace(concept))
		}
	}

	// 2. Embed the enhanced texts using the base embedder
	return ae.base.EmbedTexts(enhancedTexts)
}
