package agent

import (
	"fmt"
	"hash/fnv"
	"io"
	"path/filepath"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/domain"
	"github.com/sharedcode/sop/ai/embed"
	"github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/policy"
	"github.com/sharedcode/sop/ai/vector"
)

// Dependencies holds external dependencies required for agent creation.
type Dependencies struct {
	AgentRegistry map[string]ai.Agent[map[string]any]
}

// HashString generates a deterministic hash for the given string.
func HashString(s string) string {
	h := fnv.New64a()
	io.WriteString(h, s)
	return fmt.Sprintf("%x", h.Sum64())
}

// SetupInfrastructure initializes the Embedder and Vector Index based on the configuration.
func SetupInfrastructure(cfg Config, deps Dependencies) (ai.Embeddings, ai.VectorStore[map[string]any], error) {
	// 1. Initialize Embedder
	var emb ai.Embeddings

	switch cfg.Embedder.Type {
	case "agent":
		agent, ok := deps.AgentRegistry[cfg.Embedder.AgentID]
		if !ok {
			return nil, nil, fmt.Errorf("embedder agent '%s' not found in registry", cfg.Embedder.AgentID)
		}
		// Use a simple base embedder for the vectors, but the agent will expand the text first
		baseEmb := embed.NewSimple(cfg.ID+"-base-embed", 1024, nil)
		emb = embed.NewAgentEmbedder(agent, baseEmb, cfg.Embedder.Instruction)

	case "ollama":
		baseURL, _ := cfg.Embedder.Options["base_url"].(string)
		model, _ := cfg.Embedder.Options["model"].(string)
		emb = embed.NewOllama(baseURL, model)

	default:
		// Default: Simple Embedder with domain-specific synonyms
		// We use a higher dimensionality (1024) to reduce collisions in the simple hash embedder
		emb = embed.NewSimple(cfg.ID+"-embed", 1024, cfg.Synonyms)
	}

	// 2. Initialize Vector Database
	db := vector.NewDatabase[map[string]any](ai.Standalone)
	if cfg.ContentSize != "" {
		switch cfg.ContentSize {
		case "small":
			db.SetContentSize(sop.SmallData)
		case "medium":
			db.SetContentSize(sop.MediumData)
		case "big":
			db.SetContentSize(sop.BigData)
		}
	}
	if cfg.StoragePath != "" {
		// Ensure absolute path to avoid duplication issues with relative paths
		if absPath, err := filepath.Abs(cfg.StoragePath); err == nil {
			cfg.StoragePath = absPath
		}

		// Fix for double domain in path:
		// If the storage path ends with the Agent ID, assume the user meant "this is my folder"
		// and point the DB to the parent, so DB.Open(ID) reconstructs it correctly.
		if filepath.Base(cfg.StoragePath) == cfg.ID {
			db.SetStoragePath(filepath.Dir(cfg.StoragePath))
		} else {
			db.SetStoragePath(cfg.StoragePath)
		}
	}
	idx := db.Open(cfg.ID)

	return emb, idx, nil
}

// NewFromConfig creates and initializes a new Agent Service based on the provided configuration.
// It handles infrastructure setup (Embedder, VectorDB).
func NewFromConfig(cfg Config, deps Dependencies) (*Service, error) {
	// 1. Initialize Infrastructure
	emb, idx, err := SetupInfrastructure(cfg, deps)
	if err != nil {
		return nil, err
	}

	// Initialize Generator (LLM)
	var gen ai.Generator
	if cfg.Generator.Type != "" {
		var err error
		gen, err = generator.New(cfg.Generator.Type, cfg.Generator.Options)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize generator: %w", err)
		}
	}

	// Policies & Classifier
	var pol ai.PolicyEngine
	var class ai.Classifier

	// Create a local registry for policy agents
	policyRegistry := make(map[string]ai.Agent[map[string]any])

	for _, pCfg := range cfg.Policies {
		if pCfg.Type == "profanity" {
			// Use configured max strikes, default to 3 if 0
			strikes := pCfg.MaxStrikes
			if strikes <= 0 {
				strikes = 3
			}
			// Create the policy engine and classifier
			pEngine, pClass := policy.NewProfanityGuardrail(strikes)

			// If this is the "main" policy (no ID or first one), use it for the domain
			if pol == nil {
				pol = pEngine
				class = pClass
			}

			// If an ID is provided, register it as a PolicyAgent
			if pCfg.ID != "" {
				policyAgent := NewPolicyAgent(pCfg.ID, pEngine, pClass)
				policyRegistry[pCfg.ID] = policyAgent
			}
		}
	}

	// Merge policy registry into the main registry
	// We create a new map to avoid modifying the passed dependencies
	fullRegistry := make(map[string]ai.Agent[map[string]any])
	for k, v := range deps.AgentRegistry {
		fullRegistry[k] = v
	}
	for k, v := range policyRegistry {
		fullRegistry[k] = v
	}

	// 2. Create Domain
	dom := domain.NewGenericDomain(domain.Config[map[string]any]{
		ID:         cfg.ID,
		Name:       cfg.Name,
		Index:      idx,
		Embedder:   emb,
		Policy:     pol,
		Classifier: class,
		Prompts: map[string]string{
			"system": cfg.SystemPrompt,
		},
	})

	// 3. Ingest Data (if present)
	if len(cfg.Data) > 0 {
		texts := make([]string, len(cfg.Data))
		for i, item := range cfg.Data {
			// Combine text and description for richer embedding context
			texts[i] = fmt.Sprintf("%s %s", item.Text, item.Description)
		}

		vecs, err := emb.EmbedTexts(texts)
		if err != nil {
			return nil, fmt.Errorf("failed to embed seed data: %w", err)
		}

		items := make([]ai.Item[map[string]any], len(cfg.Data))
		for i, item := range cfg.Data {
			items[i] = ai.Item[map[string]any]{
				ID:     item.ID,
				Vector: vecs[i],
				Payload: map[string]any{
					"text":        item.Text,
					"description": item.Description,
				},
			}
		}

		if err := idx.UpsertBatch(items); err != nil {
			return nil, fmt.Errorf("failed to ingest seed data: %w", err)
		}
	}

	// 4. Create Agent Service
	svc := NewService(dom, gen, cfg.Pipeline, fullRegistry)
	return svc, nil
}
