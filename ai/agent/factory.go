package agent

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"path/filepath"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/domain"
	"github.com/sharedcode/sop/ai/embed"
	"github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/policy"
	"github.com/sharedcode/sop/ai/vector"
)

// Dependencies holds external dependencies required for agent creation.
type Dependencies struct {
	AgentRegistry map[string]ai.Agent[map[string]any]
	SystemDB      *database.Database
}

// HashString generates a deterministic hash for the given string.
func HashString(s string) string {
	h := fnv.New64a()
	io.WriteString(h, s)
	return fmt.Sprintf("%x", h.Sum64())
}

// SetupInfrastructure initializes the Embedder and Vector Index based on the configuration.
func SetupInfrastructure(ctx context.Context, cfg Config, deps Dependencies) (ai.Embeddings, *database.Database, string, vector.Config, error) {
	// 1. Initialize Embedder
	var emb ai.Embeddings

	switch cfg.Embedder.Type {
	case "agent":
		agent, ok := deps.AgentRegistry[cfg.Embedder.AgentID]
		if !ok {
			return nil, nil, "", vector.Config{}, fmt.Errorf("embedder agent '%s' not found in registry", cfg.Embedder.AgentID)
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
	storagePath := cfg.StoragePath
	if storagePath != "" {
		// Ensure absolute path to avoid duplication issues with relative paths
		if absPath, err := filepath.Abs(storagePath); err == nil {
			storagePath = absPath
		}

		// Fix for double domain in path:
		// If the storage path ends with the Agent ID, assume the user meant "this is my folder"
		// and point the DB to the parent, so DB.Open(ID) reconstructs it correctly.
		if filepath.Base(storagePath) == cfg.ID {
			storagePath = filepath.Dir(storagePath)
		}
	}

	// Determine Database Type
	var dbType sop.DatabaseType
	switch cfg.DBType {
	case "clustered":
		dbType = sop.Clustered
	case "standalone":
		dbType = sop.Standalone
	default:
		dbType = sop.Standalone
	}

	// Vector database does not support Replication disk structure, ignore error.
	db := database.NewDatabase(sop.DatabaseOptions{
		StoresFolders: []string{storagePath},
		Type:          dbType,
	})

	vCfg := vector.Config{
		UsageMode:             ai.BuildOnceQueryMany, // Default
		EnableIngestionBuffer: cfg.EnableIngestionBuffer,
		TransactionOptions: sop.TransactionOptions{
			StoresFolders: []string{storagePath},
			CacheType:     db.CacheType(),
		},
		Cache: db.Cache(),
	}

	if cfg.ContentSize != "" {
		switch cfg.ContentSize {
		case "small":
			vCfg.ContentSize = sop.SmallData
		case "medium":
			vCfg.ContentSize = sop.MediumData
		case "big":
			vCfg.ContentSize = sop.BigData
		}
	}

	return emb, db, cfg.ID, vCfg, nil
}

// NewFromConfig creates and initializes a new Agent Service based on the provided configuration.
// It handles infrastructure setup (Embedder, VectorDB).
func NewFromConfig(ctx context.Context, cfg Config, deps Dependencies) (ai.Agent[map[string]any], error) {
	// Handle specialized agent types
	switch cfg.Type {
	case "data-admin":
		return NewDataAdminAgent(cfg), nil
	// Add other types here
	case "standard", "":
		// Fallthrough to standard service creation
	default:
		// For now, default to standard service if unknown, or could return error
		// return nil, fmt.Errorf("unknown agent type: %s", cfg.Type)
	}

	// 1. Initialize Infrastructure
	emb, db, storeName, vCfg, err := SetupInfrastructure(ctx, cfg, deps)
	if err != nil {
		return nil, err
	}

	// Initialize Generator (LLM)
	var gen ai.Generator
	if cfg.Generator.Type != "" {
		var err error
		// Pass global obfuscation setting to generator options
		if cfg.EnableObfuscation {
			if cfg.Generator.Options == nil {
				cfg.Generator.Options = make(map[string]any)
			}
			cfg.Generator.Options["enable_obfuscation"] = true
		}
		gen, err = generator.New(cfg.Generator.Type, cfg.Generator.Options)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize generator: %w", err)
		}
	}

	// Policies & Classifier
	var pol ai.PolicyEngine
	var class ai.Classifier

	// Create a local registry for policy agents and sub-agents
	policyRegistry := make(map[string]ai.Agent[map[string]any])

	// Initialize Sub-Agents defined in Config
	for _, subCfg := range cfg.Agents {
		// Recursively create sub-agent
		// We pass the current dependencies, but we might need to handle circular deps if any
		subAgent, err := NewFromConfig(ctx, subCfg, deps)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize sub-agent '%s': %w", subCfg.ID, err)
		}
		policyRegistry[subCfg.ID] = subAgent
	}

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
		DB:         db,
		StoreName:  storeName,
		StoreCfg:   vCfg,
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

		vecs, err := emb.EmbedTexts(ctx, texts)
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

		// Start Transaction for Ingestion
		tx, err := dom.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		// Ensure rollback on error, but commit on success
		defer tx.Rollback(ctx)

		idx, err := dom.Index(ctx, tx)
		if err != nil {
			return nil, fmt.Errorf("failed to open index: %w", err)
		}

		if err := idx.UpsertBatch(ctx, items); err != nil {
			return nil, fmt.Errorf("failed to ingest seed data: %w", err)
		}

		// Text Indexing
		textIdx, err := dom.TextIndex(ctx, tx)
		if err != nil {
			return nil, fmt.Errorf("failed to open text index: %w", err)
		}
		for i, item := range cfg.Data {
			// Index the combined text
			if err := textIdx.Add(ctx, item.ID, texts[i]); err != nil {
				return nil, fmt.Errorf("failed to index text for item %s: %w", item.ID, err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit ingestion: %w", err)
		}
	}

	// 4. Create Agent Service
	// If the generator is "data-admin" and obfuscation is enabled, the generator handles it internally.
	// Therefore, we disable Service-level obfuscation to avoid double-obfuscation.
	serviceObfuscation := cfg.EnableObfuscation
	if cfg.Generator.Type == "data-admin" && cfg.EnableObfuscation {
		serviceObfuscation = false
	}

	svc := NewService(dom, deps.SystemDB, gen, cfg.Pipeline, fullRegistry, serviceObfuscation)
	return svc, nil
}
