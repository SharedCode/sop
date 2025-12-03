package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
	"github.com/sharedcode/sop/ai/vector"
)

// IngestAgent performs the ETL process for a specific agent configuration.
func IngestAgent(ctx context.Context, configPath, dataFile, targetAgentID string) error {
	// 1. Load Configuration
	rootCfg, err := agent.LoadConfigFromFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Determine which config to use
	var cfg *agent.Config
	if targetAgentID != "" {
		// Look for the agent in the local agents list
		for _, a := range rootCfg.Agents {
			if a.ID == targetAgentID {
				cfg = &a
				break
			}
		}
		if cfg == nil {
			// Check if the root config itself matches
			if rootCfg.ID == targetAgentID {
				cfg = rootCfg
			} else {
				return fmt.Errorf("agent '%s' not found in configuration file", targetAgentID)
			}
		}
	} else {
		cfg = rootCfg
	}

	// Ensure absolute path for storage
	if cfg.StoragePath != "" {
		if !filepath.IsAbs(cfg.StoragePath) {
			configDir := filepath.Dir(configPath)
			cfg.StoragePath = filepath.Join(configDir, cfg.StoragePath)
		}
		if absPath, err := filepath.Abs(cfg.StoragePath); err == nil {
			cfg.StoragePath = absPath
		}
	}

	fmt.Printf("Starting ETL for Agent: %s (%s)...\n", cfg.Name, cfg.ID)
	fmt.Printf("Target Storage: %s\n", cfg.StoragePath)

	// 2. Initialize Infrastructure (Embedder & Vector DB)
	deps := agent.Dependencies{
		AgentRegistry: make(map[string]ai.Agent[map[string]any]),
	}

	// If the embedder is an agent, we need to load it.
	if cfg.Embedder.Type == "agent" {
		targetID := cfg.Embedder.AgentID
		fmt.Printf("Loading embedder agent: %s...\n", targetID)

		var depSvc ai.Agent[map[string]any]
		var depErr error

		// 1. Check if the dependency is defined inline in the root config
		var depCfg *agent.Config
		for _, a := range rootCfg.Agents {
			if a.ID == targetID {
				depCfg = &a
				break
			}
		}

		if depCfg != nil {
			// Found inline, initialize it
			if depCfg.StoragePath != "" && !filepath.IsAbs(depCfg.StoragePath) {
				depCfg.StoragePath = filepath.Join(filepath.Dir(configPath), depCfg.StoragePath)
			}
			depSvc, depErr = agent.NewFromConfig(ctx, *depCfg, agent.Dependencies{AgentRegistry: make(map[string]ai.Agent[map[string]any])})
		} else {
			// 2. Fallback to looking for a separate file
			configDir := filepath.Dir(configPath)
			depConfigPath := filepath.Join(configDir, fmt.Sprintf("%s.json", targetID))
			if _, err := os.Stat(depConfigPath); os.IsNotExist(err) {
				depConfigPath = fmt.Sprintf("ai/data/%s.json", targetID)
			}

			if _, err := os.Stat(depConfigPath); err == nil {
				depCfgFromFile, err := agent.LoadConfigFromFile(depConfigPath)
				if err != nil {
					return fmt.Errorf("failed to load dependency agent config: %w", err)
				}
				if depCfgFromFile.StoragePath != "" && !filepath.IsAbs(depCfgFromFile.StoragePath) {
					depCfgFromFile.StoragePath = filepath.Join(filepath.Dir(depConfigPath), depCfgFromFile.StoragePath)
				}
				depSvc, depErr = agent.NewFromConfig(ctx, *depCfgFromFile, agent.Dependencies{AgentRegistry: make(map[string]ai.Agent[map[string]any])})
			} else {
				// 3. Check if it's the root config itself
				if rootCfg.ID == targetID {
					depSvc, depErr = agent.NewFromConfig(ctx, *rootCfg, agent.Dependencies{AgentRegistry: make(map[string]ai.Agent[map[string]any])})
				} else {
					return fmt.Errorf("dependency agent '%s' not found in inline agents or as a file", targetID)
				}
			}
		}

		if depErr != nil {
			return fmt.Errorf("failed to initialize dependency agent: %w", depErr)
		}
		deps.AgentRegistry[targetID] = depSvc
	}

	emb, db, storeName, vCfg, err := agent.SetupInfrastructure(ctx, *cfg, deps)
	if err != nil {
		return fmt.Errorf("failed to setup infrastructure: %w", err)
	}

	// Start Transaction
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	idx, err := vector.Open[map[string]any](ctx, tx, storeName, vCfg)
	if err != nil {
		return fmt.Errorf("failed to open index: %w", err)
	}

	// 3. Load Data & Process in Batches
	batchSize := 200
	totalProcessed := 0

	processBatch := func(batch []agent.DataItem) error {
		if len(batch) == 0 {
			return nil
		}
		fmt.Printf("Processing batch of %d items (Total: %d)...\n", len(batch), totalProcessed+len(batch))

		texts := make([]string, len(batch))
		for i, item := range batch {
			texts[i] = fmt.Sprintf("%s %s", item.Text, item.Description)
		}

		vecs, err := emb.EmbedTexts(ctx, texts)
		if err != nil {
			return fmt.Errorf("failed to generate embeddings: %w", err)
		}

		items := make([]ai.Item[map[string]any], len(batch))
		for i, item := range batch {
			// Generate deterministic ID based on content if not provided or if we want to enforce it
			// We use the same hashing logic as in agent/ingest.go to ensure consistency
			contentToEmbed := fmt.Sprintf("%s %s", item.Text, item.Description)
			id := agent.HashString(contentToEmbed)

			items[i] = ai.Item[map[string]any]{
				ID:     id,
				Vector: vecs[i],
				Payload: map[string]any{
					"text":        item.Text,
					"description": item.Description,
					"original_id": item.ID, // Keep original ID in payload
				},
			}
		}

		if err := idx.UpsertBatch(ctx, items); err != nil {
			return fmt.Errorf("failed to upsert batch: %w", err)
		}
		totalProcessed += len(batch)
		return nil
	}

	if dataFile != "" {
		fmt.Printf("Streaming data from %s...\n", dataFile)
		f, err := os.Open(dataFile)
		if err != nil {
			return fmt.Errorf("failed to open data file: %w", err)
		}
		defer f.Close()

		dec := json.NewDecoder(f)

		// Read opening bracket
		t, err := dec.Token()
		if err != nil {
			return fmt.Errorf("failed to read opening token: %w", err)
		}
		if delim, ok := t.(json.Delim); !ok || delim != '[' {
			return fmt.Errorf("expected JSON array of items")
		}

		var batch []agent.DataItem
		for dec.More() {
			var item agent.DataItem
			if err := dec.Decode(&item); err != nil {
				return fmt.Errorf("failed to decode item: %w", err)
			}
			batch = append(batch, item)

			if len(batch) >= batchSize {
				if err := processBatch(batch); err != nil {
					return err
				}
				batch = batch[:0] // Clear batch
			}
		}

		// Process remaining
		if len(batch) > 0 {
			if err := processBatch(batch); err != nil {
				return err
			}
		}

		// Read closing bracket
		_, err = dec.Token()
		if err != nil {
			return fmt.Errorf("failed to read closing token: %w", err)
		}

	} else {
		fmt.Println("Using data from configuration file...")
		if len(cfg.Data) == 0 {
			fmt.Println("No data to ingest.")
			// We still commit if we opened a transaction, though it's empty.
		} else {
			// Process config data in batches too
			for i := 0; i < len(cfg.Data); i += batchSize {
				end := i + batchSize
				if end > len(cfg.Data) {
					end = len(cfg.Data)
				}
				if err := processBatch(cfg.Data[i:end]); err != nil {
					return err
				}
			}
		}
	}

	// Commit
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// 4. Auto-Optimize (if enabled)
	if cfg.AutoOptimize {
		fmt.Println("Auto-Optimize enabled. Running optimization...")
		// Optimization requires its own transaction management (it commits internally)
		// We need to open the store again in a new transaction context just to call Optimize.
		// Note: Optimize() takes a context and handles transactions internally, but we need an instance.

		txOpt, err := db.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return fmt.Errorf("failed to begin optimization transaction: %w", err)
		}
		// We don't defer rollback here because Optimize commits.
		// If Optimize fails, we might need to rollback manually if it didn't commit.

		idxOpt, err := vector.Open[map[string]any](ctx, txOpt, storeName, vCfg)
		if err != nil {
			if rbErr := txOpt.Rollback(ctx); rbErr != nil {
				return fmt.Errorf("failed to open index for optimization: %w, rollback failed: %v", err, rbErr)
			}
			return fmt.Errorf("failed to open index for optimization: %w", err)
		}

		if err := idxOpt.Optimize(ctx); err != nil {
			// Optimize might have committed or not depending on where it failed.
			// Attempt rollback just in case (safe to call if already committed/rolled back? SOP handles it?)
			// SOP transactions are usually safe to rollback if already committed (no-op).
			if rbErr := txOpt.Rollback(ctx); rbErr != nil {
				return fmt.Errorf("optimization failed: %w, rollback failed: %v", err, rbErr)
			}
			return fmt.Errorf("optimization failed: %w", err)
		}
		fmt.Println("Optimization complete.")
	}

	// 5. Verify (New Transaction)
	tx2, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		fmt.Printf("Warning: Failed to verify count (transaction error): %v\n", err)
		return nil
	}
	defer tx2.Rollback(ctx)

	idx2, err := vector.Open[map[string]any](ctx, tx2, storeName, vCfg)
	if err != nil {
		fmt.Printf("Warning: Failed to verify count (open error): %v\n", err)
		return nil
	}

	count, _ := idx2.Count(ctx)
	fmt.Printf("ETL Complete. Total items in DB: %d\n", count)
	return nil
}
