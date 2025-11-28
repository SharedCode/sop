package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
)

func main() {
	configPath := flag.String("config", "", "Path to the agent configuration JSON file")
	dataFile := flag.String("data", "", "Path to the data file (JSON array of items) to ingest. If not provided, uses data from config.")
	targetAgentID := flag.String("agent", "", "Optional: ID of the specific agent within the config to target for ETL.")
	flag.Parse()

	if *configPath == "" {
		fmt.Println("Usage: go run ai/cmd/etl/main.go -config <path_to_config.json> [-data <path_to_data.json>] [-agent <agent_id>]")
		os.Exit(1)
	}

	// 1. Load Configuration
	rootCfg, err := agent.LoadConfigFromFile(*configPath)
	if err != nil {
		panic(fmt.Errorf("failed to load config: %w", err))
	}

	// Determine which config to use
	var cfg *agent.Config
	if *targetAgentID != "" {
		// Look for the agent in the local agents list
		for _, a := range rootCfg.Agents {
			if a.ID == *targetAgentID {
				cfg = &a
				break
			}
		}
		if cfg == nil {
			// Check if the root config itself matches (though unlikely if user specified -agent)
			if rootCfg.ID == *targetAgentID {
				cfg = rootCfg
			} else {
				panic(fmt.Errorf("agent '%s' not found in configuration file", *targetAgentID))
			}
		}
	} else {
		cfg = rootCfg
	}

	// Ensure absolute path for storage
	if cfg.StoragePath != "" {
		if !filepath.IsAbs(cfg.StoragePath) {
			configDir := filepath.Dir(*configPath)
			cfg.StoragePath = filepath.Join(configDir, cfg.StoragePath)
		}
		if absPath, err := filepath.Abs(cfg.StoragePath); err == nil {
			cfg.StoragePath = absPath
		}
	}

	fmt.Printf("Starting ETL for Agent: %s (%s)...\n", cfg.Name, cfg.ID)
	fmt.Printf("Target Storage: %s\n", cfg.StoragePath)

	// 2. Initialize Infrastructure (Embedder & Vector DB)
	// We use a dummy registry because ETL usually doesn't need full agent dependencies,
	// UNLESS the embedder itself is an agent (like nurse_local).
	// For production ETL, we might need to load those dependencies too.
	// For now, let's assume we can load them if needed.
	deps := agent.Dependencies{
		AgentRegistry: make(map[string]ai.Agent),
	}

	// If the embedder is an agent, we need to load it.
	if cfg.Embedder.Type == "agent" {
		targetID := cfg.Embedder.AgentID
		fmt.Printf("Loading embedder agent: %s...\n", targetID)

		var depSvc ai.Agent
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
			// Fix storage path for dependency relative to config file
			if depCfg.StoragePath != "" && !filepath.IsAbs(depCfg.StoragePath) {
				depCfg.StoragePath = filepath.Join(filepath.Dir(*configPath), depCfg.StoragePath)
			}
			depSvc, depErr = agent.NewFromConfig(*depCfg, agent.Dependencies{AgentRegistry: make(map[string]ai.Agent)})
		} else {
			// 2. Fallback to looking for a separate file
			configDir := filepath.Dir(*configPath)
			depConfigPath := filepath.Join(configDir, fmt.Sprintf("%s.json", targetID))
			if _, err := os.Stat(depConfigPath); os.IsNotExist(err) {
				depConfigPath = fmt.Sprintf("ai/data/%s.json", targetID)
			}

			if _, err := os.Stat(depConfigPath); err == nil {
				depCfgFromFile, err := agent.LoadConfigFromFile(depConfigPath)
				if err != nil {
					panic(fmt.Errorf("failed to load dependency agent config: %w", err))
				}
				// Fix storage path
				if depCfgFromFile.StoragePath != "" && !filepath.IsAbs(depCfgFromFile.StoragePath) {
					depCfgFromFile.StoragePath = filepath.Join(filepath.Dir(depConfigPath), depCfgFromFile.StoragePath)
				}
				depSvc, depErr = agent.NewFromConfig(*depCfgFromFile, agent.Dependencies{AgentRegistry: make(map[string]ai.Agent)})
			} else {
				// 3. Check if it's the root config itself (unlikely for embedder but possible)
				if rootCfg.ID == targetID {
					depSvc, depErr = agent.NewFromConfig(*rootCfg, agent.Dependencies{AgentRegistry: make(map[string]ai.Agent)})
				} else {
					panic(fmt.Errorf("dependency agent '%s' not found in inline agents or as a file", targetID))
				}
			}
		}

		if depErr != nil {
			panic(fmt.Errorf("failed to initialize dependency agent: %w", depErr))
		}
		deps.AgentRegistry[targetID] = depSvc
	}

	emb, idx, err := agent.SetupInfrastructure(*cfg, deps)
	if err != nil {
		panic(fmt.Errorf("failed to setup infrastructure: %w", err))
	}

	// 3. Load Data & Process in Batches
	batchSize := 100
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

		vecs, err := emb.EmbedTexts(texts)
		if err != nil {
			return fmt.Errorf("failed to generate embeddings: %w", err)
		}

		items := make([]ai.Item, len(batch))
		for i, item := range batch {
			items[i] = ai.Item{
				ID:     item.ID,
				Vector: vecs[i],
				Meta: map[string]any{
					"text":        item.Text,
					"description": item.Description,
				},
			}
		}

		if err := idx.UpsertBatch(items); err != nil {
			return fmt.Errorf("failed to upsert batch: %w", err)
		}
		totalProcessed += len(batch)
		return nil
	}

	if *dataFile != "" {
		fmt.Printf("Streaming data from %s...\n", *dataFile)
		f, err := os.Open(*dataFile)
		if err != nil {
			panic(fmt.Errorf("failed to open data file: %w", err))
		}
		defer f.Close()

		dec := json.NewDecoder(f)

		// Read opening bracket
		t, err := dec.Token()
		if err != nil {
			panic(fmt.Errorf("failed to read opening token: %w", err))
		}
		if delim, ok := t.(json.Delim); !ok || delim != '[' {
			panic("expected JSON array of items")
		}

		var batch []agent.DataItem
		for dec.More() {
			var item agent.DataItem
			if err := dec.Decode(&item); err != nil {
				panic(fmt.Errorf("failed to decode item: %w", err))
			}
			batch = append(batch, item)

			if len(batch) >= batchSize {
				if err := processBatch(batch); err != nil {
					panic(err)
				}
				batch = batch[:0] // Clear batch
			}
		}

		// Process remaining
		if len(batch) > 0 {
			if err := processBatch(batch); err != nil {
				panic(err)
			}
		}

		// Read closing bracket
		_, err = dec.Token()
		if err != nil {
			panic(fmt.Errorf("failed to read closing token: %w", err))
		}

	} else {
		fmt.Println("Using data from configuration file...")
		if len(cfg.Data) == 0 {
			fmt.Println("No data to ingest.")
			return
		}

		// Process config data in batches too
		for i := 0; i < len(cfg.Data); i += batchSize {
			end := i + batchSize
			if end > len(cfg.Data) {
				end = len(cfg.Data)
			}
			if err := processBatch(cfg.Data[i:end]); err != nil {
				panic(err)
			}
		}
	}

	// 5. Verify
	count, _ := idx.Count()
	fmt.Printf("ETL Complete. Total items in DB: %d\n", count)
}
