package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
)

func main() {
	configPath := flag.String("config", "", "Path to the agent configuration JSON file")
	flag.Parse()

	if *configPath == "" {
		fmt.Println("Usage: go run ai/cmd/agent/main.go -config <path_to_config.json>")
		os.Exit(1)
	}

	// 1. Load Configuration
	cfg, err := agent.LoadConfigFromFile(*configPath)
	if err != nil {
		panic(fmt.Errorf("failed to load config: %w", err))
	}

	// Ensure absolute path for storage
	if cfg.StoragePath != "" {
		if !filepath.IsAbs(cfg.StoragePath) {
			// Resolve relative to config file directory
			configDir := filepath.Dir(*configPath)
			cfg.StoragePath = filepath.Join(configDir, cfg.StoragePath)
		}
		if absPath, err := filepath.Abs(cfg.StoragePath); err == nil {
			cfg.StoragePath = absPath
		}
	}

	fmt.Printf("Initializing AI Agent: %s (%s)...\n", cfg.Name, cfg.ID)

	// 2. Initialize Agent Service
	registry := make(map[string]ai.Agent)

	// Check if this agent needs an embedder agent
	if cfg.Embedder.Type == "agent" && cfg.Embedder.AgentID != "" {
		agentID := cfg.Embedder.AgentID
		fmt.Printf("Loading dependency agent: %s...\n", agentID)

		// Try to find the dependency config relative to the current config
		configDir := filepath.Dir(*configPath)
		depConfigPath := filepath.Join(configDir, fmt.Sprintf("%s.json", agentID))

		// Fallback to default location if not found
		if _, err := os.Stat(depConfigPath); os.IsNotExist(err) {
			depConfigPath = fmt.Sprintf("ai/data/%s.json", agentID)
		}

		depCfg, err := agent.LoadConfigFromFile(depConfigPath)
		if err != nil {
			panic(fmt.Errorf("failed to load dependency agent %s: %w", agentID, err))
		}

		// Ensure absolute path for storage in dependency
		if depCfg.StoragePath != "" {
			if !filepath.IsAbs(depCfg.StoragePath) {
				depConfigDir := filepath.Dir(depConfigPath)
				depCfg.StoragePath = filepath.Join(depConfigDir, depCfg.StoragePath)
			}
			if absPath, err := filepath.Abs(depCfg.StoragePath); err == nil {
				depCfg.StoragePath = absPath
			}
		}

		// Initialize the dependency agent (assuming it doesn't have further dependencies for now)
		depSvc, err := agent.NewFromConfig(*depCfg, agent.Dependencies{
			AgentRegistry: make(map[string]ai.Agent),
		})
		if err != nil {
			panic(fmt.Errorf("failed to initialize dependency agent %s: %w", agentID, err))
		}
		registry[agentID] = depSvc
	}

	deps := agent.Dependencies{
		AgentRegistry: registry,
	}
	svc, err := agent.NewFromConfig(*cfg, deps)
	if err != nil {
		panic(fmt.Errorf("failed to initialize agent: %w", err))
	}

	// 3. Interactive Loop
	fmt.Printf("\nAI Doctor:\n")
	fmt.Println(cfg.Description)
	fmt.Println("Type 'exit' to quit.")

	if err := svc.RunLoop(context.Background(), os.Stdin, os.Stdout); err != nil {
		fmt.Printf("Error during session: %v\n", err)
	}
}
