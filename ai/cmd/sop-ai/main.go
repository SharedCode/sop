package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
	"github.com/sharedcode/sop/ai/etl"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "prepare":
		runPrepare(os.Args[2:])
	case "ingest":
		runIngest(os.Args[2:])
	case "query":
		runQuery(os.Args[2:])
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: sop-ai <command> [flags]")
	fmt.Println("\nCommands:")
	fmt.Println("  prepare    Download and format datasets (e.g. doctor)")
	fmt.Println("  ingest     Ingest data from a config file into the vector database")
	fmt.Println("  query      Query the agent")
}

// --- Prepare Command (formerly importer) ---

func runPrepare(args []string) {
	fs := flag.NewFlagSet("prepare", flag.ExitOnError)
	agentType := fs.String("type", "doctor", "Type of agent dataset to prepare (doctor)")
	output := fs.String("out", "ai/data/doctor_fallback.json", "Output JSON config file path")
	url := fs.String("url", "https://raw.githubusercontent.com/itachi9604/healthcare-chatbot/master/Data/dataset.csv", "Dataset URL")
	fs.Parse(args)

	switch *agentType {
	case "doctor":
		prepareDoctor(*url, *output)
	default:
		fmt.Printf("Unknown agent type: %s. Currently supported: doctor\n", *agentType)
		os.Exit(1)
	}
}

func prepareDoctor(datasetURL, outputFile string) {
	config, err := etl.PrepareDoctorDataset(datasetURL)
	if err != nil {
		panic(fmt.Errorf("failed to prepare doctor dataset: %w", err))
	}

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputFile), 0755); err != nil {
		panic(fmt.Errorf("failed to create output directory: %w", err))
	}

	// Write to file
	file, err := os.Create(outputFile)
	if err != nil {
		panic(fmt.Errorf("failed to create output file: %w", err))
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(config); err != nil {
		panic(fmt.Errorf("failed to encode config: %w", err))
	}

	fmt.Printf("Successfully generated %s\n", outputFile)
}

// --- Ingest Command (formerly ingest) ---

func runIngest(args []string) {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to the agent configuration JSON file")
	clean := fs.Bool("clean", false, "Clean the database before ingestion")
	fs.Parse(args)

	if *configPath == "" {
		fmt.Println("Usage: sop-ai ingest -config <path_to_config.json> [-clean]")
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

	// 2. Clean DB if requested
	if *clean {
		if cfg.StoragePath != "" {
			fmt.Printf("Cleaning database at %s...\n", cfg.StoragePath)
			if err := os.RemoveAll(cfg.StoragePath); err != nil {
				panic(fmt.Errorf("failed to clean database: %w", err))
			}
		} else {
			fmt.Println("Warning: No storage_path in config, skipping clean.")
		}
	}

	fmt.Printf("Initializing Ingestion for: %s (%s)...\n", cfg.Name, cfg.ID)

	// 3. Setup Infrastructure
	deps := agent.Dependencies{
		AgentRegistry: make(map[string]ai.Agent),
	}

	if cfg.Embedder.Type == "agent" && cfg.Embedder.AgentID != "" {
		// Load dependency agent
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

		if depCfg.StoragePath != "" {
			if !filepath.IsAbs(depCfg.StoragePath) {
				// Resolve relative to its config file directory
				depConfigDir := filepath.Dir(depConfigPath)
				depCfg.StoragePath = filepath.Join(depConfigDir, depCfg.StoragePath)
			}
			if absPath, err := filepath.Abs(depCfg.StoragePath); err == nil {
				depCfg.StoragePath = absPath
			}
		}

		depSvc, err := agent.NewFromConfig(*depCfg, agent.Dependencies{
			AgentRegistry: make(map[string]ai.Agent),
		})
		if err != nil {
			panic(fmt.Errorf("failed to initialize dependency agent %s: %w", agentID, err))
		}
		deps.AgentRegistry[agentID] = depSvc
	}

	emb, idx, err := agent.SetupInfrastructure(*cfg, deps)
	if err != nil {
		panic(fmt.Errorf("failed to setup infrastructure: %w", err))
	}

	// 4. Ingest Data
	if err := agent.IngestData(*cfg, idx, emb); err != nil {
		panic(fmt.Errorf("failed to ingest data: %w", err))
	}

	fmt.Println("Ingestion complete.")
}

// --- Query Command ---

func runQuery(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to the agent configuration JSON file")
	query := fs.String("q", "", "Query to ask the agent")
	fs.Parse(args)

	if *configPath == "" {
		fmt.Println("Usage: sop-ai query -config <path_to_config.json> [-q <query_text>]")
		fmt.Println("If -q is omitted, interactive mode is started.")
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

	// 2. Setup Infrastructure
	// We don't need full dependencies for a simple query unless the agent uses them.
	// For the doctor agent, it might use the nurse agent.
	deps := agent.Dependencies{
		AgentRegistry: make(map[string]ai.Agent),
	}

	// Check if we need to load dependencies (like nurse)
	if cfg.Embedder.Type == "agent" && cfg.Embedder.AgentID != "" {
		agentID := cfg.Embedder.AgentID

		// Try to find the dependency config relative to the current config
		configDir := filepath.Dir(*configPath)
		depConfigPath := filepath.Join(configDir, fmt.Sprintf("%s.json", agentID))

		// Fallback to default location if not found
		if _, err := os.Stat(depConfigPath); os.IsNotExist(err) {
			depConfigPath = fmt.Sprintf("ai/data/%s.json", agentID)
		}

		if _, err := os.Stat(depConfigPath); err == nil {
			depCfg, err := agent.LoadConfigFromFile(depConfigPath)
			if err == nil {
				if depCfg.StoragePath != "" {
					if !filepath.IsAbs(depCfg.StoragePath) {
						// Resolve relative to its config file directory
						depConfigDir := filepath.Dir(depConfigPath)
						depCfg.StoragePath = filepath.Join(depConfigDir, depCfg.StoragePath)
					}
					if absPath, err := filepath.Abs(depCfg.StoragePath); err == nil {
						depCfg.StoragePath = absPath
					}
				}
				depSvc, err := agent.NewFromConfig(*depCfg, agent.Dependencies{})
				if err == nil {
					deps.AgentRegistry[agentID] = depSvc
				} else {
					panic(fmt.Errorf("failed to initialize dependency agent %s: %w", agentID, err))
				}
			} else {
				panic(fmt.Errorf("failed to load dependency agent config %s: %w", agentID, err))
			}
		} else {
			panic(fmt.Errorf("embedder agent '%s' not found in registry (checked %s)", agentID, depConfigPath))
		}
	}

	svc, err := agent.NewFromConfig(*cfg, deps)
	if err != nil {
		panic(fmt.Errorf("failed to initialize agent: %w", err))
	}

	// 3. Ask
	if *query != "" {
		fmt.Printf("Querying Agent '%s' with: '%s'\n", cfg.Name, *query)
		answer, err := svc.Ask(context.Background(), *query)
		if err != nil {
			panic(fmt.Errorf("query failed: %w", err))
		}
		fmt.Printf("\nAnswer:\n%s\n", answer)
	} else {
		// Interactive Mode
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("Agent '%s' is ready. Type 'exit' or 'quit' to stop.\n", cfg.Name)
		for {
			fmt.Print("\nYou: ")
			text, _ := reader.ReadString('\n')
			text = strings.TrimSpace(text)
			if text == "exit" || text == "quit" {
				break
			}
			if text == "" {
				continue
			}
			answer, err := svc.Ask(context.Background(), text)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			fmt.Printf("Agent: %s\n", answer)
		}
	}
}
