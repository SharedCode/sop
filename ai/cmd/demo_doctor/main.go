package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
)

func main() {
	ctx := context.Background()
	cwd, _ := os.Getwd()
	fmt.Printf("Working directory: %s\n", cwd)

	// 1. Load Nurse Config
	nursePath := filepath.Join(cwd, "ai/data/nurse_local.json")
	nurseCfg, err := agent.LoadConfigFromFile(nursePath)
	if err != nil {
		panic(fmt.Errorf("failed to load nurse config: %w", err))
	}

	// 2. Create Nurse Agent
	// Nurse has no dependencies
	nurseSvc, err := agent.NewFromConfig(*nurseCfg, agent.Dependencies{})
	if err != nil {
		panic(fmt.Errorf("failed to create nurse agent: %w", err))
	}
	fmt.Println("Nurse agent created.")

	// Test Nurse
	fmt.Println("Testing Nurse...")
	nurseAns, err := nurseSvc.Ask(ctx, "itching")
	if err != nil {
		fmt.Printf("Nurse failed: %v\n", err)
	} else {
		fmt.Printf("Nurse Answer: %s\n", nurseAns)
	}

	// 3. Load Doctor Config
	doctorPath := filepath.Join(cwd, "ai/data/doctor_fallback.json")
	doctorCfg, err := agent.LoadConfigFromFile(doctorPath)
	if err != nil {
		panic(fmt.Errorf("failed to load doctor config: %w", err))
	}

	// 4. Create Doctor Agent
	// Doctor depends on Nurse
	deps := agent.Dependencies{
		AgentRegistry: map[string]ai.Agent{
			"nurse_local": nurseSvc,
		},
	}

	doctorSvc, err := agent.NewFromConfig(*doctorCfg, deps)
	if err != nil {
		panic(fmt.Errorf("failed to create doctor agent: %w", err))
	}
	fmt.Println("Doctor agent created.")

	// 5. Test Query
	// "itching" and "skin rash" are in the doctor's data (e.g. Fungal infection, Chicken pox)
	// The Nurse agent (translator) should help map terms if they were different,
	// but here we are testing the wiring.
	query := "I have a skin rash and itching"
	fmt.Printf("\nQuerying Doctor with: '%s'\n", query)

	// Use Ask (RAG)
	answer, err := doctorSvc.Ask(ctx, query)
	if err != nil {
		panic(fmt.Errorf("doctor query failed: %w", err))
	}

	fmt.Printf("\nDoctor Answer:\n%s\n", answer)
}
