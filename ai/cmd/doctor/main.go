package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/sharedcode/sop/ai/internal/adapter/generator"
	"github.com/sharedcode/sop/ai/internal/domain/vertical"
	"github.com/sharedcode/sop/ai/internal/embed"
	"github.com/sharedcode/sop/ai/internal/index"
	"github.com/sharedcode/sop/ai/internal/policy"
)

type MedicalRecord struct {
	Condition string `json:"condition"`
	Symptoms  string `json:"symptoms"`
}

func main() {
	fmt.Println("Initializing AI Doctor...")

	// 1. Initialize Components
	// Check for API Key to decide between Real LLM (Gemini) or Local Heuristic
	apiKey := os.Getenv("GEMINI_API_KEY")
	var genName string
	var genParams map[string]any

	if apiKey != "" {
		fmt.Println("Using Real LLM (Gemini)...")
		genName = "gemini"
		genParams = map[string]any{"api_key": apiKey, "model": "gemini-pro"}
	} else {
		fmt.Println("Using Local Heuristic Expert (No API Key found)...")
		genName = "local-expert"
		genParams = nil
	}

	gen, err := generator.New(genName, genParams)
	if err != nil {
		panic(err)
	}

	emb := embed.NewSimple("medical-embed", 64)
	// Use Simple JSON File for persistent storage instead of SOP B-Tree
	idx := index.NewSimple("doctor_index")

	// 2. Load Medical Data
	data, err := os.ReadFile("ai/data/medical.json")
	if err != nil {
		fmt.Printf("Error loading data: %v. Make sure you run from project root.\n", err)
		return
	}

	var records []MedicalRecord
	if err := json.Unmarshal(data, &records); err != nil {
		panic(err)
	}

	fmt.Printf("Loading %d medical records into Vector Index...\n", len(records))
	for i, rec := range records {
		text := fmt.Sprintf("%s: %s", rec.Condition, rec.Symptoms)
		vec, _ := emb.EmbedTexts([]string{text})
		if len(vec) > 0 {
			if err := idx.Upsert(fmt.Sprintf("med-%d", i), vec[0], map[string]any{"text": text}); err != nil {
				fmt.Printf("Error upserting record %d: %v\n", i, err)
			}
		}
	}

	// 3. Create Vertical
	// Define Safety Components (Global/Standardized)
	safetyPolicy, profanityClassifier := policy.NewProfanityGuardrail(3)

	doctor := vertical.New("doctor-diagnosis", emb, idx, safetyPolicy, gen, profanityClassifier)

	// Set Prompt Template
	if genName == "gemini" {
		// LLM Prompt
		doctor.SetPrompt("rag", `You are an expert medical diagnostician.
Based on the following medical knowledge (Context) and the patient's reported symptoms, provide a differential diagnosis.
Pay close attention to negative symptoms (what the patient does NOT have).

Medical Knowledge:
{{context}}

Patient Symptoms: {{query}}

Diagnosis:`)
	} else {
		// Local Heuristic Prompt (Strict Format)
		doctor.SetPrompt("rag", `Medical Knowledge:
{{context}}

Patient Symptoms: {{query}}`)
	}

	// 4. Interactive Loop
	reader := bufio.NewReader(os.Stdin)
	var symptomsHistory []string

	fmt.Println("\n--- AI Doctor Ready ---")
	fmt.Println("Describe your symptoms. Type 'exit' to quit, or 'reset' (or 'clear') to start over.")

	for {
		fmt.Print("\nPatient: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "exit" {
			break
		}
		if input == "clear" || input == "reset" {
			symptomsHistory = nil
			fmt.Print("\033[H\033[2J") // Clear screen
			fmt.Println("--- AI Doctor Ready ---")
			fmt.Println("Symptoms history cleared.")
			continue
		}
		if input == "" {
			continue
		}

		// Accumulate symptoms for narrowing
		symptomsHistory = append(symptomsHistory, input)
		query := strings.Join(symptomsHistory, " | ")

		// Process
		fmt.Print("Doctor is thinking...\n")
		response, err := doctor.Process(context.Background(), query)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			// Remove the last input if it caused an error (e.g. blocked)
			symptomsHistory = symptomsHistory[:len(symptomsHistory)-1]

			if strings.Contains(err.Error(), "Session terminated") {
				fmt.Println("Exiting due to safety policy violations.")
				break
			}
		} else {
			fmt.Printf("Doctor: %s\n", response)
		}
	}
}
