package etl

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/sharedcode/sop/ai/agent"
)

// PrepareDoctorDataset downloads the dataset from the given URL and returns a fully populated agent.Config.
// It parses the CSV data, extracts disease and symptom information, and formats it into agent.DataItem structures.
// It also configures default policies and synonyms for the "doctor" agent.
func PrepareDoctorDataset(datasetURL string) (agent.Config, error) {
	fmt.Printf("Downloading doctor dataset from %s...\n", datasetURL)
	resp, err := http.Get(datasetURL)
	if err != nil {
		return agent.Config{}, fmt.Errorf("failed to download dataset: %w", err)
	}
	defer resp.Body.Close()

	reader := csv.NewReader(resp.Body)

	// Read header (Disease, Symptom_1, Symptom_2, ...)
	_, err = reader.Read()
	if err != nil {
		return agent.Config{}, fmt.Errorf("failed to read CSV header: %w", err)
	}

	var dataItems []agent.DataItem
	fmt.Println("Processing records...")
	recordCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return agent.Config{}, fmt.Errorf("failed to read CSV record: %w", err)
		}

		disease := strings.TrimSpace(record[0])
		if disease == "" {
			continue
		}

		// Collect symptoms
		var symptoms []string
		for i := 1; i < len(record); i++ {
			s := strings.TrimSpace(record[i])
			if s != "" {
				// Clean up symptom text (e.g. "skin_rash" -> "skin rash")
				s = strings.ReplaceAll(s, "_", " ")
				symptoms = append(symptoms, s)
			}
		}

		description := fmt.Sprintf("Case of %s. Symptoms include %s.", disease, strings.Join(symptoms, ", "))

		// Generate unique ID for each case
		recordCount++
		baseID := strings.ToLower(strings.ReplaceAll(disease, " ", "_"))
		id := fmt.Sprintf("%s_%d", baseID, recordCount)

		dataItems = append(dataItems, agent.DataItem{
			ID:          id,
			Text:        disease,
			Description: description,
		})
	}

	fmt.Printf("Extracted %d medical cases.\n", len(dataItems))

	// Create the full config
	config := agent.Config{
		ID:           "doctor",
		Name:         "Dr. AI (Full)",
		Description:  "I am a medical assistant with knowledge of 40+ diseases. Tell me your symptoms.",
		SystemPrompt: "You are a helpful medical assistant. Analyze the user's symptoms and suggest possible conditions based on the knowledge base.",
		StoragePath:  "doctor", // Default storage path for this vertical
		Policies: []agent.PolicyConfig{
			{Type: "profanity", MaxStrikes: 3},
		},
		Synonyms: map[string]string{
			"hurt":      "pain",
			"ache":      "pain",
			"tummy":     "abdomen",
			"belly":     "abdomen",
			"throbbing": "pulsating",
			"hot":       "fever",
			"puke":      "vomiting",
			"throw up":  "vomiting",
		},
		Embedder: agent.EmbedderConfig{
			Type:    "agent",
			AgentID: "nurse_local",
		},
		Data: dataItems,
	}

	return config, nil
}
