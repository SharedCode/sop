package etl

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/sharedcode/sop/ai/agent"
)

// PrepareData downloads a CSV dataset and converts it to the agent DataItem JSON format.
func PrepareData(url, out string, limit int) error {
	fmt.Printf("Downloading dataset from %s...\n", url)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download dataset: %w", err)
	}
	defer resp.Body.Close()

	reader := csv.NewReader(resp.Body)

	// Read header
	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	var items []agent.DataItem
	count := 0

	for {
		if limit > 0 && count >= limit {
			break
		}
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV record: %w", err)
		}

		// First column is Disease
		disease := strings.TrimSpace(record[0])
		if disease == "" {
			continue
		}

		// Remaining columns are symptoms
		var symptoms []string
		for i := 1; i < len(record); i++ {
			s := strings.TrimSpace(record[i])
			if s != "" {
				s = strings.ReplaceAll(s, "_", " ")
				s = strings.ReplaceAll(s, "cold hands", "freezing hands")
				symptoms = append(symptoms, s)
			}
		}

		// Create item
		baseID := strings.ToLower(strings.ReplaceAll(disease, " ", "_"))
		id := fmt.Sprintf("%s_%d", baseID, count)
		desc := fmt.Sprintf("Symptoms: %s", strings.Join(symptoms, ", "))

		items = append(items, agent.DataItem{
			ID:          id,
			Text:        disease,
			Description: desc,
		})
		count++
	}

	fmt.Printf("Processed %d records.\n", count)

	// Write to JSON
	f, err := os.Create(out)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(items); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	fmt.Printf("Saved to %s\n", out)
	return nil
}
