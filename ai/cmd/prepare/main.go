package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/sharedcode/sop/ai/agent"
)

func main() {
	url := flag.String("url", "", "URL of the CSV dataset")
	out := flag.String("out", "", "Output JSON file path")
	flag.Parse()

	if *url == "" || *out == "" {
		fmt.Println("Usage: go run ai/cmd/prepare/main.go -url <csv_url> -out <output.json>")
		os.Exit(1)
	}

	fmt.Printf("Downloading dataset from %s...\n", *url)
	resp, err := http.Get(*url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	reader := csv.NewReader(resp.Body)

	// Read header
	_, err = reader.Read()
	if err != nil {
		panic(err)
	}

	var items []agent.DataItem
	count := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
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
	f, err := os.Create(*out)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(items); err != nil {
		panic(err)
	}

	fmt.Printf("Saved to %s\n", *out)
}
