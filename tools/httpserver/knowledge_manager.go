package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/vector"
	"github.com/sharedcode/sop/search"
)

type ExpertiseMetadata struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	IsDefault   bool   `json:"is_default"`
}

var AvailableExpertise = []ExpertiseMetadata{
	{
		ID:          "medical",
		Name:        "Medical Expert (Nurse/Doctor) KB",
		Description: "Pre-loads a medical knowledge base index for diagnosing illnesses.",
		IsDefault:   false,
	},
}

func handleGetAvailableKnowledgeBases(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(AvailableExpertise)
}

type PreloadKnowledgeRequest struct {
	Expertise    string `json:"expertise_id"`
	DatabaseName string `json:"database_name"`
}

func handlePreloadKnowledge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PreloadKnowledgeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	ctx := context.Background()

	opts, err := getDBOptions(ctx, req.DatabaseName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get DB config: %v", err), http.StatusBadRequest)
		return
	}

	// Assuming a simplified setup, we initialize the db
	db := database.NewDatabase(opts)

	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to begin transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// We append _knowledge_base to create the specific vector store namespace
	storeName := req.Expertise + "_knowledge_base"
	vs, err := db.OpenVectorStore(ctx, storeName, trans, vector.Config{UsageMode: ai.BuildOnceQueryMany})
	if err != nil {
		trans.Rollback(ctx)
		http.Error(w, fmt.Sprintf("Failed to open vector store '%s': %v", storeName, err), http.StatusInternalServerError)
		return
	}

	textIdx, err := db.OpenSearch(ctx, storeName, trans)
	if err != nil {
		trans.Rollback(ctx)
		http.Error(w, fmt.Sprintf("Failed to get TextIndex: %v", err), http.StatusInternalServerError)
		return
	}

	// Actually preload data
	if req.Expertise == "sop_framework" {
		// Prefer the bundled copy included by build_release.sh first,
		// falling back to relative repo paths if running in development mode.
		var fileBytes []byte
		var err error
		pathsToTry := []string{
			"sop_base_knowledge.json",          // Root of release bundle
			"ai/sop_base_knowledge.json",       // Dev: running repository root
			"../ai/sop_base_knowledge.json",    // Dev: running from tools/httpserver/
			"../../ai/sop_base_knowledge.json", // Dev: running deeper
		}

		for _, p := range pathsToTry {
			if fileBytes, err = os.ReadFile(p); err == nil {
				break
			}
		}

		if err != nil {
			trans.Rollback(ctx)
			http.Error(w, "Failed to find SOP Knowledge Base file locally", http.StatusInternalServerError)
			return
		}

		var chunks []struct {
			ID          string `json:"id"`
			Category    string `json:"category"`
			Text        string `json:"text"`
			Description string `json:"description"`
		}

		if err := json.Unmarshal(fileBytes, &chunks); err == nil {
			var items []ai.Item[map[string]any]
			for _, chunk := range chunks {
				textIndexStr := chunk.Text + " " + chunk.Description
				if textIdx != nil {
					textIdx.Add(ctx, chunk.ID, textIndexStr)
				}
				items = append(items, ai.Item[map[string]any]{
					ID:     chunk.ID,
					Vector: nil, // Text search only via textIdx, or reliant on LLM dynamic embedding if needed
					Payload: map[string]any{
						"text":        chunk.Text,
						"description": chunk.Description,
						"category":    chunk.Category,
						"original_id": chunk.ID,
					},
				})
			}
			if len(items) > 0 {
				vs.UpsertBatch(ctx, items)
			}
		}
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, fmt.Sprintf("Failed to commit vector store initialization: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Successfully pre-loaded %s", req.Expertise),
	})
}
