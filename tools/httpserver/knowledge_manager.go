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
	Expertise         string          `json:"expertise_id"`
	DatabaseName      string          `json:"database_name"`
	KnowledgeBaseName string          `json:"knowledge_base_name,omitempty"`
	URL               string          `json:"url,omitempty"`
	CustomData        json.RawMessage `json:"custom_data,omitempty"`
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

	// We append _kb to create the specific vector store namespace
	storeName := req.Expertise + "_kb"
	if req.KnowledgeBaseName != "" {
		storeName = req.KnowledgeBaseName
	}
	vsConfig := vector.Config{
		UsageMode: ai.BuildOnceQueryMany,
	}
	vs, err := db.OpenVectorStore(ctx, storeName, trans, vsConfig)
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
	var items []ai.Item[map[string]any]

	if len(req.CustomData) > 0 {
		var chunks []struct {
			ID          string `json:"id"`
			Category    string `json:"category"`
			Text        string `json:"text"`
			Description string `json:"description"`
		}
		if err := json.Unmarshal(req.CustomData, &chunks); err == nil {
			for idx, chunk := range chunks {
				cid := chunk.ID
				if cid == "" {
					cid = fmt.Sprintf("custom_%d", idx)
				}
				textIndexStr := chunk.Text + " " + chunk.Description
				if textIdx != nil {
					textIdx.Add(ctx, cid, textIndexStr)
				}
				items = append(items, ai.Item[map[string]any]{
					ID: cid, Payload: map[string]any{"text": chunk.Text, "description": chunk.Description, "category": chunk.Category, "original_id": cid},
				})
			}
		}
	} else if req.URL != "" {
		reqHTTP, err := http.NewRequestWithContext(ctx, http.MethodGet, req.URL, nil)
		if err == nil {
			resp, err := http.DefaultClient.Do(reqHTTP)
			if err == nil {
				defer resp.Body.Close()
				var chunks []struct {
					ID          string `json:"id"`
					Category    string `json:"category"`
					Text        string `json:"text"`
					Description string `json:"description"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&chunks); err == nil {
					for idx, chunk := range chunks {
						cid := chunk.ID
						if cid == "" {
							cid = fmt.Sprintf("net_%d", idx)
						}
						textIndexStr := chunk.Text + " " + chunk.Description
						if textIdx != nil {
							textIdx.Add(ctx, cid, textIndexStr)
						}
						items = append(items, ai.Item[map[string]any]{
							ID: cid, Payload: map[string]any{"text": chunk.Text, "description": chunk.Description, "category": chunk.Category, "original_id": cid},
						})
					}
				}
			}
		}
	} else if req.Expertise == "empty" {
		// Just create an empty store and return
		// (Initialize the vector store and search index is done above, no data to add.)
	} else {
		var fileBytes []byte
		var err error
		pathsToTry := []string{
			req.Expertise + ".json",
			"../" + req.Expertise + ".json",
			"../../" + req.Expertise + ".json",
			"ai/" + req.Expertise + ".json",
		}
		if req.Expertise == "sop_framework" {
			pathsToTry = append(pathsToTry, "sop_base_knowledge.json", "ai/sop_base_knowledge.json", "../ai/sop_base_knowledge.json")
		}

		for _, p := range pathsToTry {
			if fileBytes, err = os.ReadFile(p); err == nil {
				break
			}
		}

		if err == nil {
			var chunks []struct {
				ID          string `json:"id"`
				Category    string `json:"category"`
				Text        string `json:"text"`
				Description string `json:"description"`
			}
			if err := json.Unmarshal(fileBytes, &chunks); err == nil {
				for idx, chunk := range chunks {
					cid := chunk.ID
					if cid == "" {
						cid = fmt.Sprintf("loc_%d", idx)
					}
					textIndexStr := chunk.Text + " " + chunk.Description
					if textIdx != nil {
						textIdx.Add(ctx, cid, textIndexStr)
					}
					items = append(items, ai.Item[map[string]any]{
						ID: cid, Payload: map[string]any{"text": chunk.Text, "description": chunk.Description, "category": chunk.Category, "original_id": cid},
					})
				}
			}
		} else {
			trans.Rollback(ctx)
			http.Error(w, "Failed to find Knowledge Base file locally or provided data", http.StatusInternalServerError)
			return
		}
	}

	if len(items) > 0 {
		vs.UpsertBatch(ctx, items)
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
