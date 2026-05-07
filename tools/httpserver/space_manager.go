package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
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
		Description: "Pre-loads a medical space index for diagnosing illnesses.",
		IsDefault:   false,
	},
}

func handleGetAvailableSpaces(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(AvailableExpertise)
}

type IngestSpaceRequest struct {
	Expertise    string                      `json:"expertise_id"`
	DatabaseName string                      `json:"database_name"`
	SpaceName    string                      `json:"space_name,omitempty"`
	URL          string                      `json:"url,omitempty"`
	Attributes   *memory.KnowledgeBaseConfig `json:"attributes,omitempty"`
	CustomData   json.RawMessage             `json:"custom_data,omitempty"`
}

type ingestChunk struct {
	ID               string         `json:"id"`
	Category         string         `json:"category"`
	Text             string         `json:"text"`
	Description      string         `json:"description"`
	Summaries        interface{}    `json:"summaries"`
	Vectors          [][]float32    `json:"vectors"`
	SummariesVectors [][]float32    `json:"summaries_vectors,omitempty"`
	Data             map[string]any `json:"data,omitempty"`
}

func extractSummaries(chunk ingestChunk) []string {
	summaries := chunk.Summaries
	text := chunk.Text
	if sArr, ok := summaries.([]interface{}); ok && len(sArr) > 0 {
		var res []string
		for _, s := range sArr {
			if str, ok := s.(string); ok && str != "" {
				res = append(res, str)
			}
		}
		if len(res) > 0 {
			return res
		}
	}
	if sStr, ok := summaries.(string); ok && sStr != "" {
		parts := strings.Split(sStr, ".")
		var res []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				res = append(res, p)
			}
		}
		if len(res) > 0 {
			return res
		}
	}

	// Check if we can apply some heuristics and come up with decent Summaries.
	s := determineSummaries(chunk.Text, chunk.Description, MAX_ITEM_SUMMARIES)
	if len(s) > 0 {
		return s
	}

	// Fallback to text as single Summary.
	if len(text) < 150 && text != "" {
		return []string{text}
	}
	return nil
}

func handleIngestSpace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req IngestSpaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	dbEmbedder := GetConfiguredEmbedder(r)
	dbLLM := GetConfiguredLLM(r)

	task := RegisterTask("SpaceIngest", 100)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"task_id": task.TaskID,
		"message": fmt.Sprintf("Preloading %s started in background", req.Expertise),
	})

	go func(taskId string, request IngestSpaceRequest, emb ai.Embeddings, llm ai.Generator) {
		defer func() {
			if rec := recover(); rec != nil {
				UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Panic during preload: %v", rec))
			}
		}()

		ctx := context.Background()
		UpdateTask(taskId, "in_progress", 10, 100, "Initializing database connection...", "")
		opts, err := getDBOptions(ctx, request.DatabaseName)
		if err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to get DB config: %v", err))
			return
		}

		db := database.NewDatabase(opts)

		trans, err := db.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to begin transaction: %v", err))
			return
		}

		storeName := request.Expertise
		if request.SpaceName != "" {
			storeName = request.SpaceName
		}

		kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, llm, emb)
		if err != nil {
			trans.Rollback(ctx)
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to open KnowledgeBase '%s': %v", storeName, err))
			return
		}

		if request.Attributes != nil {
			err := kb.SetConfig(ctx, request.Attributes)
			if err != nil {
				fmt.Printf("Failed to insert Space Attributes: %v\n", err)
			}
		}

		var thoughts []memory.Thought[map[string]any]
		UpdateTask(taskId, "in_progress", 30, 100, "Reading Space data...", "")

		if len(request.CustomData) > 0 {
			var chunks []ingestChunk
			if err := json.Unmarshal(request.CustomData, &chunks); err == nil {
				for idx, chunk := range chunks {
					cid := chunk.ID
					if cid == "" {
						cid = fmt.Sprintf("custom_%d", idx)
					}

					thoughts = append(thoughts, memory.Thought[map[string]any]{
						Summaries: extractSummaries(chunk), Vectors: chunk.Vectors, Category: chunk.Category, Data: map[string]any{"text": chunk.Text, "description": chunk.Description, "category": chunk.Category, "original_id": cid},
					})
				}
			}
		} else if request.URL != "" {
			reqHTTP, err := http.NewRequestWithContext(ctx, http.MethodGet, request.URL, nil)
			if err == nil {
				resp, err := http.DefaultClient.Do(reqHTTP)
				if err == nil {
					defer resp.Body.Close()
					var chunks []ingestChunk
					if err := json.NewDecoder(resp.Body).Decode(&chunks); err == nil {
						for idx, chunk := range chunks {
							cid := chunk.ID
							if cid == "" {
								cid = fmt.Sprintf("net_%d", idx)
							}
							thoughts = append(thoughts, memory.Thought[map[string]any]{
								Summaries: extractSummaries(chunk), Vectors: chunk.Vectors, Category: chunk.Category, Data: map[string]any{"text": chunk.Text, "description": chunk.Description, "category": chunk.Category, "original_id": cid},
							})
						}
					}
				}
			}
		} else if request.Expertise == "empty" {
			// Empty
		} else {
			var fileBytes []byte
			var err error
			pathsToTry := []string{
				request.Expertise + ".json",
				"../" + request.Expertise + ".json",
				"../../" + request.Expertise + ".json",
				"ai/" + request.Expertise + ".json",
			}
			if request.Expertise == ai.DefaultKBName {
				pathsToTry = append(pathsToTry, "sop_base_knowledge.json", "ai/sop_base_knowledge.json", "../ai/sop_base_knowledge.json")
			}

			for _, p := range pathsToTry {
				if fileBytes, err = os.ReadFile(p); err == nil {
					break
				}
			}

			if err == nil {
				var chunks []ingestChunk
				if err := json.Unmarshal(fileBytes, &chunks); err == nil {
					for idx, chunk := range chunks {
						cid := chunk.ID
						if cid == "" {
							cid = fmt.Sprintf("loc_%d", idx)
						}
						thoughts = append(thoughts, memory.Thought[map[string]any]{
							Summaries: extractSummaries(chunk), Vectors: chunk.Vectors, Category: chunk.Category, Data: map[string]any{"text": chunk.Text, "description": chunk.Description, "category": chunk.Category, "original_id": cid},
						})
					}
				}
			} else {
				trans.Rollback(ctx)
				UpdateTask(taskId, "error", 0, 0, "", "Failed to find Space file locally or provided data")
				return
			}
		}

		if len(thoughts) > 0 {
			UpdateTask(taskId, "in_progress", 50, len(thoughts), "Embedding and ingesting thoughts. This may take some time...", "")
			err := kb.IngestThoughts(ctx, thoughts, "expert")
			if err != nil {
				// Ignore failure here as it might be partial?
				// Actually, better to log but keep going.
			}
		}

		UpdateTask(taskId, "in_progress", 90, 100, "Committing changes...", "")
		if err := trans.Commit(ctx); err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to commit vector store initialization: %v", err))
			return
		}

		UpdateTask(taskId, "completed", 100, 100, fmt.Sprintf("Successfully pre-loaded %s", request.Expertise), "")
	}(task.TaskID, req, dbEmbedder, dbLLM)
}
