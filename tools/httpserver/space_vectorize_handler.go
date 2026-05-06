package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

type VectorizeRequest struct {
	Database   string   `json:"database"`
	SpaceName  string   `json:"space"`
	CategoryID string   `json:"categoryId"`
	ItemIDs    []string `json:"itemIds,omitempty"` // nil means all items
}

func handleVectorizeSpace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var reqData VectorizeRequest
	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	dbOpt, err := getDBOptions(context.Background(), reqData.Database)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if reqData.CategoryID == "" && len(reqData.ItemIDs) == 0 {
		http.Error(w, "categoryId is required when itemIds are empty", http.StatusBadRequest)
		return
	}

	var catUUID sop.UUID
	if reqData.CategoryID != "" {
		catUUID, err = sop.ParseUUID(reqData.CategoryID)
		if err != nil {
			http.Error(w, "invalid categoryId format", http.StatusBadRequest)
			return
		}
	}
	var itemUUIDs []sop.UUID
	if len(reqData.ItemIDs) > 0 {
		for _, idStr := range reqData.ItemIDs {
			id, err := sop.ParseUUID(idStr)
			if err != nil {
				http.Error(w, "invalid itemId format", http.StatusBadRequest)
				return
			}
			itemUUIDs = append(itemUUIDs, id)
		}
	}

	dbEmbedder := GetConfiguredEmbedder(r)
	dbLLM := GetConfiguredLLM(r)

	task := RegisterTask("VectorizeSpace", 100)
	UpdateTask(task.TaskID, "in_progress", 0, 100, "Starting vectorization...", "")

	go func(taskId string, request VectorizeRequest, catId sop.UUID, itemIds []sop.UUID, opts sop.DatabaseOptions, emb ai.Embeddings, llm ai.Generator) {
		defer func() {
			if r := recover(); r != nil {
				UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Panic during vectorization: %v", r))
			}
		}()

		ctx := context.Background()
		db := database.NewDatabase(opts)

		trans, err := db.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to begin transaction: %v", err))
			return
		}
		defer trans.Rollback(ctx)

		kb, err := db.OpenKnowledgeBase(ctx, request.SpaceName, trans, llm, emb)
		if err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to open KnowledgeBase '%s': %v", request.SpaceName, err))
			return
		}

		UpdateTask(taskId, "in_progress", 10, 100, "Calculating Embeddings...", "")

		if catId == sop.NilUUID && len(itemIds) == 0 {
			err = kb.Vectorize(ctx)
			if err != nil {
				UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Vectorize failed: %v", err))
				return
			}
		} else {
			err = kb.VectorizeItems(ctx, catId, itemIds)
			if err != nil {
				UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("VectorizeItems failed: %v", err))
				return
			}
		}

		UpdateTask(taskId, "in_progress", 90, 100, "Committing changes...", "")
		if err := trans.Commit(ctx); err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to commit vectorization: %v", err))
			return
		}

		UpdateTask(taskId, "completed", 100, 100, fmt.Sprintf("Successfully vectorized category/items in %s", request.SpaceName), "")
	}(task.TaskID, reqData, catUUID, itemUUIDs, dbOpt, dbEmbedder, dbLLM)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"task_id": task.TaskID})
}
