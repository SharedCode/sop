package main

import (
	"github.com/sharedcode/sop/ai/memory"

	"context"
	"encoding/json"
	"net/http"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
)

func handleGetSpaceConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dbName := r.URL.Query().Get("database")
	spaceName := r.URL.Query().Get("name")

	if dbName == "" || spaceName == "" {
		http.Error(w, "database and name are required", http.StatusBadRequest)
		return
	}

	dbOpt, err := getDBOptions(context.Background(), dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	db := database.NewDatabase(dbOpt)
	trans, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		http.Error(w, "Failed to connect to database: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	kb, err := db.OpenKnowledgeBase(ctx, spaceName, trans, nil, nil)
	if err != nil {
		http.Error(w, "Failed to open Space: "+err.Error(), http.StatusInternalServerError)
		return
	}

	cfg, err := kb.GetConfig(ctx)

	// Apply Authorization/RBAC visibility layer
	if err != nil {
		http.Error(w, "Failed to get config: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := struct {
		memory.KnowledgeBaseConfig `json:",inline"`
		IsReadOnly                 bool `json:"is_read_only"`
	}{
		IsReadOnly: !sop.CanPerformAction(ctx, spaceName, sop.ResourceAccess{}, sop.ActionWrite),
	}

	if cfg != nil {
		response.KnowledgeBaseConfig = *cfg
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode config: "+err.Error(), http.StatusInternalServerError)
	}
}
