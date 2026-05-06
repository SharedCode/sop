package main

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
)

func handleSaveSpaceConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	authCtx := sop.GetAuthFromContext(r.Context())

	dbName := r.URL.Query().Get("database")
	spaceName := r.URL.Query().Get("name")

	if dbName == "" || spaceName == "" {
		http.Error(w, "database and name are required", http.StatusBadRequest)
		return
	}

	// Simple authorization and quota check stub
	// Real implementation would look up `system_roles` cache for authCtx.Roles
	// and verify capabilities + StorageLimits/ExecutionLimits block.
	if !authCtx.IsSystem && len(authCtx.Roles) > 0 {
		// (Example: Reject if role is Guest trying to configure a Space, or if quotas hit)
		if authCtx.Roles[0] == sop.RoleGuest {
			http.Error(w, "Guest role is not permitted to modify Space configurations", http.StatusForbidden)
			return
		}
	}

	var req memory.KnowledgeBaseConfig
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid config payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	dbOpt, err := getDBOptions(context.Background(), dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	db := database.NewDatabase(dbOpt)
	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
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

	if err := kb.SetConfig(ctx, &req); err != nil {
		http.Error(w, "Failed to save Space config: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Failed to commit transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}
