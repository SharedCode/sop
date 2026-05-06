package main

import (
	"context"
	"net/http"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
)

func handleExportSpace(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	kb, err := db.OpenKnowledgeBase(ctx, spaceName, trans, nil, nil)
	if err != nil {
		http.Error(w, "Failed to load Knowledge Base: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+spaceName+"_export.json\"")
	if err := kb.ExportJSON(ctx, w); err != nil {
		// Cannot change HTTP status code after headers are written, just log
		return
	}
}

func handleImportSpace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dbName := r.FormValue("database")
	spaceName := r.FormValue("name")

	if dbName == "" || spaceName == "" {
		http.Error(w, "database and name are required", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Failed to read uploaded file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	dbOpt, err := getDBOptions(context.Background(), dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	db := database.NewDatabase(dbOpt)

	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	kb, err := db.OpenKnowledgeBase(ctx, spaceName, trans, nil, nil)
	if err != nil {
		http.Error(w, "Failed to load Knowledge Base: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := kb.ImportJSON(ctx, file, ""); err != nil {
		http.Error(w, "Failed to import JSON: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Failed to commit transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}
