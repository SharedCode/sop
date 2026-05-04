package main

import (
	"fmt"
	"net/http"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
)

func handleExportKnowledgeBase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dbName := r.URL.Query().Get("database")
	storeName := r.URL.Query().Get("name")
	if dbName == "" || storeName == "" {
		http.Error(w, "database and name parameters are required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	opts, err := getDBOptions(ctx, dbName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get DB config: %v", err), http.StatusBadRequest)
		return
	}

	// Because OpenKnowledgeBase needs an existing Database struct
	db := database.NewDatabase(opts)
	trans, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to begin transaction: %v", err), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	// Since we are strictly just exporting the existing categories/items out of B-tree,
	// we do not need an active LLM/Embedder connected.
	dbEmbedder := GetConfiguredEmbedder(r)
		dbLLM := GetConfiguredLLM(r)

		memoryDb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open memory store: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s_export.json\"", storeName))

	if err := memoryDb.ExportJSON(ctx, w); err != nil {
		fmt.Printf("Export error: %v\n", err)
	}
}

func handleImportKnowledgeBase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dbName := r.URL.Query().Get("database")
	storeName := r.URL.Query().Get("name")
	if dbName == "" || storeName == "" {
		http.Error(w, "database and name parameters are required", http.StatusBadRequest)
		return
	}

	// 50MB limit max for memory footprint on small scale DBs.
	if err := r.ParseMultipartForm(50 << 20); err != nil {
		http.Error(w, "Failed to parse form map", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "File is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	ctx := r.Context()
	opts, err := getDBOptions(ctx, dbName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get DB config: %v", err), http.StatusBadRequest)
		return
	}

	db := database.NewDatabase(opts)
	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to begin transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// We pass nil for LLM and Embedder because ImportJSON bypasses generation
	// if the incoming JSON already contains fully-formed categories/vectors.
	dbEmbedder := GetConfiguredEmbedder(r)
		dbLLM := GetConfiguredLLM(r)

		memoryDb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder)
	if err != nil {
		trans.Rollback(ctx)
		http.Error(w, fmt.Sprintf("Failed to open memory store: %v", err), http.StatusInternalServerError)
		return
	}

	// We arbitrarily pass "imported_persona" as persona
	if err := memoryDb.ImportJSON(ctx, file, "imported_persona"); err != nil {
		trans.Rollback(ctx)
		http.Error(w, fmt.Sprintf("Failed to import JSON: %v", err), http.StatusInternalServerError)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, fmt.Sprintf("Failed to commit imported data: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"success": true}`))
}
