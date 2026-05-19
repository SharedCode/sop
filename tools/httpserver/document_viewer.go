package main

import (
	"net/http"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
)

func handleViewer(w http.ResponseWriter, r *http.Request) {
	docID := r.URL.Query().Get("docID")
	if docID == "" {
		http.Error(w, "docID is required", http.StatusBadRequest)
		return
	}
	dbName := r.URL.Query().Get("db")
	spaceName := r.URL.Query().Get("space")

	if dbName == "" || spaceName == "" {
		http.Error(w, "db and space parameters are required", http.StatusBadRequest)
		return
	}

	dbOpt, err := getDBOptions(r.Context(), dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dbInstance := database.NewDatabase(dbOpt)
	tx, err := dbInstance.BeginTransaction(r.Context(), sop.ForReading)
	if err != nil {
		http.Error(w, "failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	kb, err := dbInstance.OpenKnowledgeBase(r.Context(), spaceName, tx, nil, nil, true, false)
	if err != nil {
		http.Error(w, "failed to open knowledge base: "+err.Error(), http.StatusInternalServerError)
		return
	}

	uid, err := sop.ParseUUID(docID)
	if err != nil {
		http.Error(w, "invalid docID format", http.StatusBadRequest)
		return
	}

	docs, err := kb.Store.Documents(r.Context())
	if err != nil {
		http.Error(w, "failed to access document store: "+err.Error(), http.StatusInternalServerError)
		return
	}

	found, err := docs.Find(r.Context(), uid, false)
	if err != nil {
		http.Error(w, "document not found", http.StatusNotFound)
		return
	}
	if !found {
		http.Error(w, "document not found", http.StatusNotFound)
		return
	}

	documentVal, err := docs.GetCurrentValue(r.Context())
	if err != nil {
		http.Error(w, "failed to retrieve document content", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(documentVal.Content))
}
