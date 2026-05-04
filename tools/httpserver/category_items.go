package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sharedcode/sop"

	aidb "github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/database"
)

func handleListSpaceCategories(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	parentIDStr := r.URL.Query().Get("parent_id")

	if storeName == "" {
		http.Error(w, "Space name is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(ctx, dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	db := aidb.NewDatabase(dbOpts)
	dbEmbedder := GetConfiguredEmbedder(r)
		dbLLM := GetConfiguredLLM(r)

		memoryDb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder)
	if err != nil {
		json.NewEncoder(w).Encode(make([]map[string]any, 0))
		return
	}

	catsTree, err := memoryDb.Store.Categories(ctx)
	if err != nil {
		json.NewEncoder(w).Encode(make([]map[string]any, 0))
		return
	}

	var categories []map[string]any
	ok, _ := catsTree.First(ctx)
	for ok {
		cat, _ := catsTree.GetCurrentValue(ctx)
		if cat != nil {
			isMatch := false
			if parentIDStr == "" {
				isMatch = true
			} else {
				for _, p := range cat.ParentIDs {
					if p.ParentID.String() == parentIDStr {
						isMatch = true
						break
					}
				}
			}

			if isMatch {
				categories = append(categories, map[string]any{
					"id":          cat.ID.String(),
					"name":        cat.Name,
					"description": cat.Description,
					"item_count":  cat.ItemCount,
					"children":    cat.ChildrenIDs,
					"parents":     cat.ParentIDs,
				})
			}
		}
		ok, _ = catsTree.Next(ctx)
	}

	if categories == nil {
		categories = make([]map[string]any, 0)
	}
	json.NewEncoder(w).Encode(categories)
}

func handleListSpaceItems(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	categoryFilter := r.URL.Query().Get("category_id")
	if storeName == "" {
		http.Error(w, "Space name is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(ctx, dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	type DomainItem struct {
		ID       string    `json:"id"`
		Category string    `json:"category"`
		Text     string    `json:"text"`
		Desc     string    `json:"description"`
		Vector   []float32 `json:"vector,omitempty"`
	}
	var items []DomainItem

	// 1. Try NEW Dynamic Store (from today)
	db := aidb.NewDatabase(dbOpts)
	dbEmbedder := GetConfiguredEmbedder(r)
		dbLLM := GetConfiguredLLM(r)

		memoryDb, errDynamic := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder)
	if errDynamic == nil && memoryDb != nil {
		itemsTree, errTree := memoryDb.Store.Items(ctx)
		if errTree == nil && itemsTree != nil {
			ok, _ := itemsTree.First(ctx)
			for ok {
				val, _ := itemsTree.GetCurrentValue(ctx)
				// Filter (if user passed category_id)
				if categoryFilter == "" || val.CategoryID.String() == categoryFilter {
					t := DomainItem{
						ID:       val.ID.String(),
						Category: val.CategoryID.String(),
					}
					if val.Data != nil {
						payload := val.Data
						if text, found := payload["text"]; found {
							t.Text = fmt.Sprint(text)
						}
						if desc, found := payload["description"]; found {
							t.Desc = fmt.Sprint(desc)
						}
						if cat, found := payload["category"]; found {
							t.Category = fmt.Sprint(cat)
						}
					}
					// Fetch Vector Data
					if len(val.Positions) > 0 {
						vecTree, errVec := memoryDb.Store.Vectors(ctx)
						if errVec == nil && vecTree != nil {
							found, _ := vecTree.Find(ctx, val.Positions[0], false)
							if found {
								vVal, _ := vecTree.GetCurrentValue(ctx)
								t.Vector = vVal.Data
							}
						}
					}
					items = append(items, t)
				}

				ok, _ = itemsTree.Next(ctx)
			}
			if items == nil {
				items = make([]DomainItem, 0)
			}
			json.NewEncoder(w).Encode(items)
			return
		}
	}

	http.Error(w, "Failed to open KnowledgeBase or items tree not found", http.StatusInternalServerError)
}
