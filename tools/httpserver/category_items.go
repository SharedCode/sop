package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/memory"

	aidb "github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/database"
)

func handleListSpaceCategories(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	parentIDStr := r.URL.Query().Get("parent_id")
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	offset := 0
	if offsetStr != "" {
		fmt.Sscanf(offsetStr, "%d", &offset)
	}
	limit := 50
	if limitStr != "" {
		fmt.Sscanf(limitStr, "%d", &limit)
	}

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

	memoryDb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder, false)
	if err != nil {
		json.NewEncoder(w).Encode(make([]map[string]any, 0))
		return
	}

	parentPath := ""
	if parentIDStr == "root" {
		parentPath = "/"
	} else if parentIDStr != "" {
		parentPath = parentIDStr
	}

	var parentCatID sop.UUID
	parentUUID, errP := sop.ParseUUID(parentPath)
	if errP == nil {
		parentCatID = parentUUID
		parentPath = ""
	}

	catList, totalCount, err := memoryDb.ListCategories(ctx, memory.ListCategoriesParam{Limit: limit, Offset: offset, ParentPath: parentPath, ParentID: parentCatID})
	if err != nil {
		json.NewEncoder(w).Encode(make([]map[string]any, 0))
		return
	}

	categories := make([]map[string]any, len(catList))
	for i, cat := range catList {
		children := cat.ChildrenIDs
		if len(children) == 0 {
			children = make([]sop.UUID, 0)
		}
		categories[i] = map[string]any{
			"id":            cat.ID.String(),
			"name":          cat.Name,
			"path":          cat.Path,
			"description":   cat.Description,
			"item_count":    cat.ItemCount,
			"children":      children,
			"parents":       cat.ParentIDs,
			"center_vector": cat.CenterVector,
		}
	}

	if categories == nil {
		categories = make([]map[string]any, 0)
	}

	rbacMap := sop.ResolveRBACMap(ctx, "space", sop.EntitlementContext{AssetID: storeName, Database: dbName, IsSystemDB: IsSystemDB(dbName)}, nil)

	response := map[string]any{
		"data":   categories,
		"total":  totalCount,
		"offset": offset,
		"limit":  limit,
		// When a Space is about to be displayed, it calls handleListSpaceCategories. Thus we can send the RBAC for the Space here.
		"rbac": rbacMap,
	}
	json.NewEncoder(w).Encode(response)
}

func handleListSpaceItems(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	categoryFilter := r.URL.Query().Get("category_id")
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	offset := 0
	if offsetStr != "" {
		fmt.Sscanf(offsetStr, "%d", &offset)
	}
	limit := 50
	if limitStr != "" {
		fmt.Sscanf(limitStr, "%d", &limit)
	}

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

	var items []SpaceItemView

	// 1. Try NEW Dynamic Store (from today)
	db := aidb.NewDatabase(dbOpts)
	dbEmbedder := GetConfiguredEmbedder(r)
	dbLLM := GetConfiguredLLM(r)

	memoryDb, errDynamic := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder, false)

	totalItemsCount := 0

	if errDynamic == nil && memoryDb != nil {
		var catIDFilter sop.UUID
		if categoryFilter != "" {
			catIDFilter, _ = sop.ParseUUID(categoryFilter)
		}

		rawItems, count, _ := memoryDb.ListItems(ctx, memory.ListItemsParam{CategoryID: catIDFilter, Limit: limit, Offset: offset})
		totalItemsCount = count

		for _, val := range rawItems {
			if val.ID.IsNil() {
				continue
			}
			t := SpaceItemView{
				ID:        val.ID.String(),
				Category:  val.CategoryID.String(),
				Summaries: val.Summaries,
				DocID:     val.DocID,
			}
			if val.Data != nil {
				payload := val.Data
				if docID, found := payload["doc_id"]; found && t.DocID == "" {
					t.DocID = fmt.Sprint(docID)
				}
				if text, found := payload["text"]; found {
					t.Text = fmt.Sprint(text)
				}
				if desc, found := payload["description"]; found {
					t.Description = fmt.Sprint(desc)
				}
				if cat, found := payload["category"]; found {
					t.Category = fmt.Sprint(cat)
				}
			}
			if len(val.Positions) > 0 {
				vecTree, errVec := memoryDb.Store.Vectors(ctx)
				if errVec == nil && vecTree != nil {
					found, _ := vecTree.Find(ctx, val.Positions[0], false)
					if found {
						vVal, _ := vecTree.GetCurrentValue(ctx)
						if len(vVal.Data) > 0 {
							t.Vector = []float32{vVal.Data[0]}
							t.VectorSize = len(vVal.Data)
						}
					}
				}
			}
			items = append(items, t)
		}

		if items == nil {
			items = make([]SpaceItemView, 0)
		}

		response := map[string]any{
			"data":   items,
			"total":  totalItemsCount,
			"offset": offset,
			"limit":  limit,
		}
		json.NewEncoder(w).Encode(response)
		return
	}
	http.Error(w, "Failed to open KnowledgeBase or items tree not found", http.StatusInternalServerError)
}
