package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sharedcode/sop"
	aidb "github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/embed"
	"github.com/sharedcode/sop/ai/memory"
	"github.com/sharedcode/sop/database"
)

func generateDeterministicID(catID sop.UUID, dataStr string) sop.UUID {
	hashBytes := sha256.Sum256([]byte(catID.String() + ":" + dataStr))
	id, _ := sop.ParseUUID(fmt.Sprintf("%x", hashBytes[:16]))
	return id
}

type addCategoryResponse struct {
	Success bool   `json:"success"`
	ID      string `json:"id,omitempty"`
	Message string `json:"message,omitempty"`
}

func handleAddSpaceCategory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	if storeName == "" {
		http.Error(w, "Knowledge Base name is required", http.StatusBadRequest)
		return
	}

	var req AddSpaceCategoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(ctx, dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	db := aidb.NewDatabase(dbOpts)
	dbEmbedder := GetConfiguredEmbedder(r)
	dbLLM := GetConfiguredLLM(r)

	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder, false)
	if err != nil {
		http.Error(w, "Failed to open knowledge base: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var vec []float32
	if config.ProductionMode {
		embedder := embed.NewSimple("simple_hash", 384, nil)
		v, err := embed.CategoryTexts(ctx, embedder, []string{req.Name})
		if err != nil || len(v) == 0 {
			http.Error(w, "Failed to embed category", http.StatusInternalServerError)
			return
		}
		vec = v[0]
	} else {
		vec = []float32{0.1, 0.2, 0.3}
	}

	var id sop.UUID
	isUpdate := req.ID != ""

	var cat *memory.Category

	if isUpdate {
		categoriesTree, err := kb.Store.Categories(ctx)
		if err != nil {
			http.Error(w, "Failed to load categories store", http.StatusInternalServerError)
			return
		}

		targetID, err := sop.ParseUUID(req.ID)
		if err != nil {
			http.Error(w, "Invalid ID format format", http.StatusBadRequest)
			return
		}

		found, err := categoriesTree.Find(ctx, targetID, false)
		if err != nil || !found {
			http.Error(w, "Category not found", http.StatusNotFound)
			return
		}

		cat, err = categoriesTree.GetCurrentValue(ctx)
		if err != nil {
			http.Error(w, "Failed to load category details", http.StatusInternalServerError)
			return
		}

		cat.Name = req.Name
		cat.Description = req.Description

		// Only generate a new vector if CenterVector already existed
		if len(cat.CenterVector) > 0 {
			cat.CenterVector = vec
		}

		_, err = categoriesTree.UpdateCurrentItem(ctx, targetID, cat)
		if err != nil {
			http.Error(w, "Failed to update category: "+err.Error(), http.StatusInternalServerError)
			return
		}
		id = targetID
	} else {
		newID := sop.NewUUID()

		pathVal := strings.TrimSpace(req.Path)
		if pathVal == "" {
			pathVal = strings.TrimSpace(req.Name)
		}
		var parentIDs []memory.CategoryParent

		if req.ParentID != "" {
			parsedParent, err := sop.ParseUUID(req.ParentID)
			if err == nil {
				parentIDs = []memory.CategoryParent{{ParentID: parsedParent, UseCase: "Manual Subcategory"}}

				categoriesTree, err := kb.Store.Categories(ctx)
				if err == nil {
					found, _ := categoriesTree.Find(ctx, parsedParent, false)
					if found {
						pCat, _ := categoriesTree.GetCurrentValue(ctx)
						if pCat != nil {
							parentPath := strings.TrimSpace(pCat.Path)
							childName := strings.TrimSpace(req.Name)
							if parentPath != "" && childName != "" {
								pathVal = parentPath + "/" + childName
							} else if childName != "" {
								pathVal = childName
							}
						}
					}
				}
			}
		}

		cat = &memory.Category{
			ID:              newID,
			Name:            req.Name,
			Path:            pathVal,
			Description:     req.Description,
			CenterVector:    vec,
			SummaryMaxCount: MAX_ITEM_SUMMARIES,
			ParentIDs:       parentIDs,
		}

		id, err = kb.Store.AddCategory(ctx, cat)
		if err != nil {
			http.Error(w, "Failed to add category: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Failed to commit transaction", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(addCategoryResponse{
		Success: true,
		ID:      id.String(),
		Message: "Category added/updated",
	})
}

type deleteCategoryRequest struct {
	ID string `json:"id"`
}

func handleDeleteSpaceCategory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	if storeName == "" {
		http.Error(w, "Knowledge Base name is required", http.StatusBadRequest)
		return
	}

	var req deleteCategoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	parsedID, err := sop.ParseUUID(req.ID)
	if err != nil {
		http.Error(w, "Invalid ID format", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(ctx, dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	db := aidb.NewDatabase(dbOpts)
	dbEmbedder := GetConfiguredEmbedder(r)
	dbLLM := GetConfiguredLLM(r)

	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder, false)
	if err != nil {
		http.Error(w, "Failed to open knowledge base", http.StatusInternalServerError)
		return
	}

	err = kb.DeleteCategories(ctx, []sop.UUID{parsedID})
	if err != nil {
		http.Error(w, "Category not found or delete fail", http.StatusInternalServerError)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Commit failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(addCategoryResponse{
		Success: true,
		Message: "Category deleted",
	})
}

func handleAddSpaceItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	if storeName == "" {
		http.Error(w, "Knowledge Base name is required", http.StatusBadRequest)
		return
	}

	var req AddSpaceItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	parsedCatID, err := sop.ParseUUID(req.CategoryID)
	if err != nil {
		http.Error(w, "Invalid category_id", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(ctx, dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	db := aidb.NewDatabase(dbOpts)
	dbEmbedder := GetConfiguredEmbedder(r)
	dbLLM := GetConfiguredLLM(r)

	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder, false)
	if err != nil {
		http.Error(w, "Failed to open knowledge base", http.StatusInternalServerError)
		return
	}

	var vecs [][]float32
	chunkStr := ""
	if chunk, ok := req.Data["chunk"].(string); ok {
		chunkStr = chunk
	} else if textVal, ok := req.Data["text"].(string); ok {
		chunkStr = textVal
	} else if textVal, ok := req.Data["Text"].(string); ok {
		chunkStr = textVal
	}

	categoriesTree, _ := kb.Store.Categories(ctx)
	found, cErr := categoriesTree.Find(ctx, parsedCatID, false)
	if cErr != nil || !found {
		http.Error(w, "Category not found", http.StatusNotFound)
		return
	}
	cat, _ := categoriesTree.GetCurrentValue(ctx)

	maxSummaries := cat.SummaryMaxCount
	if maxSummaries <= 0 {
		maxSummaries = MAX_ITEM_SUMMARIES
	}

	dataBytes, _ := json.Marshal(req.Data)
	dataStr := string(dataBytes)

	detID := generateDeterministicID(parsedCatID, dataStr)
	itemsTree, err := kb.Store.Items(ctx)
	if err == nil {
		if found, _ := itemsTree.Find(ctx, memory.ItemKey{CategoryID: parsedCatID, ItemID: detID}, false); found {
			// Item already exists. Skip summarization and vector generation.
			if err := trans.Commit(ctx); err != nil {
				http.Error(w, "Commit failed", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(addCategoryResponse{
				Success: true,
				ID:      detID.String(),
				Message: "Item already exists",
			})
			return
		}
	}

	summarizer := GetSummarizer(config.ProductionMode, GetConfiguredLLMClient(r))
	summaries := DetermineSummaries(ctx, summarizer, req.Summaries, chunkStr, dataStr, maxSummaries)

	if len(req.Positions) > 0 {
		vecs = req.Positions
	}

	newItem := memory.Item[map[string]any]{
		ID:         detID,
		DocID:      req.DocID,
		CategoryID: parsedCatID,
		Summaries:  summaries,
		Data:       req.Data,
	}

	err = kb.Store.UpsertByCategoryPath(ctx, cat.Name, newItem, vecs)
	if err != nil {
		http.Error(w, "Failed to insert item: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if cfg, err := kb.GetConfig(ctx); err == nil {
		if cfg == nil {
			cfg = &memory.KnowledgeBaseConfig{}
		}
		cfg.LastModified = time.Now().Unix()
		kb.SetConfig(ctx, cfg)
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Commit failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(addCategoryResponse{
		Success: true,
		ID:      newItem.ID.String(),
		Message: "Item added safely",
	})
}

type deleteItemRequest struct {
	ID         string `json:"id"`
	CategoryID string `json:"category_id"`
}

func handleDeleteSpaceItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	if storeName == "" {
		http.Error(w, "Knowledge Base name is required", http.StatusBadRequest)
		return
	}

	var req deleteItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	parsedID, err := sop.ParseUUID(req.ID)
	if err != nil {
		http.Error(w, "Invalid ID format", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(ctx, dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	db := aidb.NewDatabase(dbOpts)
	dbEmbedder := GetConfiguredEmbedder(r)
	dbLLM := GetConfiguredLLM(r)

	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder, false)
	if err != nil {
		http.Error(w, "Failed to open knowledge base", http.StatusInternalServerError)
		return
	}

	itemsTree, err := kb.Store.Items(ctx)
	if err != nil {
		http.Error(w, "Failed to access items", http.StatusInternalServerError)
		return
	}

	var foundKey memory.ItemKey
	var found bool

	if req.CategoryID != "" {
		parsedCatID, catErr := sop.ParseUUID(req.CategoryID)
		if catErr == nil {
			foundKey = memory.ItemKey{CategoryID: parsedCatID, ItemID: parsedID}
			if ok, _ := itemsTree.Find(ctx, foundKey, false); ok {
				found = true
			}
		}
	}

	// Fallback to sequential scan if CategoryID is missing or not found
	if !found {
		itemsTree.First(ctx)
		for {
			k := itemsTree.GetCurrentKey()
			if k.Key.ItemID == parsedID {
				foundKey = k.Key
				found = true
				break
			}
			if ok, _ := itemsTree.Next(ctx); !ok {
				break
			}
		}
	}

	if !found {
		http.Error(w, "Item not found", http.StatusNotFound)
		return
	}

	err = kb.DeleteItems(ctx, []memory.ItemKey{{CategoryID: foundKey.CategoryID, ItemID: foundKey.ItemID}})
	if err != nil {
		http.Error(w, "Failed to delete item", http.StatusInternalServerError)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Commit failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(addCategoryResponse{
		Success: true,
		Message: "Item deleted successfully",
	})
}

type batchResponse struct {
	Success bool     `json:"success"`
	Count   int      `json:"count"`
	IDs     []string `json:"ids"`
}

func handleAddSpaceItemsBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	if storeName == "" {
		http.Error(w, "Knowledge Base name is required", http.StatusBadRequest)
		return
	}

	var req AddSpaceItemsBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if len(req.Items) == 0 {
		http.Error(w, "No items provided", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(ctx, dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	db := aidb.NewDatabase(dbOpts)
	dbEmbedder := GetConfiguredEmbedder(r)
	dbLLM := GetConfiguredLLM(r)

	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder, false)
	if err != nil {
		http.Error(w, "Failed to open knowledge base", http.StatusInternalServerError)
		return
	}

	categoriesTree, _ := kb.Store.Categories(ctx)

	type itemState struct {
		OriginalReq  AddSpaceItemRequest
		ParsedCatID  sop.UUID
		CatName      string
		Item         memory.Item[map[string]any]
		ChunkStr     string
		MaxSummaries int
		Skip         bool
	}

	var states []itemState

	summarizer := GetSummarizer(config.ProductionMode, GetConfiguredLLMClient(r))

	for _, itm := range req.Items {
		parsedCatID, err := sop.ParseUUID(itm.CategoryID)
		if err != nil {
			http.Error(w, "Invalid category_id in batch", http.StatusBadRequest)
			return
		}

		found, cErr := categoriesTree.Find(ctx, parsedCatID, false)
		if cErr != nil || !found {
			http.Error(w, "Category not found", http.StatusNotFound)
			return
		}
		cat, _ := categoriesTree.GetCurrentValue(ctx)

		maxSummaries := cat.SummaryMaxCount
		if maxSummaries <= 0 {
			maxSummaries = MAX_ITEM_SUMMARIES
		}

		chunkStr := ""
		if chunk, ok := itm.Data["chunk"].(string); ok {
			chunkStr = chunk
		} else if textVal, ok := itm.Data["Text"].(string); ok {
			chunkStr = textVal
		}

		dataBytes, _ := json.Marshal(itm.Data)
		dataStr := string(dataBytes)

		detID := generateDeterministicID(parsedCatID, dataStr)
		itemsTree, err := kb.Store.Items(ctx)
		if err == nil {
			if found, _ := itemsTree.Find(ctx, memory.ItemKey{CategoryID: parsedCatID, ItemID: detID}, false); found {
				state := itemState{
					OriginalReq:  itm,
					ParsedCatID:  parsedCatID,
					CatName:      cat.Name,
					ChunkStr:     chunkStr,
					MaxSummaries: maxSummaries,
					Skip:         true,
					Item: memory.Item[map[string]any]{
						ID:         detID,
						DocID:      itm.DocID,
						CategoryID: parsedCatID,
						Data:       itm.Data,
					},
				}
				states = append(states, state)
				continue
			}
		}

		summaries := DetermineSummaries(ctx, summarizer, itm.Summaries, chunkStr, dataStr, maxSummaries)

		state := itemState{
			OriginalReq:  itm,
			ParsedCatID:  parsedCatID,
			CatName:      cat.Name,
			ChunkStr:     chunkStr,
			MaxSummaries: maxSummaries,
			Skip:         false,
			Item: memory.Item[map[string]any]{
				ID:         detID,
				DocID:      itm.DocID,
				CategoryID: parsedCatID,
				Summaries:  summaries,
				Data:       itm.Data,
			},
		}
		states = append(states, state)
	}

	var batchEmbedder *embed.Simple
	if config.ProductionMode {
		batchEmbedder = embed.NewSimple("simple_hash", 384, nil)
	}

	flatSummariesToEmbed := make([]string, 0)
	flatPositionsMap := make(map[int]struct{ stateIdx int })
	flatIdx := 0

	for i, s := range states {
		if len(s.OriginalReq.Positions) == 0 && config.ProductionMode {
			for _, summ := range s.Item.Summaries {
				flatSummariesToEmbed = append(flatSummariesToEmbed, summ)
				flatPositionsMap[flatIdx] = struct{ stateIdx int }{stateIdx: i}
				flatIdx++
			}
		}
	}

	var allEmbeddings [][]float32
	if len(flatSummariesToEmbed) > 0 && config.ProductionMode {
		// High-speed batched Embeddings call
		v, err := embed.DocumentTexts(ctx, batchEmbedder, flatSummariesToEmbed)
		if err != nil || len(v) != len(flatSummariesToEmbed) {
			http.Error(w, "Failed to embed item chunks in batch", http.StatusInternalServerError)
			return
		}
		allEmbeddings = v
	}

	var generatedIDs []string
	for i, s := range states {
		if s.Skip {
			generatedIDs = append(generatedIDs, s.Item.ID.String())
			continue
		}

		var vecs [][]float32
		if len(s.OriginalReq.Positions) > 0 {
			vecs = s.OriginalReq.Positions
		} else if config.ProductionMode {
			for j, emb := range allEmbeddings {
				if mapping, ok := flatPositionsMap[j]; ok && mapping.stateIdx == i {
					vecs = append(vecs, emb)
				}
			}
		} else {
			vecs = make([][]float32, len(s.Item.Summaries))
			for k := range s.Item.Summaries {
				vecs[k] = []float32{0.4, 0.5, 0.6}
			}
		}

		err = kb.Store.UpsertByCategoryPath(ctx, s.CatName, s.Item, vecs)
		if err != nil {
			http.Error(w, "Failed to insert batch item: "+err.Error(), http.StatusInternalServerError)
			return
		}
		generatedIDs = append(generatedIDs, s.Item.ID.String())
	}

	if cfg, err := kb.GetConfig(ctx); err == nil {
		if cfg == nil {
			cfg = &memory.KnowledgeBaseConfig{}
		}
		cfg.LastModified = time.Now().Unix()
		kb.SetConfig(ctx, cfg)
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Commit failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(batchResponse{
		Success: true,
		Count:   len(generatedIDs),
		IDs:     generatedIDs,
	})
}
