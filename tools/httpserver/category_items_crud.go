package main

import (
	"encoding/json"
	"net/http"

	"github.com/sharedcode/sop"
	aidb "github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/embed"
	"github.com/sharedcode/sop/ai/memory"
	"github.com/sharedcode/sop/database"
)

type addCategoryRequest struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description"`
	ParentID    string `json:"parent_id,omitempty"`
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

	var req addCategoryRequest
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

	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder)
	if err != nil {
		http.Error(w, "Failed to open knowledge base: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var vec []float32
	if config.ProductionMode {
		embedder := embed.NewSimple("simple_hash", 384, nil)
		v, err := embedder.EmbedTexts(ctx, []string{req.Name})
		if err != nil || len(v) == 0 {
			http.Error(w, "Failed to embed category", http.StatusInternalServerError)
			return
		}
		vec = v[0]
	} else {
		vec = []float32{0.1, 0.2, 0.3}
	}

	newID := sop.NewUUID()
	cat := &memory.Category{
		ID:              newID,
		Name:            req.Name,
		Description:     req.Description,
		CenterVector:    vec,
		SummaryMaxCount: MAX_ITEM_SUMMARIES,
	}

	if req.ParentID != "" {
		parsedParent, err := sop.ParseUUID(req.ParentID)
		if err == nil {
			cat.ParentIDs = []memory.CategoryParent{
				{
					ParentID: parsedParent,
					UseCase:  "Manual Subcategory",
				},
			}
		}
	}

	id, err := kb.Store.AddCategory(ctx, cat)
	if err != nil {
		http.Error(w, "Failed to add category: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Failed to commit transaction", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(addCategoryResponse{
		Success: true,
		ID:      id.String(),
		Message: "Category added",
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

	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder)
	if err != nil {
		http.Error(w, "Failed to open knowledge base", http.StatusInternalServerError)
		return
	}

	categoriesTree, err := kb.Store.Categories(ctx)
	if err != nil {
		http.Error(w, "Failed to get categories tree", http.StatusInternalServerError)
		return
	}

	found, err := categoriesTree.Remove(ctx, parsedID)
	if err != nil || !found {
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

type addItemRequest struct {
	ID         string         `json:"id,omitempty"`
	CategoryID string         `json:"category_id"`
	Summaries  []string       `json:"summaries,omitempty"`
	Positions  [][]float32    `json:"positions,omitempty"`
	Data       map[string]any `json:"data"`
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

	var req addItemRequest
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

	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder)
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

	summarizer := GetSummarizer(config.ProductionMode, GetConfiguredLLMClient(r))
	summaries := DetermineSummaries(ctx, summarizer, req.Summaries, chunkStr, dataStr, maxSummaries)

	if len(req.Positions) > 0 {
		vecs = req.Positions
	} else if config.ProductionMode {
		embedder := embed.NewSimple("simple_hash", 384, nil)
		v, err := embedder.EmbedTexts(ctx, summaries)
		if err != nil || len(v) == 0 {
			http.Error(w, "Failed to embed item chunk", http.StatusInternalServerError)
			return
		}
		vecs = v
	} else {
		vecs = make([][]float32, len(summaries))
		for i := range summaries {
			vecs[i] = []float32{0.4, 0.5, 0.6}
		}
	}

	newItem := memory.Item[map[string]any]{
		ID:         sop.NewUUID(),
		CategoryID: parsedCatID,
		Summaries:  summaries,
		Data:       req.Data,
	}

	err = kb.Store.UpsertByCategory(ctx, cat.Name, newItem, vecs)
	if err != nil {
		http.Error(w, "Failed to insert item: "+err.Error(), http.StatusInternalServerError)
		return
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
	ID string `json:"id"`
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

	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder)
	if err != nil {
		http.Error(w, "Failed to open knowledge base", http.StatusInternalServerError)
		return
	}

	err = kb.Store.Delete(ctx, parsedID)
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

type addItemsBatchRequest struct {
	Items []addItemRequest `json:"items"`
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

	var req addItemsBatchRequest
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

	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, dbLLM, dbEmbedder)
	if err != nil {
		http.Error(w, "Failed to open knowledge base", http.StatusInternalServerError)
		return
	}

	categoriesTree, _ := kb.Store.Categories(ctx)

	type itemState struct {
		OriginalReq  addItemRequest
		ParsedCatID  sop.UUID
		CatName      string
		Item         memory.Item[map[string]any]
		ChunkStr     string
		MaxSummaries int
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

		summaries := DetermineSummaries(ctx, summarizer, itm.Summaries, chunkStr, dataStr, maxSummaries)

		state := itemState{
			OriginalReq:  itm,
			ParsedCatID:  parsedCatID,
			CatName:      cat.Name,
			ChunkStr:     chunkStr,
			MaxSummaries: maxSummaries,
			Item: memory.Item[map[string]any]{
				ID:         sop.NewUUID(),
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
		v, err := batchEmbedder.EmbedTexts(ctx, flatSummariesToEmbed)
		if err != nil || len(v) != len(flatSummariesToEmbed) {
			http.Error(w, "Failed to embed item chunks in batch", http.StatusInternalServerError)
			return
		}
		allEmbeddings = v
	}

	var generatedIDs []string
	for i, s := range states {
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

		err = kb.Store.UpsertByCategory(ctx, s.CatName, s.Item, vecs)
		if err != nil {
			http.Error(w, "Failed to insert batch item: "+err.Error(), http.StatusInternalServerError)
			return
		}
		generatedIDs = append(generatedIDs, s.Item.ID.String())
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
