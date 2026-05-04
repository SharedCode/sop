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

// chunkSentences limits to a max number of sentences.
// We use a constant to ensure high semantic density without bloat.
const MAX_ITEM_SUMMARIES = 5

func handleUpdateSpaceItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID         string         `json:"id"`
		CategoryID string         `json:"category_id"`
		Summaries  []string       `json:"summaries,omitempty"`
		Positions  [][]float32    `json:"positions,omitempty"`
		Data       map[string]any `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	newText, hasChunk := req.Data["chunk"].(string)
	if !hasChunk {
		newText, hasChunk = req.Data["Text"].(string)
	}

	if req.ID == "" {
		http.Error(w, "Missing ID", http.StatusBadRequest)
		return
	}
	if !hasChunk && len(req.Summaries) == 0 {
		http.Error(w, "Missing fields: must provide Data 'chunk' or 'Text', or 'summaries'", http.StatusBadRequest)
		return
	}

	itemID, err := sop.ParseUUID(req.ID)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	if storeName == "" {
		http.Error(w, "Knowledge Base name is required", http.StatusBadRequest)
		return
	}

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

	itemsTree, err := kb.Store.Items(ctx)
	if err != nil {
		http.Error(w, "Failed to access items", http.StatusInternalServerError)
		return
	}

	// Fetch existing
	found, err := itemsTree.Find(ctx, itemID, false)
	if err != nil || !found {
		http.Error(w, "Item not found", http.StatusNotFound)
		return
	}
	existingItem, _ := itemsTree.GetCurrentValue(ctx)

	// Merge data (twist for UI integration)
	mergedData := existingItem.Data
	if mergedData == nil {
		mergedData = make(map[string]any)
	}

	// Apply all incoming data props to our existing data to retain other fields
	for k, v := range req.Data {
		mergedData[k] = v
	}

	// Fetch category to get SummaryMaxCount and Name
	maxSummaries := MAX_ITEM_SUMMARIES
	var categoryName string
	categoriesTree, err := kb.Store.Categories(ctx)
	if err == nil {
		catFound, _ := categoriesTree.Find(ctx, existingItem.CategoryID, false)
		if catFound {
			if cat, _ := categoriesTree.GetCurrentValue(ctx); cat != nil {
				categoryName = cat.Name
				if cat.SummaryMaxCount > 0 {
					maxSummaries = cat.SummaryMaxCount
				}
			}
		}
	}

	if categoryName == "" {
		http.Error(w, "Category not found for existing item", http.StatusInternalServerError)
		return
	}

	mergedDataBytes, _ := json.Marshal(mergedData)
	mergedDataStr := string(mergedDataBytes)

	textToSummarize := ""
	if hasChunk {
		textToSummarize = newText
	}

	summarizer := GetSummarizer(config.ProductionMode, GetConfiguredLLMClient(r)) // In future, inject active LLM here
	summaries := DetermineSummaries(ctx, summarizer, req.Summaries, textToSummarize, mergedDataStr, maxSummaries)

	// Create multiple vectors with Differential Sync/Merge
	var vecs [][]float32

	if len(req.Positions) > 0 && len(req.Positions) == len(summaries) {
		vecs = req.Positions
	} else {
		vectorsTree, err := kb.Store.Vectors(ctx)
		if err != nil {
			http.Error(w, "Failed to access vectors tree", http.StatusInternalServerError)
			return
		}

		// 1. O(N) Match existing summaries to their previous vector math
		existingSummariesMap := make(map[string][]float32)
		for i, summary := range existingItem.Summaries {
			if i < len(existingItem.Positions) {
				vKey := existingItem.Positions[i]
				found, err := vectorsTree.Find(ctx, vKey, false)
				if err == nil && found {
					vecObj, _ := vectorsTree.GetCurrentValue(ctx)
					existingSummariesMap[summary] = vecObj.Data
				}
			}
		}

		var textsToEmbed []string
		var textIndices []int
		vecs = make([][]float32, len(summaries))

		// 2. Identify unchanged vectors to recycle vs changes to embed
		for i, summary := range summaries {
			if existingVec, ok := existingSummariesMap[summary]; ok {
				vecs[i] = existingVec
			} else {
				textsToEmbed = append(textsToEmbed, summary)
				textIndices = append(textIndices, i)
			}
		}

		// 3. Batch embed only the actual deltas
		if len(textsToEmbed) > 0 {
			var newVectors [][]float32
			if config.ProductionMode {
				embedder := embed.NewSimple("simple_hash", 384, nil)
				v, err := embedder.EmbedTexts(ctx, textsToEmbed)
				if err != nil || len(v) != len(textsToEmbed) {
					http.Error(w, "Embedding failed: "+err.Error(), http.StatusInternalServerError)
					return
				}
				newVectors = v
			} else {
				for range textsToEmbed {
					newVectors = append(newVectors, []float32{0.1, 0.2, 0.3})
				}
			}

			// 4. Sew delta vectors back into their positional slots
			for idx, newVec := range newVectors {
				targetIndex := textIndices[idx]
				vecs[targetIndex] = newVec
			}
		}
	}

	newItem := memory.Item[map[string]any]{
		ID:         itemID,
		CategoryID: existingItem.CategoryID,
		Summaries:  summaries,
		Data:       mergedData,
	}

	err = kb.Store.UpsertByCategory(ctx, categoryName, newItem, vecs)
	if err != nil {
		http.Error(w, "Upsert failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Commit failed", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}
