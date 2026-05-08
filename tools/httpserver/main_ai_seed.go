package main

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"os"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	aidb "github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
)

func seedSOPKnowledge(ctx context.Context, db *aidb.Database) {
	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to begin transaction for seeding knowledge base: %v", err))
		return
	}

	storeName := ai.DefaultKBName

	// Open KnowledgeBase and TextIndex
	embedder := GetConfiguredEmbedder(nil)
	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, nil, embedder)
	if err != nil {
		trans.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to open KnowledgeBase '%s': %v", storeName, err))
		return
	}

	// Check if already populated to avoid duplicate seeding
	if count, countErr := kb.Store.Count(ctx); countErr == nil && count > 0 {
		trans.Rollback(ctx)
		log.Debug(fmt.Sprintf("KnowledgeBase '%s' is already populated with %d items. Skipping seed.", storeName, count))
		return
	}

	pathsToTry := []string{
		"sop_base_knowledge.json",
		"ai/sop_base_knowledge.json",
		"../ai/sop_base_knowledge.json",
		"ai/cmd/knowledge_compiler/sop_base_knowledge.json",
		"../ai/cmd/knowledge_compiler/sop_base_knowledge.json",
		"../../ai/cmd/knowledge_compiler/sop_base_knowledge.json",
	}

	var fileBytes []byte
	for _, p := range pathsToTry {
		if fileBytes, err = os.ReadFile(p); err == nil {
			break
		}
	}

	if err != nil || len(fileBytes) == 0 {
		trans.Rollback(ctx)
		log.Debug(fmt.Sprintf("Knowledge Base file sop_base_knowledge.json not found locally. Skipping preload. Paths tried: %v", pathsToTry))
		return
	}

	type chunkType struct {
		ID          string `json:"id"`
		Category    string `json:"category"`
		Text        string `json:"text"`
		Description string `json:"description"`
	}

	var seedData struct {
		Config *memory.KnowledgeBaseConfig `json:"config"`
		Items  []chunkType                 `json:"items"`
	}

	if err := json.Unmarshal(fileBytes, &seedData); err != nil {
		// Fallback to old flat array structure for backwards compatibility temporarily
		var chunks []chunkType
		if err2 := json.Unmarshal(fileBytes, &chunks); err2 != nil {
			trans.Rollback(ctx)
			log.Error(fmt.Sprintf("Failed to unmarshal knowledge base JSON: %v", err))
			return
		}
		seedData.Items = chunks
	}

	if seedData.Config != nil {
		kb.SetConfig(ctx, seedData.Config)
	}

	var thoughts []memory.Thought[map[string]any]
	for idx, chunk := range seedData.Items {
		cid := chunk.ID
		if cid == "" {
			cid = fmt.Sprintf("loc_%d", idx)
		}

		thoughts = append(thoughts, memory.Thought[map[string]any]{
			Summaries: []string{chunk.Text}, Category: chunk.Category, Data: map[string]any{"description": chunk.Description, "original_id": cid},
		})
	}

	// Log the creation of the SystemDB into SOP as a fact
	thoughts = append(thoughts, memory.Thought[map[string]any]{
		Summaries: []string{"System database and initial settings provisioned successfully."},
		Category:  "System_Initialization",
		Data:      map[string]any{"event": "system_initialization", "text": "Initial provisioning of the system and user configurations."},
	})

	if len(thoughts) > 0 {
		// Batch ingestion to prevent deadlocks on large datasets
		const batchSize = 200
		for i := 0; i < len(thoughts); i += batchSize {
			end := i + batchSize
			if end > len(thoughts) {
				end = len(thoughts)
			}
			if err := kb.IngestThoughts(ctx, thoughts[i:end], "expert"); err != nil {
				log.Error(fmt.Sprintf("Failed during batched ingestion: %v", err))
				return
			}
		}
	}

	if err := trans.Commit(ctx); err != nil {
		log.Error(fmt.Sprintf("Failed to commit vector store initialization: %v", err))
		return
	}

	log.Debug(fmt.Sprintf("Successfully injected SOP Knowledge Base with %d chunks into SystemDB '%s'", len(thoughts), storeName))
}
