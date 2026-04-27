package main

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"os"

	"github.com/sharedcode/sop"
	
	"github.com/sharedcode/sop/ai/memory"
	aidb "github.com/sharedcode/sop/ai/database"
)

func seedSOPKnowledge(ctx context.Context, db *aidb.Database) {
	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to begin transaction for seeding knowledge base: %v", err))
		return
	}

	storeName := "SOP_KB"

	// Open KnowledgeBase and TextIndex
	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, nil, nil)
	if err != nil {
		trans.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to open KnowledgeBase '%s': %v", storeName, err))
		return
	}

	textIdx, err := db.OpenSearch(ctx, storeName, trans)
	if err != nil {
		trans.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to open text index for '%s': %v", storeName, err))
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

	var chunks []struct {
		ID          string `json:"id"`
		Category    string `json:"category"`
		Text        string `json:"text"`
		Description string `json:"description"`
	}

	if err := json.Unmarshal(fileBytes, &chunks); err != nil {
		trans.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to unmarshal knowledge base JSON: %v", err))
		return
	}

	var thoughts []memory.Thought[map[string]any]
	for idx, chunk := range chunks {
		cid := chunk.ID
		if cid == "" {
			cid = fmt.Sprintf("loc_%d", idx)
		}
		textIndexStr := chunk.Text + " " + chunk.Description
		if textIdx != nil {
			textIdx.Add(ctx, cid, textIndexStr)
		}
		thoughts = append(thoughts, memory.Thought[map[string]any]{
			Text: chunk.Text, Category: chunk.Category, Data: map[string]any{"text": chunk.Text, "description": chunk.Description, "category": chunk.Category, "original_id": cid},
		})
	}

	if len(thoughts) > 0 {
		kb.IngestThoughts(ctx, thoughts, "expert")
	}

	if err := trans.Commit(ctx); err != nil {
		log.Error(fmt.Sprintf("Failed to commit vector store initialization: %v", err))
		return
	}

	log.Debug(fmt.Sprintf("Successfully injected SOP Knowledge Base with %d chunks into SystemDB '%s'", len(thoughts), storeName))
}
