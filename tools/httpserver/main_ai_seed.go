package main

import (
	"context"
	"fmt"
	log "log/slog"
	"os"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	aidb "github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
)

func seedSOPKnowledge(ctx context.Context, db *aidb.Database) error {
	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to begin transaction for seeding knowledge base: %v", err))
		return err
	}

	storeName := ai.DefaultKBName

	// Open KnowledgeBase and TextIndex
	embedder := GetConfiguredEmbedder(nil)
	kb, err := db.OpenKnowledgeBase(ctx, storeName, trans, nil, embedder)
	if err != nil {
		trans.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to open KnowledgeBase '%s': %v", storeName, err))
		return err
	}

	// Check if already populated to avoid duplicate seeding
	if count, countErr := kb.Store.Count(ctx); countErr == nil && count > 0 {
		trans.Rollback(ctx)
		log.Debug(fmt.Sprintf("KnowledgeBase '%s' is already populated with %d items. Skipping seed.", storeName, count))
		return nil
	}

	pathsToTry := []string{
		"sop_base_knowledge.json",
		"ai/sop_base_knowledge.json",
		"../ai/sop_base_knowledge.json",
		"ai/cmd/knowledge_compiler/sop_base_knowledge.json",
		"../ai/cmd/knowledge_compiler/sop_base_knowledge.json",
		"../../ai/cmd/knowledge_compiler/sop_base_knowledge.json",
	}

	var file *os.File
	for _, p := range pathsToTry {
		if file, err = os.Open(p); err == nil {
			break
		}
	}

	if err != nil || file == nil {
		trans.Rollback(ctx)
		log.Debug(fmt.Sprintf("Knowledge Base file sop_base_knowledge.json not found locally. Skipping preload. Paths tried: %v", pathsToTry))
		return nil
	}
	defer file.Close()

	if err := kb.ImportJSON(ctx, file, "expert"); err != nil {
		trans.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to ingest knowledge base via ImportJSON: %v", err))
		return err
	}

	// Log the creation of the SystemDB into SOP as a fact
	sysThought := memory.Thought[map[string]any]{
		Summaries: []string{"System database and initial settings provisioned successfully."},
		Category:  "System_Initialization",
		Data:      map[string]any{"event": "system_initialization", "text": "Initial provisioning of the system and user configurations."},
	}

	if err := kb.IngestThoughts(ctx, []memory.Thought[map[string]any]{sysThought}, "expert"); err != nil {
		trans.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to log system init thought: %v", err))
		return err
	}

	if err := trans.Commit(ctx); err != nil {
		log.Error(fmt.Sprintf("Failed to commit vector store initialization: %v", err))
		return err
	}

	log.Debug(fmt.Sprintf("Successfully injected SOP Knowledge Base into SystemDB '%s'", storeName))
	return nil
}
