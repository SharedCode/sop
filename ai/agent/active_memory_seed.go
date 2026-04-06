package agent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"time"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
)

type KnowledgeChunk struct {
	Category string `json:"category"`
	Title    string `json:"title"`
	Content  string `json:"content"`
}

func (s *Service) SeedSemanticBaseKnowledge(jsonFilePath string) {
	if s.domain == nil || !s.EnableActiveMemory {
		log.Warn("SeedSemanticBaseKnowledge aborted: Domain not initialized or Active Memory disabled")
		return
	}

	go func() {
		embedCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		log.Info("SeedSemanticBaseKnowledge: Starting async RAG documentation ingestion", "file", jsonFilePath)

		data, err := os.ReadFile(jsonFilePath)
		if err != nil {
			log.Warn("SeedSemanticBaseKnowledge failed to read file", "error", err)
			return
		}

		var chunks []KnowledgeChunk
		if err := json.Unmarshal(data, &chunks); err != nil {
			log.Warn("SeedSemanticBaseKnowledge failed to unmarshal json", "error", err)
			return
		}

		tx, err := s.domain.BeginTransaction(embedCtx, sop.ForWriting)
		if err != nil {
			log.Warn("SeedSemanticBaseKnowledge: Failed to start transaction", "error", err)
			return
		}

		vectorStore, err := s.domain.Index(embedCtx, tx)
		if err != nil {
			tx.Rollback(embedCtx)
			log.Warn("SeedSemanticBaseKnowledge: Failed to get TempVectors", "error", err)
			return
		}

		log.Info("SeedSemanticBaseKnowledge: Ingesting chunks...", "count", len(chunks))

		var batchItems []*ai.Item[map[string]any]
		var batchTexts []string
		for i, chunk := range chunks {
			chunkID := fmt.Sprintf("core_knowledge_%d_%x", i, sha256.Sum256([]byte(chunk.Title+chunk.Content)))

			_, err := vectorStore.Get(embedCtx, chunkID)
			if err == nil {
				continue // already exists
			}

			batchText := fmt.Sprintf("Base Knowledge -> %s -> %s\n%s", chunk.Category, chunk.Title, chunk.Content)
			batchTexts = append(batchTexts, batchText)

			item := &ai.Item[map[string]any]{
				ID: chunkID,
				Payload: map[string]any{
					"category": chunk.Category,
					"title":    chunk.Title,
					"ts":       time.Now().UnixMilli(),
					"type":     "semantic_base",
					"content":  chunk.Content,
				},
			}
			batchItems = append(batchItems, item)
		}

		if len(batchItems) > 0 {
			if s.domain.Embedder() != nil {
				vectors, err := s.domain.Embedder().EmbedTexts(embedCtx, batchTexts)
				if err != nil {
					log.Warn("SeedSemanticBaseKnowledge: Failed to embed texts", "error", err)
				} else {
					for i, item := range batchItems {
						if i < len(vectors) {
							item.Vector = vectors[i]
						}
					}
				}
			}

			for _, item := range batchItems {
				if err := vectorStore.Upsert(embedCtx, *item); err != nil {
					log.Warn("SeedSemanticBaseKnowledge: Failed to upsert batch item", "error", err)
				}
			}
		}

		if err := tx.Commit(embedCtx); err != nil {
			log.Warn("SeedSemanticBaseKnowledge: Transaction commit failed", "error", err)
			return
		}

		log.Info("SeedSemanticBaseKnowledge: Documentation RAG ingestion complete!", "chunks", len(chunks))
	}()
}
