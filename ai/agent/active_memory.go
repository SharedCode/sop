package agent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
)

// StartActiveMemorySleepCycle launches a background consolidator that sweeps TempVectors
// taking Short-Term episodic memories and migrating them into Semantic Long-Term Categories.
// Call this once on Service initialization if EnableActiveMemory = true.
func (s *Service) StartActiveMemorySleepCycle(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		log.Info("Active Memory: Initiating Sleep Cycle Background Worker", "interval", interval)

		for {
			select {
			case <-ctx.Done():
				log.Info("Active Memory: Sleep Cycle Interrupted (Context Done)")
				return
			case <-ticker.C:
				if !s.EnableActiveMemory {
					continue
				}

				if s.domain == nil {
					continue
				}

				log.Debug("Active Memory: Sleep Cycle Triggered. Consolidating memories...")

				tx, err := s.domain.BeginTransaction(ctx, sop.ForReading)
				if err != nil {
					log.Warn("Active Memory: Sleep Cycle Failed to get transaction", "error", err)
					continue
				}

				vectorStore, err := s.domain.Index(ctx, tx)
				if err != nil {
					tx.Rollback(ctx)
					log.Warn("Active Memory: Sleep Cycle Failed to get vector store", "error", err)
					continue
				}

				// Perform consolidation (Migrate TempVectors -> Vectors & Centroids)
				err = vectorStore.Consolidate(context.Background())

				// After Consolidate runs (which manages its own writing transactions),
				// we close the outer read transaction.
				tx.Rollback(ctx)

				if err != nil {
					log.Warn("Active Memory: Sleep Cycle Consolidation encountered an error", "error", err)
				} else {
					log.Debug("Active Memory: Sleep Cycle completed successfully.")
				}
			}
		}
	}()
}

// logEpisode intercepts script execution outcomes and embeds them into the TempVectors store (Short-Term Memory).
// This runs async via a goroutine to prevent blocking the interactive user loop.
func (s *Service) logEpisode(ctx context.Context, intent string, astPayload any, outcome string, executeErr error) {
	if s.domain == nil || s.domain.Embedder() == nil {
		log.Warn("Active Memory: Domain or Embedder not configured, skipping ingestion.")
		return
	}

	// 1. Serialize Context (The "Thought")
	astBytes, err := json.Marshal(astPayload)
	var astStr string
	if err == nil {
		astStr = string(astBytes)
	} else {
		astStr = fmt.Sprintf("%T", astPayload)
	}

	status := "Success"
	errorDesc := ""
	if executeErr != nil {
		status = "Error"
		errorDesc = executeErr.Error()
	}

	// Combine into a structured representation for embedding and retrieval
	thought := fmt.Sprintf("Intent: %s\nAST: %s\nStatus: %s\n", intent, astStr, status)
	if errorDesc != "" {
		thought += fmt.Sprintf("Error: %s\n", errorDesc)
	}
	if status == "Success" && outcome != "" {
		outLog := outcome
		if len(outLog) > 100 {
			outLog = outLog[:100] + "..."
		}
		thought += fmt.Sprintf("Outcome: %s\n", outLog)
	}

	log.Debug("Active Memory: Embedding thought snippet", "thought", thought)

	// 2. Embed the Thought
	embedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	vecs, err := s.domain.Embedder().EmbedTexts(embedCtx, []string{thought})
	if err != nil || len(vecs) == 0 {
		log.Warn("Active Memory: Failed to embed thought", "error", err)
		return
	}
	vec := vecs[0]

	// 3. Insert into B-Tree (TempVectors via VectorStore.Upsert)
	tx, err := s.domain.BeginTransaction(embedCtx, sop.ForWriting)
	if err != nil {
		log.Warn("Active Memory: Failed to start transaction", "error", err)
		return
	}

	vectorStore, err := s.domain.Index(embedCtx, tx)
	if err != nil {
		tx.Rollback(embedCtx)
		log.Warn("Active Memory: Failed to get vector store", "error", err)
		return
	}

	// Deterministic Deduplication: Hash the semantic thought payload
	// The B-Tree will automatically overwrite the existing item with zero graph-rebalancing cost.
	hash := sha256.Sum256([]byte(thought))
	itemID := fmt.Sprintf("%x", hash)

	item := ai.Item[map[string]any]{
		ID:     itemID,
		Vector: vec,
		Payload: map[string]any{
			"intent":  intent,
			"ast":     astBytes,
			"status":  status,
			"error":   errorDesc,
			"outcome": outcome,
			"ts":      time.Now().UnixMilli(),
			"type":    "episode",
		},
	}

	if err := vectorStore.Upsert(embedCtx, item); err != nil {
		tx.Rollback(embedCtx)
		log.Warn("Active Memory: Failed to log episode", "error", err)
		return
	}

	if err := tx.Commit(embedCtx); err != nil {
		log.Warn("Active Memory: Transaction commit failed", "error", err)
		return
	}

	log.Info("Active Memory: Episode logged successfully", "temp_id", itemID)
}
