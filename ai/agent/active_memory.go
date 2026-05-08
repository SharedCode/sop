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
	"github.com/sharedcode/sop/ai/memory"
)

// InitializeShortTermMemory buffers the DDL creation of the active scratchpad B-Tree store.
// This MUST be called sequentially during Service setup before DML (logEpisode) operations begin.
func (s *Service) InitializeShortTermMemory(ctx context.Context) error {
	if s.systemDB == nil {
		return fmt.Errorf("systemDB is not configured")
	}

	storeName := "user_scratchpad"
	exists, err := s.systemDB.StoreExists(ctx, storeName)
	if err != nil {
		return fmt.Errorf("failed to check STM store existence: %w", err)
	}

	if !exists {
		tx, err := s.systemDB.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for STM initialization: %w", err)
		}

		store, err := s.systemDB.NewBtree(ctx, storeName, tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("failed to initialize STM buffer store: %w", err)
		}

		// Insert a root anchor item to give identity to the btree immediately upon DDL.
		rootAnchor := map[string]any{
			"id":         "root_anchor",
			"user_id":    "system",
			"session_id": "system",
			"intent":     "Initialization",
			"ts":         time.Now().UnixMilli(),
			"type":       "system_anchor",
			"thought":    "Active memory root anchor initialized.",
		}
		_, err = store.Add(ctx, "root_anchor", rootAnchor)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("failed to insert root anchor into STM buffer store: %w", err)
		}

		err = tx.Commit(ctx)
		if err != nil {
			return fmt.Errorf("failed to commit STM initialization transaction: %w", err)
		}
		log.Info("ShortTermMemory: Successfully initialized DDL and root anchor for 'user_scratchpad'")
	}

	// Launch single-threaded sequence batch writer for DML
	s.StartShortTermMemoryBatchWriter(context.Background())

	return nil
}

// StartShortTermMemoryBatchWriter launches the dedicated background worker that
// reads episodes from the channel and batches them into single transactions.
func (s *Service) StartShortTermMemoryBatchWriter(ctx context.Context) {
	go func() {
		log.Info("ShortTermMemory: Batch Writer Worker Started")
		storeName := "user_scratchpad"

		for {
			select {
			case <-ctx.Done():
				log.Info("ShortTermMemory: Batch Writer Worker Interrupted")
				return
			case firstPayload := <-s.episodeQueue:
				tx, err := s.systemDB.BeginTransaction(ctx, sop.ForWriting)
				if err != nil {
					log.Warn("ShortTermMemory batcher: Failed to begin transaction", "error", err)
					continue
				}

				store, err := s.systemDB.OpenBtree(ctx, storeName, tx)
				if err != nil {
					log.Warn("ShortTermMemory batcher: Failed to open BTree", "error", err)
					tx.Rollback(ctx)
					continue
				}

				batchCount := 0

				// Add first item
				ok, err := store.Add(ctx, firstPayload["id"].(string), firstPayload)
				if err != nil {
					log.Warn("ShortTermMemory batcher: Failed to add item", "error", err)
				} else if !ok {
					store.Update(ctx, firstPayload["id"].(string), firstPayload)
				}
				batchCount++

				// Batch remaining items
				timeout := time.After(5 * time.Second)
				batching := true
				for batching && batchCount < 500 {
					select {
					case nextPayload := <-s.episodeQueue:
						pk := nextPayload["id"].(string)
						ok, err := store.Add(ctx, pk, nextPayload)
						if err != nil {
							log.Warn("ShortTermMemory batcher: Failed to add item", "error", err)
							continue
						}
						if !ok {
							store.Update(ctx, pk, nextPayload)
						}
						batchCount++
					case <-timeout:
						batching = false
					case <-ctx.Done():
						batching = false
					}
				}

				if err := tx.Commit(ctx); err != nil {
					log.Warn("ShortTermMemory batcher: Transaction commit failed", "error", err)
				} else {
					log.Info("ShortTermMemory: Episode batch buffered to STM successfully", "count", batchCount)
				}
			}
		}
	}()
}

// StartShortTermMemorySleepCycle launches a background consolidator that sweeps TempVectors
// taking Short-Term episodic memories and migrating them into Semantic Long-Term Categories.
// Call this once on Service initialization if EnableShortTermMemory = true.
func (s *Service) StartShortTermMemorySleepCycle(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		log.Info("ShortTermMemory: Initiating Sleep Cycle Background Worker", "interval", interval)

		for {
			select {
			case <-ctx.Done():
				log.Info("ShortTermMemory: Sleep Cycle Interrupted (Context Done)")
				return
			case <-ticker.C:
				if !s.EnableShortTermMemory {
					continue
				}

				if s.domain == nil || s.systemDB == nil {
					continue
				}

				// Use a background context so it does not conflict with or reuse the caller's transaction
				bgCtx := context.Background()

				// 1. Open read-write on systemDB to get the buffered episodes
				sysTx, err := s.systemDB.BeginTransaction(bgCtx, sop.ForWriting)
				if err != nil {
					log.Warn("ShortTermMemory: Sleep Cycle Failed to start sysTx", "error", err)
					continue
				}

				storeName := "user_scratchpad"
				store, err := s.systemDB.OpenBtree(bgCtx, storeName, sysTx)
				if err != nil {
					sysTx.Rollback(bgCtx)
					log.Warn("ShortTermMemory: Sleep Cycle Failed to open scratchpad", "error", err)
					continue
				}

				var thoughts []memory.Thought[map[string]any]
				var itemIDs []string

				ok, _ := store.First(bgCtx)
				for ok {
					key := store.GetCurrentKey()
					// Skip the root anchor
					if key.Key != "root_anchor" {
						val, _ := store.GetCurrentValue(bgCtx)

						log.Info("ShortTermMemory: Inspecting scratchpad item", "type", fmt.Sprintf("%T", val))

						if payload, valid := val.(map[string]any); valid {
							thoughtText := ""
							if txt, has := payload["thought"].(string); has {
								thoughtText = txt
							}

							thoughts = append(thoughts, memory.Thought[map[string]any]{
								Summaries: []string{thoughtText},
								Data:      payload,
								// kb.IngestThoughts handles missing vectors automatically!
							})
							itemIDs = append(itemIDs, key.Key)
						} else {
							log.Info("ShortTermMemory: Scratchpad item has invalid type, ignoring payload", "type", fmt.Sprintf("%T", val), "key", key.Key)
							itemIDs = append(itemIDs, key.Key)
						}
					}
					ok, _ = store.Next(bgCtx)
				}

				if len(thoughts) == 0 {
					log.Info("ShortTermMemory: Sleep Cycle found 0 valid thoughts...")
					sysTx.Rollback(bgCtx)
					continue
				}

				log.Info("ShortTermMemory: Sleep Cycle Triggered. Consolidating memories...", "count", len(thoughts))

				// 2. Open ForWriting on Domain for KnowledgeBase ingestion
				domTx, err := s.domain.BeginTransaction(bgCtx, sop.ForWriting)
				if err != nil {
					sysTx.Rollback(bgCtx)
					log.Warn("ShortTermMemory: Sleep Cycle Failed to get domain transaction", "error", err)
					continue
				}

				memStoreAny, err := s.domain.Memory(bgCtx, domTx)
				if err != nil {
					domTx.Rollback(bgCtx)
					sysTx.Rollback(bgCtx)
					log.Warn("ShortTermMemory: Sleep Cycle Failed to get memory store", "error", err)
					continue
				}

				kb, ok := memStoreAny.(*memory.KnowledgeBase[map[string]any])
				if !ok {
					domTx.Rollback(bgCtx)
					sysTx.Rollback(bgCtx)
					log.Warn("ShortTermMemory: Sleep Cycle Failed, memory interface invalid")
					continue
				}

				// Perform sleep cycle reorganization / ingestion
				err = kb.IngestThoughts(bgCtx, thoughts, "System")
				if err != nil {
					domTx.Rollback(bgCtx)
					sysTx.Rollback(bgCtx)
					log.Warn("ShortTermMemory: Sleep Cycle Consolidation encountered an error during ingestion", "error", err)
					continue
				}

				err = kb.TriggerSleepCycle(bgCtx)
				if err != nil {
					log.Warn("ShortTermMemory: TriggerSleepCycle reorganization logic returned error", "error", err)
				}

				err = domTx.Commit(bgCtx)
				if err != nil {
					sysTx.Rollback(bgCtx)
					log.Warn("ShortTermMemory: Consolidation commit failed", "error", err)
					continue
				}

				// 3. Remove processed items from scratchpad
				for _, id := range itemIDs {
					_, err = store.Remove(bgCtx, id)
					if err != nil {
						log.Warn("ShortTermMemory: Failed to remove scrubbed item from scratchpad", "id", id, "error", err)
					}
				}

				err = sysTx.Commit(bgCtx)
				if err != nil {
					log.Warn("ShortTermMemory: Failed to commit removals from scratchpad", "error", err)
				} else {
					log.Debug("ShortTermMemory: Sleep Cycle completed successfully.")
				}
			}
		}
	}()
}

// logEpisode intercepts script execution outcomes and embeds them into the TempVectors store (Short-Term Memory).
// This runs async via a goroutine to prevent blocking the interactive user loop.
func (s *Service) logEpisode(ctx context.Context, intent string, astPayload any, outcome string, executeErr error) {
	if s.systemDB == nil {
		log.Warn("ShortTermMemory: SystemDB not configured, skipping ingestion.")
		return
	}

	payloadInfo := ai.GetSessionPayload(ctx)
	userID := "system"
	sessionID := "unknown"
	if payloadInfo != nil {
		if payloadInfo.UserID != "" {
			userID = payloadInfo.UserID
		}
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

	log.Debug("ShortTermMemory: Buffering thought snippet to STM", "thought", thought, "user_id", userID)

	// Deterministic Deduplication: Hash the semantic thought payload
	hash := sha256.Sum256([]byte(userID + sessionID + thought))
	itemID := fmt.Sprintf("%x", hash) // NewUUID internally isnt used for STM caching buffer

	payload := map[string]any{
		"id":         itemID,
		"user_id":    userID,
		"session_id": sessionID,
		"intent":     intent,
		"ast":        astBytes,
		"status":     status,
		"error":      errorDesc,
		"outcome":    outcome,
		"thought":    thought,
		"ts":         time.Now().UnixMilli(),
		"type":       "episode",
	}

	// Queue the episode asynchronously to the STM batch writer
	select {
	case s.episodeQueue <- payload:
		log.Debug("ShortTermMemory: Episode queued for batch writing", "temp_id", itemID)
	default:
		log.Warn("ShortTermMemory: Batch queue is full, dropping episode", "temp_id", itemID)
	}
}
