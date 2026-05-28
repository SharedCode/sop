package memory

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
)

// MemoryUnit encapsulates the cognitive state and boundaries of an Agent instance.
type MemoryUnit struct {
	AgentID    string
	AllowedKBs []string // LTM scoping boundaries

	// Tracks the last time an episode was logged to STM for idle sleep cycles
	LastEpisodeTS atomic.Int64
	// Agent-scoped STM batching queue
	episodeQueue chan map[string]any

	// Episodic B-Tree buffer for the cognitive footprint
	stm btree.BtreeInterface[string, any]
}

func NewMemoryUnit(agentID string) *MemoryUnit {
	return &MemoryUnit{
		AgentID:      agentID,
		AllowedKBs:   []string{},
		episodeQueue: make(chan map[string]any, 100),
	}
}
func (m *MemoryUnit) ShortTermMemory() btree.BtreeInterface[string, any] {
	return m.stm
}

func (m *MemoryUnit) ShortTermMemoryName() string {
	return fmt.Sprintf("stm_%s", m.AgentID)
}
func (m *MemoryUnit) LongTermMemoryName() string {
	return fmt.Sprintf("ltm_%s", m.AgentID)
}

func (m *MemoryUnit) OpenShortTermMemory(ctx context.Context, systemDB Database, trans sop.Transaction) (btree.BtreeInterface[string, any], error) {
	log.Info("MemoryUnit.openShortTermMemory")

	// Prevent opening the STM if already opened.
	if m.stm != nil {
		return m.stm, nil
	}

	// Dynamic store naming based on Avatar ID to ensure physical strict isolation
	stmStoreName := m.ShortTermMemoryName()
	store, err := systemDB.NewBtree(ctx, stmStoreName, trans)
	if err != nil {
		return nil, fmt.Errorf("failed to create/open isolated STM BTree: %w", err)
	}
	m.stm = store

	return m.stm, nil
}

func (m *MemoryUnit) CloseShortTermMemory() {
	m.stm = nil
}

func (m *MemoryUnit) OpenLongTermMemory(ctx context.Context, systemDB Database, trans sop.Transaction, llm ai.Generator, embedder ai.Embeddings) (*KnowledgeBase[map[string]any], error) {
	log.Info("MemoryUnit.OpenLongTermMemory")

	ltmStoreName := m.LongTermMemoryName()
	ltm, err := systemDB.OpenKnowledgeBase(ctx, ltmStoreName, trans, llm, embedder, false, true)
	if err != nil {
		return nil, fmt.Errorf("failed to open isolated LTM KnowledgeBase: %w", err)
	}
	return ltm, nil
}

// LogEpisodeToSTM directly writes to the Agent's physical STM structure
func (m *MemoryUnit) LogEpisodeToSTM(ctx context.Context, intent string, astPayload any, outcome string, executeErr error) {
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

	hash := sha256.Sum256([]byte(thought))
	itemID := fmt.Sprintf("%x", hash)

	payload := map[string]any{
		"id":         itemID,
		"intent":     intent,
		"thought":    thought,
		"status":     status,
		"outcome":    outcome,
		"created_at": time.Now().UnixMilli(),
		"agent_id":   m.AgentID,
	}

	m.LastEpisodeTS.Store(time.Now().UnixMilli())

	select {
	case m.episodeQueue <- payload:
		log.Debug("Isolated STM: Buffered thought snippet to queue successfully", "agent_id", m.AgentID)
	default:
		log.Warn("Isolated STM: Episode queue is full, dropping thought snippet", "agent_id", m.AgentID)
	}
}

// StartMemoryWorkers launches the dedicated background worker that
// reads episodes from the channel and flushes them to STM.
func (m *MemoryUnit) StartMemoryWorkers(ctx context.Context, systemDB Database) error {
	if systemDB == nil {
		return fmt.Errorf("systemDB can't be nil")
	}

	go func() {
		log.Info("MemoryUnit: STM Batch Writer Worker Started", "agent_id", m.AgentID)

		for {
			select {
			case <-ctx.Done():
				log.Info("MemoryUnit: STM Batch Writer Worker Interrupted", "agent_id", m.AgentID)
				return
			case firstPayload := <-m.episodeQueue:

				var tx sop.Transaction
				if m.stm == nil {
					var err error
					tx, err = systemDB.BeginTransaction(ctx, sop.ForWriting)
					if err != nil {
						log.Warn("MemoryUnit batcher: tx begin failed", "error", err)
						continue
					}
					_, err = m.OpenShortTermMemory(ctx, systemDB, tx)
					if err != nil {
						log.Warn("MemoryUnit batcher: tx begin failed", "error", err)
						tx.Rollback(ctx)
						continue
					}
				}

				batchCount := 0
				pk := firstPayload["id"].(string)
				ok, err := m.stm.Add(ctx, pk, firstPayload)
				if err != nil {
					log.Warn("MemoryUnit batcher: Failed to add item", "error", err)
				} else if !ok {
					m.stm.Update(ctx, pk, firstPayload)
				}
				batchCount++

				timeout := time.After(5 * time.Second)
				batching := true
				for batching && batchCount < 500 {
					select {
					case nextPayload := <-m.episodeQueue:
						pk := nextPayload["id"].(string)
						ok, err := m.stm.Add(ctx, pk, nextPayload)
						if err != nil {
							log.Warn("CopilotAgent batcher: Failed to add item", "error", err)
							continue
						}
						if !ok {
							m.stm.Update(ctx, pk, nextPayload)
						}
						batchCount++
					case <-timeout:
						batching = false
					case <-ctx.Done():
						batching = false
					}
				}

				tx.Commit(ctx)
				m.stm = nil
				log.Debug("CopilotAgent: Episode batch buffered to STM successfully", "count", batchCount, "agent_id", m.AgentID)
			}
		}
	}()

	return nil
}
