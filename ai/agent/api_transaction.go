package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sharedcode/sop/ai"
)

// BeginTransaction starts a new transaction and returns a handle
func (a *CopilotAgent) BeginTransaction(ctx context.Context, args TransactionArgs) (*TransactionHandle, error) {
	// Get session payload for database resolution
	p := ai.GetSessionPayload(ctx)
	database := args.Database
	if database == "" && p != nil {
		database = p.CurrentDB
	}
	if database == "" {
		return nil, fmt.Errorf("database is required")
	}

	// Validate mode
	mode := args.Mode
	if mode == "" {
		mode = "read"
	}
	if mode != "read" && mode != "write" {
		return nil, fmt.Errorf("mode must be 'read' or 'write'")
	}

	// Delegate to existing tool
	argsMap := map[string]any{
		"action":   "begin",
		"database": database,
		"mode":     mode,
	}

	_, err := a.toolManageTransaction(ctx, argsMap)
	if err != nil {
		return nil, err
	}

	// Generate unique transaction ID
	txID := uuid.New().String()

	// Store transaction handle (the existing implementation should track this)
	handle := &TransactionHandle{
		ID:       txID,
		Database: database,
		Mode:     mode,
		Started:  time.Now(),
	}

	return handle, nil
}

// CommitTransaction commits an active transaction
func (a *CopilotAgent) CommitTransaction(ctx context.Context, args TransactionCommitArgs) error {
	if args.TransactionID == "" {
		return fmt.Errorf("transaction_id is required")
	}

	// Delegate to existing tool
	argsMap := map[string]any{
		"action": "commit",
		// Note: Current implementation uses session-tracked transaction
		// May need to enhance toolManageTransaction to support explicit IDs
	}

	_, err := a.toolManageTransaction(ctx, argsMap)
	return err
}

// RollbackTransaction rolls back an active transaction
func (a *CopilotAgent) RollbackTransaction(ctx context.Context, args TransactionRollbackArgs) error {
	if args.TransactionID == "" {
		return fmt.Errorf("transaction_id is required")
	}

	// Delegate to existing tool
	argsMap := map[string]any{
		"action": "rollback",
	}

	_, err := a.toolManageTransaction(ctx, argsMap)
	return err
}
