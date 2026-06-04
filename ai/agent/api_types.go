package agent

import (
	"time"
)

// TransactionMode controls how transactions are managed in bulk operations
type TransactionMode string

const (
	// TransactionModeAutoBatch creates transactions automatically, commits per batch
	// Use for: Large single-operation bulk inserts (10K+ items)
	TransactionModeAutoBatch TransactionMode = "auto_batch"

	// TransactionModeExplicit uses provided transaction, never auto-commits
	// Use for: Multi-operation atomicity (all-or-nothing across operations)
	TransactionModeExplicit TransactionMode = "explicit"

	// TransactionModeSingle creates ONE transaction for ALL items, single commit at end
	// Use for: Moderate datasets (< 10K items) requiring atomicity
	TransactionModeSingle TransactionMode = "single"
)

// TransactionHandle represents an active transaction
type TransactionHandle struct {
	ID       string    `json:"id"`       // Unique transaction ID
	Database string    `json:"database"` // Database name
	Mode     string    `json:"mode"`     // "read" or "write"
	Started  time.Time `json:"started"`  // When transaction began
}

// TransactionArgs for beginning a transaction
type TransactionArgs struct {
	Database string `json:"database,omitempty" example:"dev_db"`
	Mode     string `json:"mode" binding:"required" enums:"read,write" default:"read"`
}

// TransactionCommitArgs for committing a transaction
type TransactionCommitArgs struct {
	TransactionID string `json:"transaction_id" binding:"required"`
}

// TransactionRollbackArgs for rolling back a transaction
type TransactionRollbackArgs struct {
	TransactionID string `json:"transaction_id" binding:"required"`
}

// BulkAddArgs represents arguments for bulk insert operations
// @Description Bulk insert multiple items with automatic transaction batching
type BulkAddArgs struct {
	Database        string          `json:"database,omitempty" example:"dev_db"`
	Store           string          `json:"store" binding:"required" example:"users"`
	Items           []BulkItem      `json:"items" binding:"required"`
	TransactionID   string          `json:"transaction_id,omitempty"` // Use existing transaction
	TransactionMode TransactionMode `json:"transaction_mode,omitempty" default:"auto_batch" enums:"auto_batch,explicit,single"`
	BatchSize       int             `json:"batch_size,omitempty" default:"100" example:"250"`
}

// BulkUpdateArgs represents arguments for bulk update operations
type BulkUpdateArgs struct {
	Database        string          `json:"database,omitempty" example:"dev_db"`
	Store           string          `json:"store" binding:"required" example:"users"`
	Items           []BulkItem      `json:"items" binding:"required"`
	TransactionID   string          `json:"transaction_id,omitempty"`
	TransactionMode TransactionMode `json:"transaction_mode,omitempty" default:"auto_batch" enums:"auto_batch,explicit,single"`
	BatchSize       int             `json:"batch_size,omitempty" default:"100" example:"250"`
}

// BulkDeleteArgs represents arguments for bulk delete operations
type BulkDeleteArgs struct {
	Database        string          `json:"database,omitempty" example:"dev_db"`
	Store           string          `json:"store" binding:"required" example:"users"`
	Keys            []any           `json:"keys" binding:"required"`
	TransactionID   string          `json:"transaction_id,omitempty"`
	TransactionMode TransactionMode `json:"transaction_mode,omitempty" default:"auto_batch" enums:"auto_batch,explicit,single"`
	BatchSize       int             `json:"batch_size,omitempty" default:"100" example:"250"`
}

// BulkItem represents a single item in a bulk operation
type BulkItem struct {
	Key   any            `json:"key" binding:"required" example:"user_123"`
	Value map[string]any `json:"value" binding:"required"`
}

// BulkOperationResult represents the result of a bulk operation
type BulkOperationResult struct {
	Success                bool                 `json:"success"`
	Processed              int                  `json:"processed"`
	Failed                 int                  `json:"failed"`
	Duration               time.Duration        `json:"duration"`
	Errors                 []BulkOperationError `json:"errors,omitempty"`
	Metrics                BulkOperationMetrics `json:"metrics"`
	TransactionsCreated    int                  `json:"transactions_created"`     // For auto_batch mode
	TransactionsCommitted  int                  `json:"transactions_committed"`   // For auto_batch mode
	TransactionsRolledBack int                  `json:"transactions_rolled_back"` // On error
}

// BulkOperationError represents an error that occurred during a bulk operation
type BulkOperationError struct {
	Index   int    `json:"index"`
	Key     any    `json:"key,omitempty"`
	Message string `json:"message"`
}

// BulkOperationMetrics contains performance metrics for bulk operations
type BulkOperationMetrics struct {
	TotalItems      int           `json:"total_items"`
	BatchesExecuted int           `json:"batches_executed"`
	AvgBatchTime    time.Duration `json:"avg_batch_time"`
	ItemsPerSecond  float64       `json:"items_per_second"`
}

// AddArgs represents arguments for adding a single item
type AddArgs struct {
	Database string         `json:"database,omitempty" example:"dev_db"`
	Store    string         `json:"store" binding:"required" example:"users"`
	Key      any            `json:"key" binding:"required" example:"user_123"`
	Value    map[string]any `json:"value" binding:"required"`
}

// UpdateArgs represents arguments for updating a single item
type UpdateArgs struct {
	Database string         `json:"database,omitempty" example:"dev_db"`
	Store    string         `json:"store" binding:"required" example:"users"`
	Key      any            `json:"key" binding:"required" example:"user_123"`
	Value    map[string]any `json:"value" binding:"required"`
}

// DeleteArgs represents arguments for deleting a single item
type DeleteArgs struct {
	Database string `json:"database,omitempty" example:"dev_db"`
	Store    string `json:"store" binding:"required" example:"users"`
	Key      any    `json:"key" binding:"required" example:"user_123"`
}

// SelectArgs represents arguments for selecting data from a store
type SelectArgs struct {
	Database     string         `json:"database,omitempty" example:"dev_db"`
	Store        string         `json:"store" binding:"required" example:"users"`
	Key          any            `json:"key,omitempty"`
	KeyMatch     any            `json:"key_match,omitempty"`
	Value        map[string]any `json:"value,omitempty"`
	Filter       map[string]any `json:"filter,omitempty"`
	Fields       []string       `json:"fields,omitempty"`
	Limit        int            `json:"limit,omitempty" default:"10"`
	OrderBy      string         `json:"order_by,omitempty"`
	Direction    string         `json:"direction,omitempty" enums:"asc,desc"`
	Action       string         `json:"action,omitempty" enums:"delete,update"`
	UpdateValues map[string]any `json:"update_values,omitempty"`
}

// ExecuteScriptArgs represents arguments for executing a script
type ExecuteScriptArgs struct {
	Database string              `json:"database,omitempty" example:"dev_db"`
	Script   []ScriptInstruction `json:"script" binding:"required"`
}

// JoinArgs represents arguments for joining two stores
type JoinArgs struct {
	Database        string         `json:"database,omitempty" example:"dev_db"`
	LeftStore       string         `json:"left_store" binding:"required" example:"users"`
	RightStore      string         `json:"right_store" binding:"required" example:"orders"`
	LeftJoinFields  []string       `json:"left_join_fields" binding:"required" example:"user_id"`
	RightJoinFields []string       `json:"right_join_fields" binding:"required" example:"user_id"`
	JoinType        string         `json:"join_type,omitempty" default:"inner" enums:"inner,left,right"`
	Fields          []string       `json:"fields,omitempty"`
	Limit           int            `json:"limit,omitempty" default:"10"`
	Direction       string         `json:"direction,omitempty" enums:"asc,desc"`
	Action          string         `json:"action,omitempty" enums:"delete_left,update_left"`
	UpdateValues    map[string]any `json:"update_values,omitempty"`
}
