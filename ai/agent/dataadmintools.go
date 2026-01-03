package agent

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

// registerTools registers all available tools for the DataAdminAgent.
func (a *DataAdminAgent) registerTools() {
	if a.registry == nil {
		a.registry = NewRegistry()
	}

	a.registry.Register("list_databases", "Lists all available databases.", "()", a.toolListDatabases)
	a.registry.Register("list_stores", "Lists all stores in the current or specified database.", "(database: string)", a.toolListStores)
	a.registry.Register("select", "Retrieve data from a store. Supports filtering by key subset and value subset. You can optionally specify a list of fields to return (supports 'field AS alias'). Supports 'action'='delete' to delete matching records, or 'action'='update' to update matching records with 'update_values'. Supports MongoDB-style operators in key_match and value_match: $eq, $ne, $gt, $gte, $lt, $lte (e.g. {age: {$gt: 18}}). Supports 'order_by' (e.g. 'key desc').", "(database: string, store: string, limit: number, scan_limit: number, key_match: any, value_match: any, fields: []string, action: string, update_values: map[string]any, order_by: string)", a.toolSelect)
	a.registry.Register("manage_transaction", "Manage database transactions (begin, commit, rollback).", "(action: string)", a.toolManageTransaction)
	a.registry.Register("delete", "Delete an item from a store.", "(store: string, key: any)", a.toolDelete)
	a.registry.Register("add", "Add an item to a store. You can pass the value as a single 'value' argument, or pass individual fields as arguments.", "(store: string, key: any, value: any, ...fields)", a.toolAdd)
	a.registry.Register("update", "Update an item in a store. You can pass the value as a single 'value' argument, or pass individual fields as arguments.", "(store: string, key: any, value: any, ...fields)", a.toolUpdate)
	a.registry.Register("list_macros", "Lists all available macros.", "()", a.toolListMacros)
	a.registry.Register("get_macro_details", "Get details of a specific macro.", "(name: string)", a.toolGetMacroDetails)
	a.registry.Register("macro_insert_step", "Insert a step into a macro.", "(macro: string, index: number, type: string, ...params)", a.toolMacroInsertStep)
	a.registry.Register("macro_delete_step", "Delete a step from a macro.", "(macro: string, index: number)", a.toolMacroDeleteStep)
	a.registry.Register("macro_update_step", "Update a step in a macro.", "(macro: string, index: number, ...params)", a.toolMacroUpdateStep)
	a.registry.Register("macro_reorder_steps", "Move a step in a macro to a new position.", "(macro: string, from_index: number, to_index: number)", a.toolMacroReorderSteps)
	a.registry.Register("macro_add_step_from_last", "Add the last executed tool call as a new step to a macro. If 'index' is not provided, it appends to the end. If 'index' is provided, it inserts 'after' that index by default, unless 'position' is set to 'before'.", "(macro: string, index: number, position: string)", a.toolMacroAddStepFromLast)

	// Navigation tools
	a.registry.Register("find", "Find an item in a store. Returns exact match only. You can optionally specify a list of fields to return.", "(store: string, key: any, fields: []string)", a.toolFind)
	a.registry.Register("find_nearest", "Find an item in a store. If no exact match, returns the nearest items (previous and next). You can optionally specify a list of fields to return.", "(store: string, key: any, fields: []string)", a.toolFindNearest)
	a.registry.Register("next", "Move to the next item in a store.", "(store: string)", a.toolNext)
	a.registry.Register("previous", "Move to the previous item in a store.", "(store: string)", a.toolPrevious)
	a.registry.Register("first", "Move to the first item in a store.", "(store: string)", a.toolFirst)
	a.registry.Register("last", "Move to the last item in a store.", "(store: string)", a.toolLast)
	a.registry.Register("refactor_last_interaction", "Refactor the last interaction's steps into a new macro or block.", "(mode: string, name: string)", a.toolRefactorMacro)
	a.registry.Register("join", "Join two stores. Supports inner, left, right, full joins. Supports joining on multiple fields via 'left_join_fields' and 'right_join_fields'. If 'right_join_fields' contains 'key', it uses efficient lookup; otherwise it performs a scan (slower). Supports 'action'='delete_left' to delete matching records from the left store, or 'action'='update_left' to update them with 'update_values'. You can optionally specify a list of fields to return (supports 'field AS alias'). Supports 'order_by' (e.g. 'key desc').", "(database: string, left_store: string, right_database: string, right_store: string, left_join_fields: []string, right_join_fields: []string, join_type: string, limit: number, action: string, update_values: map[string]any, fields: []string, order_by: string)", a.toolJoin)
}

func (a *DataAdminAgent) toolListDatabases(ctx context.Context, args map[string]any) (string, error) {
	var names []string
	for k := range a.databases {
		names = append(names, k)
	}
	sort.Strings(names)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Databases: %s", strings.Join(names, ", ")))

	if a.systemDB != nil {
		sb.WriteString("\nSystem Database: system")
	}

	return sb.String(), nil
}

func (a *DataAdminAgent) toolListStores(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	var db *database.Database
	dbName, _ := args["database"].(string)
	if dbName == "" {
		dbName = p.CurrentDB
	}

	if dbName != "" {
		if dbName == "system" && a.systemDB != nil {
			db = a.systemDB
		} else if opts, ok := a.databases[dbName]; ok {
			db = database.NewDatabase(opts)
		}
	}

	if db == nil {
		// Debugging
		var keys []string
		for k := range a.databases {
			keys = append(keys, k)
		}
		return "", fmt.Errorf("database not found or not selected. Requested: '%s', Available: %v", dbName, keys)
	}

	// Need transaction
	var tx sop.Transaction
	var autoCommit bool

	if p.Transaction != nil {
		if t, ok := p.Transaction.(sop.Transaction); ok {
			tx = t
		}
	}
	if tx == nil {
		// If no transaction in payload, try to start one on the DB
		if db != nil {
			var err error
			tx, err = db.BeginTransaction(ctx, sop.ForReading)
			if err != nil {
				return "", fmt.Errorf("failed to begin transaction: %w", err)
			}
			// Auto-commit if we started it locally
			autoCommit = true
		} else {
			return "", fmt.Errorf("no active transaction and no database to start one")
		}
	}
	stores, err := tx.GetStores(ctx)
	if err != nil {
		if autoCommit {
			tx.Rollback(ctx)
		}
		return "", fmt.Errorf("failed to list stores: %w", err)
	}
	sort.Strings(stores)
	if autoCommit {
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("failed to commit transaction: %w", err)
		}
	}
	return fmt.Sprintf("Stores: %s", strings.Join(stores, ", ")), nil
}
