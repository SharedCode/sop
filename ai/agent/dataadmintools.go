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

	// Script Management
	a.registry.Register("list_scripts", "Lists all available scripts.", "()", a.toolListScripts)
	a.registry.Register("get_script_details", "Get details of a specific script.", "(name: string)", a.toolGetScriptDetails)
	a.registry.Register("script_insert_step", "Insert a step into a script.", "(script: string, index: number, type: string, ...params)", a.toolScriptInsertStep)
	a.registry.Register("script_delete_step", "Delete a step from a script.", "(script: string, index: number)", a.toolScriptDeleteStep)
	a.registry.Register("script_update_step", "Update a step in a script.", "(script: string, index: number, ...params)", a.toolScriptUpdateStep)
	a.registry.Register("script_reorder_steps", "Move a step in a script to a new position.", "(script: string, from_index: number, to_index: number)", a.toolScriptReorderSteps)
	a.registry.Register("script_add_step_from_last", "Add the last executed tool call as a new step to a script. If 'index' is not provided, it appends to the end. If 'index' is provided, it inserts 'after' that index by default, unless 'position' is set to 'before'.", "(script: string, index: number, position: string)", a.toolScriptAddStepFromLast)
	a.registry.Register("refactor_last_interaction", "Refactor the last interaction's steps into a new script or block.", "(mode: string, name: string)", a.toolRefactorScript)

	// High-Level Tools
	a.registry.Register("select", "Selects data from a store. Arguments: store (string), key (any, optional), value (any, optional), fields (list<string>, optional), limit (number, optional), order_by (string, optional, e.g. 'field desc'), action (string, optional: 'delete', 'update'), update_values (map, optional).", "(store: string, ...)", a.toolSelect)
	a.registry.RegisterHidden("join", "Joins data from two stores. Arguments: left_store (string), right_store (string), left_join_fields (list<string>), right_join_fields (list<string>), join_type (string, optional: 'inner', 'left', 'right'), fields (list<string>, optional), limit (number, optional), order_by (string, optional), action (string, optional: 'delete_left', 'update_left'), update_values (map, optional).", "(left_store: string, right_store: string, ...)", a.toolJoin)
	a.registry.Register("add", "Adds data to a store.", "(store: string, key: any, value: any)", a.toolAdd)
	a.registry.Register("update", "Updates data in a store.", "(store: string, key: any, value: any)", a.toolUpdate)
	a.registry.Register("delete", "Deletes data from a store.", "(store: string, key: any)", a.toolDelete)
	a.registry.Register("manage_transaction", "Manages transactions (begin, commit, rollback).", "(action: string)", a.toolManageTransaction)

	// The Core Engine
	a.registry.Register("execute_script", `Execute a programmatic script to interact with databases. Use this for complex multi-step operations not covered by high-level tools. Supports variables, transaction management, B-Tree cursor navigation, and memory list manipulation.

Operations:
- open_db(name) -> db
- begin_tx(database, mode) -> tx
- commit_tx(transaction)
- rollback_tx(transaction)
- open_store(transaction, name) -> store
- scan(store, limit, direction, start_key, prefix, filter, stream=true) -> cursor
- find(store, key, desc) -> bool
- next(store) -> bool
- previous(store) -> bool
- first(store) -> bool
- last(store) -> bool
- get_current_key(store) -> key
- get_current_value(store) -> value
- add(store, key, value)
- update(store, key, value)
- delete(store, key)
- list_new() -> list
- list_append(list, item)
- map_merge(map1, map2) -> map
- sort(input, fields) -> list
- filter(input, condition) -> cursor/list
- project(input, fields) -> cursor/list
- limit(input, limit) -> cursor/list
- join(input, with, type, on) -> cursor/list
- join_right(input, store, type, on) -> cursor/list (Pipeline alias for join)
- if(condition, then, else)
- loop(condition, body)
- call_script(name, params)
- return(value) -> stops execution and returns value

Example Pipeline Join:
[
  {"op": "open_db", "args": {"name": "mydb"}},
  {"op": "begin_tx", "args": {"database": "mydb", "mode": "read"}, "result_var": "tx1"},
  {"op": "open_store", "args": {"transaction": "tx1", "name": "users"}, "result_var": "users"},
  {"op": "open_store", "args": {"transaction": "tx1", "name": "orders"}, "result_var": "orders"},
  {"op": "scan", "args": {"store": "users", "stream": true}, "result_var": "stream"},
  {"op": "join_right", "args": {"store": "orders", "on": {"user_id": "user_id"}}, "input_var": "stream", "result_var": "stream"},
  {"op": "limit", "args": {"limit": 5}, "input_var": "stream", "result_var": "output"},
  {"op": "commit_tx", "args": {"transaction": "tx1"}}
]`, "(script: Array<{op: string, args?: object, input_var?: string, result_var?: string}>)", a.toolExecuteScript)
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
	stores, err := tx.GetPhasedTransaction().GetStores(ctx)
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
