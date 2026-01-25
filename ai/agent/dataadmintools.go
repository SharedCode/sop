package agent

import (
	"context"
	"fmt"
	"sort"
	"strings"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	core "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/jsondb"
)

const ExecuteScriptInstruction = `Execute a programmatic script to interact with databases. Use this for complex multi-step operations not covered by high-level tools. Supports variables, transaction management, B-Tree cursor navigation, and memory list manipulation.

Important:
1. Inspect Schema First: Use 'list_stores' to discover stores and their schema, use field names when referencing, e.g. writing projection fields, filtering logic.
2. Inspect the "relations" fields of the store schemas to determine the correct join logic and optimized access paths.
3. When joining using a Secondary Index or KV store, respect the field names in the 'Relation'. If a Relation maps '[Value]' to 'target_id', use 'Value' in your 'on' clause (e.g. {"on": {"Value": "target_id"}}). Do not assume the source has the target's field name.
4. 'scan' and 'join' return full objects. To select specific fields or renamed columns, you MUST add a 'project' step. If the user asks for "all entities" (e.g. "all users"), prioritize projecting "store.*" (e.g. ["users.*"]) to return all fields of that entity.
5. For large datasets, prefer using 'limit' to avoid memory exhaustion.
6. Store names are sometimes entity's plural form or singular form, e.g. user entity stored in users store.
7. Field names sometimes use underscore('_') separator instead of space(' '), e.g. - "total amount" as field name is "total_amount".

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
- project(input, fields) -> cursor/list (fields: list<string> ['field', 'field AS alias', 'a.*'] (PREFERRED), or map {alias: field} (no ordering guaranteed))
- limit(input, limit) -> cursor/list
- join(input, with, type, on) -> cursor/list
- join_right(input, store, type, on) -> cursor/list (Pipeline alias for join)
- if(condition, then, else)
- loop(condition, body)
- call_script(name, params)
- select(store, key, value, fields, limit, order_by, action, update_values) -> list (high-level tool integration)
- return(value) -> stops execution and returns value

Example Pipeline Join:
[
  {"op": "open_db", "args": {"name": "mydb"}},
  {"op": "begin_tx", "args": {"database": "mydb", "mode": "read"}, "result_var": "tx1"},
  {"op": "open_store", "args": {"transaction": "tx1", "name": "users"}, "result_var": "users"},
  {"op": "open_store", "args": {"transaction": "tx1", "name": "orders"}, "result_var": "orders"},
  {"op": "scan", "args": {"store": "users", "stream": true}, "result_var": "stream"},
  {"op": "join_right", "args": {"store": "orders", "on": {"user_id": "user_id"}}, "input_var": "stream", "result_var": "stream"},
    {"op": "project", "args": {"fields": ["name", "order_date"]}, "input_var": "stream", "result_var": "projected"},
  {"op": "limit", "args": {"limit": 5}, "input_var": "projected", "result_var": "output"},
  {"op": "commit_tx", "args": {"transaction": "tx1"}}
]
Note: 'scan' and 'join' return full objects. To select specific fields or renamed columns, you MUST add a 'project' step.`

const (
	SelectInstruction = "Selects data from a store. Arguments: store (string), key (any, optional), value (any, optional), fields (list<string>, optional. Use ['*'] or nil for all fields. Supported formats: ['*'], ['field1', 'field2'], ['field AS alias'], ['a.*', 'b.name AS employee']), limit (number, optional), order_by (string, optional, e.g. 'field desc'), action (string, optional: 'delete', 'update'), update_values (map, optional)."

	JoinInstruction = "Joins data from two stores. Arguments: left_store (string), right_store (string), left_join_fields (list<string>), right_join_fields (list<string>), join_type (string, optional: 'inner', 'left', 'right'), fields (list<string>, optional. Use ['*'] or nil for all fields. Supported formats: ['*'], ['field1', 'field2'], ['field AS alias'], ['a.*', 'b.name AS employee']), limit (number, optional), order_by (string, optional), action (string, optional: 'delete_left', 'update_left'), update_values (map, optional)."

	AddInstruction               = "Adds data to a store."
	UpdateInstruction            = "Updates data in a store."
	DeleteInstruction            = "Deletes data from a store."
	ManageTransactionInstruction = "Manages transactions (begin, commit, rollback)."
)

// registerTools registers all available tools for the DataAdminAgent.
func (a *DataAdminAgent) registerTools(ctx context.Context) {
	log.Debug("DataAdminAgent.registerTools: Starting registration...")
	if a.registry == nil {
		a.registry = NewRegistry()
	}

	a.registry.Register("list_databases", "Lists all available databases.", "()", a.toolListDatabases)
	a.registry.Register("list_stores", "Lists all stores in the current or specified database.", "(database: string)", a.toolListStores)

	// Script Management
	a.registry.Register("list_scripts", "Lists all available scripts.", "()", a.toolListScripts)
	a.registry.Register("create_script", "Creates a new script.", "(name: string, description: string, steps: list<object> (optional, e.g. [{'type':'command', 'command':'select', 'args':{...}}]))", a.toolCreateScript)
	a.registry.Register("save_script", "Saves a full script definition (create or overwrite).", "(name: string, description: string, steps: list<object>)", a.toolSaveScript)
	a.registry.Register("get_script_details", "Get details of a specific script.", "(name: string)", a.toolGetScriptDetails)
	a.registry.Register("save_step", "Appends a new step to a script. Usage: save_step(script='MyScript', type='command', command='select', ...).", "(script: string, ...step_def)", a.toolScriptSaveStep)
	a.registry.Register("insert_step", "Insert a step into a script.", "(script: string, index: number, type: string, description: string, name: string, ...params)", a.toolScriptInsertStep)
	a.registry.Register("delete_step", "Delete a step from a script.", "(script: string, index: number)", a.toolScriptDeleteStep)
	a.registry.Register("update_step", "Update a step in a script.", "(script: string, index: number, description: string, name: string, ...params)", a.toolScriptUpdateStep)
	a.registry.Register("reorder_steps", "Move a step in a script to a new position.", "(script: string, from_index: number, to_index: number)", a.toolScriptReorderSteps)
	a.registry.Register("add_step_from_last", "Add the last executed tool call as a new step to a script. If 'index' is not provided, it appends to the end. If 'index' is provided, it inserts 'after' that index by default, unless 'position' is set to 'before'.", "(script: string, index: number, position: string, description: string, name: string)", a.toolScriptAddStepFromLast)
	a.registry.Register("refactor_last_interaction", "Refactor the last interaction's steps into a new script or block.", "(mode: string, name: string)", a.toolRefactorScript)

	// High-Level Tools
	a.registry.Register("select", a.getToolInstruction(ctx, "select", SelectInstruction), "(store: string, ...)", a.toolSelect)
	a.registry.RegisterHidden("join", a.getToolInstruction(ctx, "join", JoinInstruction), "(left_store: string, right_store: string, ...)", a.toolJoin)
	a.registry.Register("add", a.getToolInstruction(ctx, "add", AddInstruction), "(store: string, key: any, value: any)", a.toolAdd)
	a.registry.Register("update", a.getToolInstruction(ctx, "update", UpdateInstruction), "(store: string, key: any, value: any)", a.toolUpdate)
	a.registry.Register("delete", a.getToolInstruction(ctx, "delete", DeleteInstruction), "(store: string, key: any)", a.toolDelete)
	a.registry.Register("manage_transaction", a.getToolInstruction(ctx, "manage_transaction", ManageTransactionInstruction), "(action: string)", a.toolManageTransaction)

	// The Core Engine
	a.registry.Register("execute_script", a.getToolInstruction(ctx, "execute_script", ExecuteScriptInstruction), "(script: Array<{op: string, args?: object, input_var?: string, result_var?: string}>)", a.toolExecuteScript)

	// Self-Correction Tools
	a.registry.Register("manage_knowledge", "Manages the AI's long-term knowledge base. Use this to save, retrieve, or list learned information. Namespaces organize knowledge (e.g. 'term', 'tool', 'finance', 'project_alpha'). Action: 'upsert', 'delete', 'read', 'list'. For 'list', key is ignored.", "(namespace: string, key: string, value: string, action: string)", a.toolManageKnowledge)

	// Conversation Management
	a.registry.Register("conclude_topic", "Conclusion of the current conversation thread. Use this when the user is satisfied, a resolution is reached, or to summarize before moving to a new topic. This saves the summary to memory and cleans up the context.", "(summary: string, topic_label: string)", a.toolConcludeTopic)
}
func (a *DataAdminAgent) getToolInstruction(ctx context.Context, toolName, defaultInst string) string {
	if a.systemDB == nil {
		return defaultInst
	}

	// Start a read-only transaction
	tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		log.Warn("Failed to start transaction for "+KnowledgeStore, "error", err)
		return defaultInst
	}
	defer tx.Rollback(ctx)

	// Open the store
	// We use the raw B-Tree accessor (core.OpenBtree) to bypass higher-level abstractions
	// and access the KnowledgeKey composite key structure directly.
	store, err := core.OpenBtree[KnowledgeKey, string](ctx, a.systemDB.Options(), KnowledgeStore, tx, nil)
	if err != nil {
		// This is expected if the store hasn't been created yet or isn't compatible
		return defaultInst
	}

	// Look up the instruction using the "tool" namespace
	found, err := store.Find(ctx, KnowledgeKey{Category: "tool", Name: toolName}, false)
	if err != nil || !found {
		return defaultInst
	}

	val, err := store.GetCurrentValue(ctx)

	if err != nil {
		return defaultInst
	}
	return val
}

// toolConcludeTopic is a placeholder. The actual logic requires Session access and is handled/overridden in Service.
func (a *DataAdminAgent) toolConcludeTopic(ctx context.Context, args map[string]interface{}) (string, error) {
	return "Topic concluded.", nil
}
func (a *DataAdminAgent) getSystemInstructions(ctx context.Context, defaultInst string) string {
	if a.systemDB == nil {
		return defaultInst
	}

	// Start a read-only transaction
	tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return defaultInst
	}
	defer tx.Rollback(ctx)

	// Open the store
	store, err := core.OpenBtree[KnowledgeKey, string](ctx, a.systemDB.Options(), KnowledgeStore, tx, nil)
	if err != nil {
		return defaultInst
	}

	// Look up the instruction using the "memory" namespace
	found, err := store.Find(ctx, KnowledgeKey{Category: "memory", Name: "system_prompt"}, false)
	if err != nil || !found {
		return defaultInst
	}

	val, err := store.GetCurrentValue(ctx)
	if err != nil {
		return defaultInst
	}
	return val
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

	// Enrich with brief schema info
	var descriptions []string

	// Resolve DatabaseOptions for inspection
	var dbOpts sop.DatabaseOptions
	var hasOpts bool

	if dbName != "system" {
		dbOpts, hasOpts = a.databases[dbName]
	}

	for _, sName := range stores {
		desc := sName
		if hasOpts {
			// Peek for schema to guide the LLM
			// Use a sub-scope or just open. jsondb.OpenStore is idempotent-ish for the same tx.
			// Note: We ignore errors here because listing stores should succeed even if inspection fails.
			s, err := jsondb.OpenStore(ctx, dbOpts, sName, tx)
			if err == nil {
				if ok, _ := s.First(ctx); ok {
					k := s.GetCurrentKey()
					v, _ := s.GetCurrentValue(ctx)
					flat := flattenItem(k, v)
					schema := inferSchema(flat)
					desc = fmt.Sprintf("%s schema=%s", sName, formatSchema(schema))
				}
			}
		}
		descriptions = append(descriptions, desc)
	}

	if autoCommit {
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("failed to commit transaction: %w", err)
		}
	}
	return fmt.Sprintf("Stores:\n%s", strings.Join(descriptions, "\n")), nil
}

func (a *DataAdminAgent) toolManageKnowledge(ctx context.Context, args map[string]any) (string, error) {
	if a.systemDB == nil {
		return "", fmt.Errorf("system database not available")
	}

	namespace, _ := args["namespace"].(string)
	key, _ := args["key"].(string)
	value, _ := args["value"].(string)
	action, _ := args["action"].(string)

	if namespace == "" || key == "" {
		return "", fmt.Errorf("arguments 'namespace' and 'key' are required")
	}
	if action == "" {
		action = "upsert" // Default
	}

	// Validate namespace
	if namespace == "" {
		return "", fmt.Errorf("namespace is required")
	}

	// Start a read-write transaction
	tx, err := a.systemDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return "", fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() {
		if tx.HasBegun() {
			tx.Rollback(ctx)
		}
	}()

	// Open the Knowledge Store
	ks, err := OpenKnowledgeStore(ctx, tx, a.systemDB.Options())
	if err != nil {
		return "", fmt.Errorf("failed to open knowledge store: %w", err)
	}

	var resultMsg string

	if action == "delete" {
		if ok, err := ks.Remove(ctx, namespace, key); err != nil {
			return "", fmt.Errorf("failed to remove key '%s/%s': %w", namespace, key, err)
		} else if !ok {
			return fmt.Sprintf("Key '%s/%s' not found, nothing to delete.", namespace, key), nil
		}
		resultMsg = fmt.Sprintf("Successfully removed knowledge for '%s/%s'.", namespace, key)
	} else if action == "read" || action == "get" {
		val, found, err := ks.Get(ctx, namespace, key)
		if err != nil {
			return "", fmt.Errorf("failed to read key '%s/%s': %w", namespace, key, err)
		} else if !found {
			return fmt.Sprintf("Key '%s/%s' not found.", namespace, key), nil
		}
		resultMsg = fmt.Sprintf("Knowledge '%s/%s':\n%s", namespace, key, val)
	} else if action == "list" {
		// List all keys in the namespace
		items, err := ks.ListContent(ctx, namespace)
		if err != nil {
			return "", fmt.Errorf("failed to list knowledge: %w", err)
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Keys in namespace '%s':\n", namespace))

		if len(items) == 0 {
			sb.WriteString("(No keys found)")
		} else {
			// Sort keys for deterministic output? ListContent comes from B-Tree so it might be sorted by name already if ListContent iterates.
			// Actually the map iteration order is random. We should probably return a sorted list from ListContent or sort here.
			// Let's just iterate the map, random order is fine for LLM usually, but sorted is better.
			// ListContent in knowledge.go iterates B-Tree so keys are read in order, but put into map.
			// Optimization: ListContent should probably return sorted slice.
			// For now, let's just dump.
			for k, v := range items {
				// Truncate value
				shortVal := v
				if len(shortVal) > 50 {
					shortVal = shortVal[:47] + "..."
				}
				sb.WriteString(fmt.Sprintf("- %s: %s\n", k, shortVal))
			}
		}
		resultMsg = sb.String()
	} else {
		// Default to upsert
		if value == "" {
			return "", fmt.Errorf("argument 'value' is required for upsert action")
		}
		if err := ks.Upsert(ctx, namespace, key, value); err != nil {
			return "", fmt.Errorf("failed to upsert knowledge for '%s/%s': %w", namespace, key, err)
		}
		resultMsg = fmt.Sprintf("Successfully saved knowledge for '%s/%s'.", namespace, key)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	return resultMsg, nil
}
