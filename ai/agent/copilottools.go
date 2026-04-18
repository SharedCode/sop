package agent

import (
	"context"
	"encoding/json"
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

const ExecuteScriptInstruction = `Execute a programmatic script to interact with databases. Use this for complex multi-step operations not covered by high-level tools. (For detailed DSL operations, refer to your knowledge base).
IMPORTANT RULES:
1. ONLY use valid script operations explicitly defined in the AST Grammar.
2. DO NOT use high-level tools (like 'select' or 'add') inside execute_script unless explicitly allowed.
3. EVERY script MUST start with 'begin_tx' (e.g. {"op": "begin_tx", "result_var": "tx1"}) and end with 'commit_tx'.
4. Pass the transaction variable (e.g. "tx1") to 'open_store' via the 'transaction' argument.
5. 'scan' and 'join' return full objects. To project specific fields, you MUST add a 'project' step.
6. When joining using a Secondary Index, respect the field names in the 'Relation'. e.g., if it maps '[Value]' to 'target_id', use 'Value' in your 'on' clause.
7. Group atomic operations (scan, filter, join, project) into a single 'execute_script' block.
8. Inspect Schema First using 'list_stores' to discover stores, field names, and relations before scripting.`

const (
	SelectInstruction = "Selects data from a store. Supported formats for fields: ['*'], ['field1', 'field2'], ['field AS alias'], ['a.*', 'b.name AS employee']. The order of this list is respected in the output. Note: select only sorts by the store PRIMARY KEY. To sort by other fields, you must use execute_script with an index store."

	JoinInstruction = "Joins data from two stores. Supported formats for fields: ['*'], ['field1', 'field2'], ['field AS alias'], ['a.*', 'b.name AS employee']. The order of this list is respected in the output. Note: only sorts by the primary key."

	AddInstruction               = "Adds data to a store."
	UpdateInstruction            = "Updates data in a store."
	DeleteInstruction            = "Deletes data from a store."
	ManageTransactionInstruction = "Manages transactions (begin, commit, rollback)."
)

// registerTools registers all available tools for the CopilotAgent.
func (a *CopilotAgent) registerTools(ctx context.Context) {
	log.Debug("CopilotAgent.registerTools: Starting registration...")
	if a.registry == nil {
		a.registry = NewRegistry()
	}

	a.registry.Register("list_databases", "Lists all available databases.", "()", a.toolListDatabases)
	// a.registry.Register("switch_database", "Switches the active database context for the AI and the user UI.", "(database: string)", a.toolSwitchDatabase)
	a.registry.Register("list_stores", "Lists all stores in the current or specified database.", "(database: string)", a.toolListStores)
	a.registry.Register("list_tools", "Lists all available tools and their usage instructions.", "()", a.toolListTools)

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
	a.registry.Register("save_last_step", "Add the last executed tool call as a new step to a script. If 'index' is not provided, it appends to the end. If 'index' is provided, it inserts 'after' that index by default, unless 'position' is set to 'before'.", "(script: string, index: number, position: string, description: string, name: string)", a.toolScriptAddStepFromLast)
	a.registry.Register("refactor_last_interaction", "Refactor the last interaction's steps into a new script or block.", "(mode: string, name: string)", a.toolRefactorScript)

	// High-Level Tools
	a.registry.RegisterWithUI("select", "Selects data from a store using criteria.", a.getToolInstruction(ctx, "select", SelectInstruction), "(store: string, key?: any, value?: any, fields?: Array<string>, limit?: number, direction?: \"asc\" | \"desc\", action?: \"delete\" | \"update\", update_values?: object)", a.toolSelect)
	a.registry.RegisterHidden("join", a.getToolInstruction(ctx, "join", JoinInstruction), "(left_store: string, right_store: string, left_join_fields: Array<string>, right_join_fields: Array<string>, join_type?: \"inner\" | \"left\" | \"right\", fields?: Array<string>, limit?: number, direction?: \"asc\" | \"desc\", action?: \"delete_left\" | \"update_left\", update_values?: object)", a.toolJoin)
	a.registry.Register("explain_join", "Predicts the execution strategy (Index Scan vs Full Scan) for a join operation. Useful for performance debugging.", "(right_store: string, on: map, database?: string)", a.toolExplainJoin)
	// a.registry.Register("fetch", "Fetches raw key/value pairs from a store. Useful for diagnostics to see the actual B-Tree data. Supports optional direct key lookup, prefix scan, or filtering on Key fields.", "(store: string, key?: any, limit?: number, prefix?: string, filter?: map)", a.toolFetch)
	a.registry.RegisterWithUI("add", "Adds data to a store.", a.getToolInstruction(ctx, "add", AddInstruction), "(store: string, key: any, value: any)", a.toolAdd)
	a.registry.RegisterWithUI("update", "Updates data in a store.", a.getToolInstruction(ctx, "update", UpdateInstruction), "(store: string, key: any, value: any)", a.toolUpdate)
	a.registry.RegisterWithUI("delete", "Deletes data from a store.", a.getToolInstruction(ctx, "delete", DeleteInstruction), "(store: string, key: any)", a.toolDelete)
	a.registry.RegisterWithUI("manage_transaction", "Manages transactions (begin, commit, rollback).", a.getToolInstruction(ctx, "manage_transaction", ManageTransactionInstruction), "(action: \"begin\" | \"commit\" | \"rollback\")", a.toolManageTransaction)

	// The Core Engine
	var ops = "\"open_db\" | \"begin_tx\" | \"commit_tx\" | \"rollback_tx\" | \"open_store\" | \"scan\" | \"select\" | \"filter\" | \"sort\" | \"project\" | \"limit\" | \"join\" | \"join_right\" | \"update\" | \"delete\" | \"inspect\" | \"defer\" | \"assign\" | \"if\" | \"loop\" | \"call_script\" | \"script\" | \"call_function\" | \"list_new\" | \"list_append\" | \"map_merge\" | \"first\" | \"last\" | \"next\" | \"previous\" | \"find\" | \"add\" | \"get_current_key\" | \"get_current_value\" | \"return\""
	a.registry.RegisterWithUI("execute_script", "Executes a multi-step programmatic script for advanced queries.", a.getToolInstruction(ctx, "execute_script", ExecuteScriptInstruction), "(script: Array<{op: "+ops+", args?: object, input_var?: string, result_var?: string}>)", a.toolExecuteScript)

	// Conversation Management
	a.registry.Register("conclude_topic", "Conclusion of the current conversation thread. Use this when the user is satisfied, a resolution is reached, or to summarize before moving to a new topic. This saves the summary to memory and cleans up the context.", "(summary: string, topic_label: string)", a.toolConcludeTopic)

	// Communication Tools
	a.registry.Register("send_email", "Sends an email.", "(to: string, subject: string, body: string)", a.toolSendEmail)

	// Register Atomic Operations (Internal/Granular)
	// a.registerAtomicTools()
}
func (a *CopilotAgent) getToolInstruction(ctx context.Context, toolName, defaultInst string) string {
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
func (a *CopilotAgent) toolConcludeTopic(ctx context.Context, args map[string]interface{}) (string, error) {
	return "Topic concluded.", nil
}
func (a *CopilotAgent) getSystemInstructions(ctx context.Context, defaultInst string) string {
	if a.systemDB == nil {
		return defaultInst
	}

	// Start a read-only transaction
	tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return defaultInst
	}
	defer tx.Rollback(ctx)

	// Open the Knowledge Config Store to check what categories we should load
	// We use a simple String Key (Category Name) -> String Value (Priority/Metadata) store?
	// Or we can just reuse the generic BTree logic if we have a simple store wrapper.
	// We'll trust that the store exists. If not, we skip dynamic loading.

	// 1. Base Prompt (Static or from 'memory/system_prompt')
	baseInst := defaultInst
	store, err := core.OpenBtree[KnowledgeKey, string](ctx, a.systemDB.Options(), KnowledgeStore, tx, nil)
	if err == nil {
		found, err := store.Find(ctx, KnowledgeKey{Category: "memory", Name: "system_prompt"}, false)
		if err == nil && found {
			if val, err := store.GetCurrentValue(ctx); err == nil {
				baseInst = val
			}
		}
	} else {
		// If main store fails, just return default
		return defaultInst
	}

	// 2. Load MRU / Active Knowledge
	// We open the MRU store to see what "Categories" or "Items" are marked as active.
	// MRU Store: Key = Category (string), Value = Timestamp (string/int)
	// For now, we assume simple string->string map where Key is the Category to load.

	categoriesToLoad := make(map[string]bool)
	// Always load core categories
	categoriesToLoad["data_generation"] = true
	categoriesToLoad["store_rules"] = true
	categoriesToLoad["policy"] = true
	categoriesToLoad["recipes"] = true

	mruStore, err := core.OpenBtree[string, string](ctx, a.systemDB.Options(), MRUKnowledgeStore, tx, nil)
	if err == nil {
		// Iterate all keys in MRU store
		if ok, err := mruStore.First(ctx); ok && err == nil {
			for {
				cat := mruStore.GetCurrentKey().Key
				categoriesToLoad[cat] = true
				if ok, err := mruStore.Next(ctx); !ok || err != nil {
					break
				}
			}
		}
	}

	var sb strings.Builder
	sb.WriteString(baseInst)
	sb.WriteString("\n\n### Loaded Knowledge:\n")

	// Convert map to sorted slice for consistent prompt caching
	var sortedCats []string
	for c := range categoriesToLoad {
		sortedCats = append(sortedCats, c)
	}
	sort.Strings(sortedCats)

	for _, cat := range sortedCats {
		// Use Range API
		// We want all keys where Category == cat.
		// B-Tree is ordered by Category then Name.
		// So we seek to {Category: cat, Name: ""} and iterate until Category changes.

		startKey := KnowledgeKey{Category: cat, Name: ""}

		if found, err := store.Find(ctx, startKey, true); err == nil && found {
			// Iterate
			for {
				// GetCurrentKey returns just the Item (with Key inside), no error in signature
				item := store.GetCurrentKey()
				k := item.Key

				if k.Category != cat {
					break // Done with this category
				}

				val, err := store.GetCurrentValue(ctx)
				if err == nil {
					sb.WriteString(fmt.Sprintf("- [%s] %s: %s\n", cat, k.Name, val))
				}

				if ok, err := store.Next(ctx); err != nil || !ok {
					break
				}
			}
		}
	}

	return sb.String()
}

func (a *CopilotAgent) toolListTools(ctx context.Context, args map[string]any) (string, error) {
	tools := a.registry.List()

	// Sort tools by name for consistent output
	sort.Slice(tools, func(i, j int) bool {
		return tools[i].Name < tools[j].Name
	})

	var sb strings.Builder
	// Note: Header is handled by the caller or omitted to allow clean concatenation
	// But since this tool can be called independently by the agent, we should perhaps skip heavy headers if it's meant to be a simple list.
	// However, the prompt says "it should display complete set". I'll just list them.

	for _, t := range tools {
		if t.Hidden {
			continue
		}

		// Use ShortDescription if available for UI brevity, otherwise fallback and clean Description
		var desc string
		if t.ShortDescription != "" {
			desc = t.ShortDescription
		} else {
			desc = strings.ReplaceAll(t.Description, "\n", " ")
			// Limit to the first sentence to keep the UI clean (hide heavy LLM instructions)
			if idx := strings.Index(desc, "."); idx > 0 {
				desc = desc[:idx+1]
			}
			if len(desc) > 150 {
				desc = desc[:147] + "..."
			}
		}

		// Simplify ArgsSchema for CLI display
		// Convert "(arg1: type, arg2: type)" -> "arg1, arg2"
		argsSchema := t.ArgsSchema
		argsSchema = strings.TrimPrefix(argsSchema, "(")
		argsSchema = strings.TrimSuffix(argsSchema, ")")

		// Remove types (primitive heuristic)
		var simpleArgs []string
		if len(argsSchema) > 0 {
			parts := strings.Split(argsSchema, ",")
			for _, p := range parts {
				// Get arg name
				argName := strings.Split(p, ":")[0]
				argName = strings.TrimSpace(argName)
				if argName != "" {
					simpleArgs = append(simpleArgs, "<"+argName+">")
				}
			}
		}

		prettyArgs := strings.Join(simpleArgs, " ")

		cmdStr := fmt.Sprintf("/%s", t.Name)
		if prettyArgs != "" {
			cmdStr += " " + prettyArgs
		}

		sb.WriteString(fmt.Sprintf("- `%s`: %s\n", cmdStr, desc))
	}

	return sb.String(), nil
}

func (a *CopilotAgent) toolListDatabases(ctx context.Context, args map[string]any) (string, error) {
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

func (a *CopilotAgent) toolListStores(ctx context.Context, args map[string]any) (string, error) {
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
		if strings.Contains(sName, "/") {
			continue
		}
		desc := sName
		if hasOpts {
			// Peek for schema to guide the LLM
			// Use a sub-scope or just open. jsondb.OpenStore is idempotent-ish for the same tx.
			// Note: We ignore errors here because listing stores should succeed even if inspection fails.
			s, err := jsondb.OpenStore(ctx, dbOpts, sName, tx)
			if err == nil {
				info := s.GetStoreInfo()
				var extras string
				if info.Description != "" {
					extras += fmt.Sprintf(" description=\"%s\"", info.Description)
				}
				if len(info.Relations) > 0 {
					rels, _ := json.Marshal(info.Relations)
					extras += fmt.Sprintf(" relations=%s", string(rels))
				}

				if ok, _ := s.First(ctx); ok {
					k := s.GetCurrentKey()
					v, _ := s.GetCurrentValue(ctx)
					flat := flattenItem(k, v)
					schema := inferSchema(flat)
					desc = fmt.Sprintf("%s schema=%s%s", sName, formatSchema(schema), extras)
				} else {
					desc = fmt.Sprintf("%s (empty store)%s", sName, extras)
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

func (a *CopilotAgent) toolSwitchDatabase(ctx context.Context, args map[string]any) (string, error) {
	dbName, _ := args["database"].(string)
	if dbName == "" {
		return "", fmt.Errorf("argument 'database' is required")
	}

	exists := false
	if dbName == "system" && a.systemDB != nil {
		exists = true
	} else {
		_, exists = a.databases[dbName]
	}

	if !exists {
		var names []string
		for k := range a.databases {
			names = append(names, k)
		}
		if a.systemDB != nil {
			names = append(names, "system")
		}
		sort.Strings(names)
		return "", fmt.Errorf("database '%s' not found. Available: %v", dbName, names)
	}

	if p := ai.GetSessionPayload(ctx); p != nil {
		p.CurrentDB = dbName
		p.Transaction = nil
	}

	return fmt.Sprintf("Active database context switched to '%s'.", dbName), nil
}
