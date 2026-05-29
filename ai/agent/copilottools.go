package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/jsondb"
)

const (
	ExecuteScriptInstruction = `Execute a full ordered JSON AST under script for multi-step store operations. Each step should be an object such as {op, args?, input_var?, result_var?}. Focus on orchestration semantics: begin a transaction, read or mutate stores, then commit or rollback. Chain multi-step reads with result_var/input_var. Use list_stores to research stores before multi-store joins or whenever schema is uncertain. Prefer scoped calls such as stores:["users","users_orders","orders"] so research stays compact on large databases. list_stores returns grounded per-store lines with schema=... and optional relations=[...]. Read schema=... for exact field names and value types, and read relations=[...] for related-store and join-field semantics. Treat those returned relations=[...] entries as the source of truth. When list_stores confirms a relation path, prefer relation + target for join repair instead of inventing a fresh on mapping; if on is still needed, rewrite only the invalid join slice with confirmed concrete field strings. join and join_right emit a combined flat record by default, so reuse dotted store-qualified field paths unless a later project step reshapes the output. If the AST shape is ambiguous, call gettoolinfo('execute_script'). Use concrete predicate objects and concrete join mappings; do not guess missing values.`
	ListStoresInstruction    = "Research store structure before writing multi-store reads or repairs. Pass stores:[...] to scope the response to likely targets. The result returns grounded schema=... and optional relations=[...] lines; reuse those returned relations as the source of truth for join mappings and field paths rather than guessing them."
)

const emptyObjectArgsSchema = `{"type":"object","properties":{}}`

const listStoresArgsSchema = `{"type":"object","properties":{"database":{"type":"string","description":"Optional database override. Defaults to the active session database."},"stores":{"type":"array","description":"Optional exact store names to research. Use likely target names such as [\"users\",\"users_orders\",\"orders\"] to keep research compact instead of listing the whole database.","items":{"type":"string"}}}}`

const (
	SelectInstruction                   = "Read or mutate one store directly when you do not need a multi-step AST. Provide the store name plus optional key/value criteria, fields, limit, and direction. For mutations set action=delete or action=update and include grounded update_values instead of placeholder objects. This tool still executes inside a transaction: it reuses an explicit transaction when one is active, otherwise it opens and auto-commits its own local transaction. Prefer execute_script when you need multi-step transaction orchestration."
	JoinInstruction                     = "Join two stores directly when the join fields are already grounded. Provide left_store, right_store, aligned join field arrays, and optional fields/limit/direction. Prefer execute_script plus list_stores first when join mappings or field paths are still uncertain."
	ExplainJoinInstruction              = "Preview how a join will execute before running it. Provide the target right_store and a grounded on mapping to see whether the engine can use an index scan or will fall back to a full scan. This is a single read-oriented operation and will use a local read transaction when no explicit transaction is active. Use this after list_stores research when join-key selection or performance is still uncertain."
	AddInstruction                      = "Insert one record into a store by providing store, key, and value. Use this for single-record writes; use execute_script when the write must be part of a larger transaction or multi-step flow. This tool reuses an explicit transaction when one is active, otherwise it opens and auto-commits its own local write transaction."
	UpdateInstruction                   = "Replace or update one record in a store by key. Provide the exact store, key, and value payload. Use execute_script when the update depends on prior reads or must participate in a broader transaction. This tool reuses an explicit transaction when one is active, otherwise it opens and auto-commits its own local write transaction."
	DeleteInstruction                   = "Delete one record from a store by exact key. Use execute_script when deletion depends on researched predicates, joins, or transaction orchestration rather than a single known key. This tool reuses an explicit transaction when one is active, otherwise it opens and auto-commits its own local write transaction."
	ManageTransactionInstruction        = "Control a transaction directly with action=begin, commit, or rollback. Use this only for explicit transaction control outside execute_script; for multi-step read/write orchestration, prefer execute_script and keep begin_tx/commit_tx/rollback_tx inside the AST."
	MintToSpaceInstruction              = "Store durable generated or discovered knowledge in a Space for future retrieval. Provide the exact kb_name the user asked for plus the content to persist; optional category groups related entries. Use this for facts, notes, solutions, or generated content that should persist beyond the current chat. Do not replace it with an external import workflow, and do not wrap it in begin_tx or commit_tx because mint_to_space manages its own transaction."
	DeleteSpaceInstruction              = "Delete an entire Space and all of its stored knowledge. Use only when the user explicitly wants the whole knowledge base removed, not when they only want to change content or configuration. Provide the exact kb_name to remove, and do not wrap delete_space in begin_tx or commit_tx because it runs through its own deletion path."
	EnrichSpaceInstruction              = "Run the Space enrichment pipeline so stored items can be normalized, linked, or expanded by the knowledge workflow. Use this after meaningful content changes only when the user explicitly wants derived knowledge refreshed or enrichment rerun; it is not the default follow-up to every mint or config change."
	UpdateSpaceConfigInstruction        = "Change Space-level configuration such as routing, system prompts, persona behavior, or enabled tool access. Provide the exact kb_name and a grounded config object with the intended settings. Read the current config first when you need to inspect or preserve existing values instead of guessing a partial patch from natural language alone."
	ReadSpaceConfigInstruction          = "Read the current configuration for a Space before changing it or when the user asks how the Space behaves. Use this to inspect routing rules, system prompts, persona settings, and enabled tool access for the target kb_name so later updates stay grounded."
	VectorizeSpaceInstruction           = "Generate or refresh embeddings for every eligible item in a Space. Use this only when the user explicitly asks for vectorization, embeddings, semantic refresh, or full reindexing of the whole knowledge base; do not call it automatically after normal content writes."
	VectorizeSpaceCategoriesInstruction = "Generate or refresh embeddings for specific categories within a Space. Provide kb_name and categories when the user wants semantic refresh for selected sections instead of the whole Space, and prefer this over full-space vectorization when the request is narrower."
	VectorizeSpaceItemsInstruction      = "Generate or refresh embeddings for specific items inside a Space category. Provide kb_name, category, and item_names when the refresh should stay tightly scoped to known changed items, and prefer this over category-wide or full-space vectorization when possible."
)

// registerSystemTools registers the core system inspection tools.
func (a *CopilotAgent) registerSystemTools(ctx context.Context) {
	a.registry.Register("list_databases", "Lists all available databases.", emptyObjectArgsSchema, a.toolListDatabases)
	a.registry.Register("list_stores", ListStoresInstruction, listStoresArgsSchema, a.toolListStores)
	a.registry.Register("list_tools", "Lists all available tools and their usage instructions.", emptyObjectArgsSchema, a.toolListTools)
}

// registerTools registers all available tools for the CopilotAgent.
func (a *CopilotAgent) registerTools(ctx context.Context) {
	log.Debug("CopilotAgent.registerTools: Starting registration...")
	if a.registry == nil {
		a.registry = NewRegistry()
	}

	a.registerSystemTools(ctx)
	a.registerStoresTools(ctx)
	a.registerSpaceTools(ctx)
	a.registerScriptTools(ctx)
	a.registerRoutingTools(ctx)
	a.registerAutomationTools(ctx)

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
		argsSchema := t.ArgsSchema
		var simpleArgs []string
		if strings.HasPrefix(strings.TrimSpace(argsSchema), "{") {
			var schema map[string]any
			if err := json.Unmarshal([]byte(argsSchema), &schema); err == nil {
				if props, ok := schema["properties"].(map[string]any); ok {
					names := make([]string, 0, len(props))
					for name := range props {
						names = append(names, name)
					}
					sort.Strings(names)
					for _, name := range names {
						simpleArgs = append(simpleArgs, "<"+name+">")
					}
				}
			}
		} else {
			argsSchema = strings.TrimPrefix(argsSchema, "(")
			argsSchema = strings.TrimSuffix(argsSchema, ")")
			if len(argsSchema) > 0 {
				parts := strings.Split(argsSchema, ",")
				for _, p := range parts {
					argName := strings.Split(p, ":")[0]
					argName = strings.TrimSpace(argName)
					if argName != "" {
						simpleArgs = append(simpleArgs, "<"+argName+">")
					}
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

	requestedStores := make(map[string]struct{})
	if rawStores, ok := args["stores"].([]any); ok {
		for _, raw := range rawStores {
			if name, ok := raw.(string); ok && strings.TrimSpace(name) != "" {
				requestedStores[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
			}
		}
	} else if rawStores, ok := args["stores"].([]string); ok {
		for _, name := range rawStores {
			if strings.TrimSpace(name) != "" {
				requestedStores[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
			}
		}
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
		if len(requestedStores) > 0 {
			if _, ok := requestedStores[strings.ToLower(strings.TrimSpace(sName))]; !ok {
				continue
			}
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
	if len(requestedStores) > 0 && len(descriptions) == 0 {
		return "", fmt.Errorf("requested stores not found in database '%s'", dbName)
	}

	if autoCommit {
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("failed to commit transaction: %w", err)
		}
	}
	result := fmt.Sprintf("Stores:\n%s", strings.Join(descriptions, "\n"))
	if nativeToolHintsEnabled(ctx) {
		return wrapToolResultWithListStoresHint(result, descriptions), nil
	}
	return result, nil
}

func wrapToolResultWithListStoresHint(result string, descriptions []string) string {
	hint := &ai.ToolProgressHint{
		Status:             "progressing",
		CompletionDelta:    0.25,
		Tips:               []string{"Reuse the exact store names, grounded schema fields, and relations from list_stores in the next tool call."},
		SuggestedNextTools: []string{"execute_script"},
	}
	for _, desc := range descriptions {
		trimmed := strings.TrimSpace(desc)
		if trimmed == "" {
			continue
		}
		hint.Clues = append(hint.Clues, "Grounded store info: "+trimmed)
		if len(hint.Clues) == 3 {
			break
		}
	}
	return wrapNativeToolResultEnvelope(result, hint)
}

func nativeToolHintsEnabled(ctx context.Context) bool {
	return ctx.Value(ai.CtxKeyNativeToolHints) == true
}

func wrapNativeTerminalToolResult(ctx context.Context, result string, status string, tips ...string) string {
	if !nativeToolHintsEnabled(ctx) {
		return result
	}
	return wrapNativeToolResultEnvelope(result, &ai.ToolProgressHint{
		Status: strings.TrimSpace(status),
		Tips:   append([]string(nil), tips...),
	})
}

func wrapNativeToolResultEnvelope(result string, hint *ai.ToolProgressHint) string {
	envelope := ai.ToolResultEnvelope{
		ToolResult:   json.RawMessage(strconv.Quote(result)),
		ProgressHint: hint,
	}
	bytes, err := json.Marshal(envelope)
	if err != nil {
		return result
	}
	return string(bytes)
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

// toolConcludeTopic is a placeholder. The actual logic requires Session access and is handled/overridden in Service.
func (a *CopilotAgent) toolConcludeTopic(ctx context.Context, args map[string]interface{}) (string, error) {
	return "Topic concluded.", nil
}
