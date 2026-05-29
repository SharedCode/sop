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

const ExecuteScriptInstruction = `Execute a JSON AST script for multi-step store operations. Focus on orchestration semantics: begin a transaction, read or mutate stores, then commit or rollback. Chain multi-step reads with result_var/input_var. If schema or join mapping is ambiguous, call list_stores first and treat relations=[...] as the source of truth. If the AST shape is ambiguous, call gettoolinfo('execute_script'). Use concrete predicate objects and concrete join mappings; do not guess missing values.`

const (
	SelectInstruction = "Selects data from a store. See SOP KB for instructions."

	JoinInstruction = "Joins data from two stores. See SOP KB for instructions."

	AddInstruction               = "Adds data to a store. See SOP KB for instructions."
	UpdateInstruction            = "Updates data in a store. See SOP KB for instructions."
	DeleteInstruction            = "Deletes data from a store by key. See SOP KB for instructions."
	ManageTransactionInstruction = "Manages transactions (begin, commit, rollback). See SOP KB for instructions."
)

// registerSystemTools registers the core system inspection tools.
func (a *CopilotAgent) registerSystemTools(ctx context.Context) {
	a.registry.Register("list_databases", "Lists all available databases.", "()", a.toolListDatabases)
	a.registry.Register("list_stores", "Lists all stores in the current or specified database, optionally filtered to specific store names.", "(database?: string, stores?: Array<string>)", a.toolListStores)
	a.registry.Register("list_tools", "Lists all available tools and their usage instructions.", "()", a.toolListTools)
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
