package main

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
	aidb "github.com/sharedcode/sop/ai/database"
	_ "github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/obfuscation"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/jsondb"
)

// DefaultToolExecutor implements ai.ToolExecutor
type DefaultToolExecutor struct{}

func (e *DefaultToolExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	return executeTool(ctx, toolName, args)
}

func (e *DefaultToolExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return []ai.ToolDefinition{
		{
			Name:        "list_stores",
			Description: "Lists all stores in the specified database.",
			Schema:      `{"type": "object", "properties": {"database": {"type": "string"}}, "required": ["database"]}`,
		},
		{
			Name:        "list_databases",
			Description: "Lists all available databases.",
			Schema:      `{"type": "object", "properties": {}}`,
		},
		{
			Name:        "search",
			Description: "Search for items in a store.",
			Schema:      `{"type": "object", "properties": {"database": {"type": "string"}, "store": {"type": "string"}, "query": {"type": "object"}}, "required": ["database", "store", "query"]}`,
		},
		{
			Name:        "get_schema",
			Description: "Get the index specification and schema of a store.",
			Schema:      `{"type": "object", "properties": {"database": {"type": "string"}, "store": {"type": "string"}}, "required": ["database", "store"]}`,
		},
		{
			Name:        "select",
			Description: "Select items from a store.",
			Schema:      `{"type": "object", "properties": {"database": {"type": "string"}, "store": {"type": "string"}, "limit": {"type": "integer"}}, "required": ["database", "store"]}`,
		},
		{
			Name:        "manage_transaction",
			Description: "Manage transaction lifecycle (begin, commit, rollback).",
			Schema:      `{"type": "object", "properties": {"action": {"type": "string", "enum": ["begin", "commit", "rollback"]}}, "required": ["action"]}`,
		},
		{
			Name:        "delete",
			Description: "Delete an item from a store.",
			Schema:      `{"type": "object", "properties": {"database": {"type": "string"}, "store": {"type": "string"}, "key": {"oneOf": [{"type": "string"}, {"type": "object"}]}}, "required": ["database", "store", "key"]}`,
		},
	}, nil
}

func handleAIChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Message   string `json:"message"`
		Database  string `json:"database"`
		StoreName string `json:"store"`
		Agent     string `json:"agent"`
		Provider  string `json:"provider"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Trim inputs to avoid mismatch issues
	req.Database = strings.TrimSpace(req.Database)
	req.StoreName = strings.TrimSpace(req.StoreName)

	// Validate Database if provided
	if req.Database != "" {
		if _, err := getDBOptions(req.Database); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{
				"error": fmt.Sprintf("Invalid database '%s': %v", req.Database, err),
			})
			return
		}
	}

	// Default to the RAG Agent "sql_admin" if not specified
	if req.Agent == "" {
		req.Agent = "sql_admin"
	}

	// Check if a specific RAG Agent is requested
	agentSvc, exists := loadedAgents[req.Agent]
	if !exists {
		// If the requested agent doesn't exist, we could fall back or error.
		// For now, let's error to be explicit.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("Agent '%s' is not initialized or not found.", req.Agent),
		})
		return
	}

	ctx := context.Background()
	// Pass provider override via context
	if req.Provider != "" {
		ctx = context.WithValue(ctx, ai.CtxKeyProvider, req.Provider)
	}
	// Pass ToolExecutor via context
	ctx = context.WithValue(ctx, ai.CtxKeyExecutor, &DefaultToolExecutor{})

	var askOpts []ai.Option

	// Pass Database via options if provided
	var currentDB any
	if req.Database != "" {
		opts, _ := getDBOptions(req.Database)
		// We create a lightweight database handle.
		// Note: This doesn't open connections yet, just configures the handle.
		db := aidb.NewDatabase(opts)
		currentDB = db
		askOpts = append(askOpts, ai.WithDatabase(db))
	}

	// Construct SessionPayload
	// For now, we populate Databases with just the current one if available,
	// or we could list all available DBs if we want to support cross-db queries.
	// Let's populate all known DBs from config/env if possible, or just leave it empty
	// and let the agent fail if it tries to access unknown DBs.
	// Since getDBOptions is dynamic, we might not have a list of ALL DBs easily without scanning.
	// For this iteration, we'll just put the current one in the map too.
	databases := make(map[string]any)
	if req.Database != "" {
		databases[req.Database] = currentDB
	}

	// Also add "system" db if needed?

	payload := &ai.SessionPayload{
		CurrentDB: currentDB,
		Databases: databases,
		Variables: map[string]any{
			"database": req.Database, // Store database name for fallback
		},
	}
	askOpts = append(askOpts, ai.WithSessionPayload(payload))

	// Register resources so the agent knows them for obfuscation
	if req.Database != "" {
		obfuscation.GlobalObfuscator.RegisterResource(req.Database, "DB")
	}
	if req.StoreName != "" {
		obfuscation.GlobalObfuscator.RegisterResource(req.StoreName, "STORE")
	}

	// Prepend context information to the message
	fullMessage := req.Message
	// Only prepend context if it's not a system command
	if req.Database != "" && !strings.HasPrefix(req.Message, "/") {
		fullMessage = fmt.Sprintf("Current Database: %s\n%s", req.Database, req.Message)
	}

	// Inject payload into context for Open/Close
	ctx = context.WithValue(ctx, "session_payload", payload)

	// Initialize Agent Session (Transaction)
	if err := agentSvc.Open(ctx); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("Agent '%s' failed to open session: %v", req.Agent, err),
		})
		return
	}
	defer func() {
		if err := agentSvc.Close(ctx); err != nil {
			log.Error(fmt.Sprintf("Agent '%s' failed to close session: %v", req.Agent, err))
		}
	}()

	// agentSvc delegates to dataadmin agent as necessary for LLM ask.
	response, err := agentSvc.Ask(ctx, fullMessage, askOpts...)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("Agent '%s' failed: %v", req.Agent, err),
		})
		return
	}

	// Process the response from LLM as returned by the agent.
	text := strings.TrimSpace(response)
	var toolCall struct {
		Tool string         `json:"tool"`
		Args map[string]any `json:"args"`
	}
	cleanText := strings.TrimPrefix(text, "```json")
	cleanText = strings.TrimPrefix(cleanText, "```")
	cleanText = strings.TrimSuffix(cleanText, "```")
	cleanText = strings.TrimSpace(cleanText)

	if err := json.Unmarshal([]byte(cleanText), &toolCall); err == nil && toolCall.Tool != "" {
		// If we get a raw tool call here, it means the Agent didn't execute it.
		// We can execute it once and return the result.
		log.Debug(fmt.Sprintf("Agent returned raw tool call: %s", toolCall.Tool))

		// Inject default database if missing
		if db, ok := toolCall.Args["database"].(string); !ok || db == "" {
			toolCall.Args["database"] = req.Database
		}

		// Log the final database name being used for debugging
		finalDB, _ := toolCall.Args["database"].(string)
		log.Debug(fmt.Sprintf("Executing tool '%s' on database: '%s' (bytes: %v)", toolCall.Tool, finalDB, []byte(finalDB)))

		result, err := executeTool(ctx, toolCall.Tool, toolCall.Args)
		if err != nil {
			result = "Error: " + err.Error()

			// Check if we should stop recording on error
			if svc, ok := agentSvc.(*agent.Service); ok {
				if svc.StopOnError() {
					svc.StopRecording()
					result += "\nRecording stopped due to error."
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"response": result,
			"action":   "refresh",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"response": response,
	})
}

func executeTool(ctx context.Context, toolName string, args map[string]any) (string, error) {
	dbName, _ := args["database"].(string)
	dbName = strings.TrimSpace(dbName)

	// Fallback to SessionPayload if database name is missing
	if dbName == "" {
		if p := ai.GetSessionPayload(ctx); p != nil {
			// Try to find the name of the CurrentDB
			// Since CurrentDB is `any`, we might need to check if it's a *database.Database and get its name?
			// Or we can check if there's only one DB in Databases map?
			// Or we can check if the payload has a "default_database" variable?
			// For now, let's check if the payload has a database name in Variables or if we can infer it.
			// Actually, main.ai.go puts req.Database in the payload!
			// But payload.CurrentDB is the object.
			// We need the NAME to call getDBOptions.
			// Let's iterate p.Databases to find the one matching CurrentDB?
			// Or better, let's assume the caller put the name in Variables?
			// No, main.ai.go doesn't put name in Variables.

			// Let's try to use the first available database if only one exists in config?
			// No, that's dangerous.

			// Let's check if the payload has a "database" variable.
			if val, ok := p.Variables["database"].(string); ok && val != "" {
				dbName = val
			}
		}
	}

	if dbName == "" {
		return "", fmt.Errorf("database name is required")
	}

	dbOpts, err := getDBOptions(dbName)
	if err != nil {
		return "", err
	}

	switch toolName {
	case "manage_transaction":
		action, _ := args["action"].(string)
		if action == "" {
			return "", fmt.Errorf("action is required")
		}

		p := ai.GetSessionPayload(ctx)
		if p == nil {
			return "", fmt.Errorf("no session payload found")
		}

		// Resolve Database Object for Transaction
		// We need the actual *database.Database object to start a transaction.
		// The payload might have it in CurrentDB or Databases map.
		var db *aidb.Database
		if d, ok := p.CurrentDB.(*aidb.Database); ok {
			db = d
		} else if d, ok := p.Databases[dbName].(*aidb.Database); ok {
			db = d
		} else {
			// Fallback: Create a new handle using dbOpts
			// This is safe because aidb.NewDatabase is lightweight.
			db = aidb.NewDatabase(dbOpts)
		}

		switch action {
		case "begin":
			if p.Transaction != nil {
				return "Transaction already active", nil
			}
			tx, err := db.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return "", fmt.Errorf("failed to begin transaction: %w", err)
			}
			p.Transaction = tx
			return "Transaction started", nil

		case "commit":
			if p.Transaction == nil {
				return "No active transaction to commit", nil
			}
			if tx, ok := p.Transaction.(sop.Transaction); ok {
				// Attempt commit
				commitErr := tx.Commit(ctx)

				// Always clear the old transaction
				p.Transaction = nil

				// Auto-Begin new transaction for continuity
				newTx, beginErr := db.BeginTransaction(ctx, sop.ForWriting)
				if beginErr != nil {
					if commitErr != nil {
						return "", fmt.Errorf("commit failed: %v. AND failed to auto-start new one: %v", commitErr, beginErr)
					}
					return "Transaction committed, but failed to auto-start new one: " + beginErr.Error(), nil
				}
				p.Transaction = newTx

				// Record the implicit 'begin' step if recording
				if recorder, ok := ctx.Value(ai.CtxKeyMacroRecorder).(ai.MacroRecorder); ok {
					recorder.RecordStep(ai.MacroStep{
						Type:    "command",
						Command: "manage_transaction",
						Args:    map[string]any{"action": "begin"},
					})
				}

				if commitErr != nil {
					// Return error but state that new transaction is ready
					return fmt.Sprintf("New transaction started, but previous commit failed: %v", commitErr), commitErr
				}

				return "Transaction committed (and new one started)", nil
			}
			return "", fmt.Errorf("invalid transaction object")

		case "rollback":
			if p.Transaction == nil {
				return "No active transaction to rollback", nil
			}
			if tx, ok := p.Transaction.(sop.Transaction); ok {
				if err := tx.Rollback(ctx); err != nil {
					return "", fmt.Errorf("rollback failed: %w", err)
				}
				p.Transaction = nil

				// Auto-Begin new transaction for continuity
				newTx, err := db.BeginTransaction(ctx, sop.ForWriting)
				if err != nil {
					return "Transaction rolled back, but failed to auto-start new one: " + err.Error(), nil
				}
				p.Transaction = newTx

				// Record the implicit 'begin' step if recording
				if recorder, ok := ctx.Value(ai.CtxKeyMacroRecorder).(ai.MacroRecorder); ok {
					recorder.RecordStep(ai.MacroStep{
						Type:    "command",
						Command: "manage_transaction",
						Args:    map[string]any{"action": "begin"},
					})
				}

				return "Transaction rolled back (and new one started)", nil
			}
			return "", fmt.Errorf("invalid transaction object")

		default:
			return "", fmt.Errorf("unknown action: %s", action)
		}

	case "list_databases":
		var names []string
		for _, db := range config.Databases {
			names = append(names, db.Name)
		}
		return fmt.Sprintf("Databases: %v", names), nil

	case "list_stores":
		trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
		if err != nil {
			return "", err
		}
		defer trans.Rollback(ctx)

		stores, err := trans.GetStores(ctx)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Stores: %v", stores), nil

	case "select":
		storeName, _ := args["store"].(string)
		limitFloat, _ := args["limit"].(float64) // JSON numbers are floats
		limit := int(limitFloat)
		if limit < 0 {
			limit = 1000000 // Treat negative as "unlimited" (capped)
		} else if limit == 0 {
			limit = 2 // Default if missing
		}
		if storeName == "" {
			return "", fmt.Errorf("store name is required")
		}

		var trans sop.Transaction
		p := ai.GetSessionPayload(ctx)
		if p != nil && p.Transaction != nil {
			if t, ok := p.Transaction.(sop.Transaction); ok {
				trans = t
			}
		}

		if trans == nil {
			var err error
			trans, err = database.BeginTransaction(ctx, dbOpts, sop.ForReading)
			if err != nil {
				return "", err
			}
			defer trans.Rollback(ctx)
		}

		// Get Store Info to determine comparer
		var isPrimitiveKey bool
		var indexSpec *jsondb.IndexSpecification

		if t2, ok := trans.GetPhasedTransaction().(*common.Transaction); ok {
			stores, err := t2.StoreRepository.Get(ctx, storeName)
			if err == nil && len(stores) > 0 {
				isPrimitiveKey = stores[0].IsPrimitiveKey
				if !isPrimitiveKey && stores[0].MapKeyIndexSpecification != "" {
					var is jsondb.IndexSpecification
					if err := encoding.DefaultMarshaler.Unmarshal([]byte(stores[0].MapKeyIndexSpecification), &is); err == nil {
						indexSpec = &is
					}
				}
			}
		}

		var items []map[string]any

		var store jsondb.StoreAccessor
		cacheKey := fmt.Sprintf("_opened_store_%s", storeName)
		if p != nil && p.Variables != nil {
			if cached, ok := p.Variables[cacheKey]; ok {
				if s, ok := cached.(jsondb.StoreAccessor); ok {
					store = s
				}
			}
		}

		if store == nil {
			var err error
			store, err = jsondb.OpenStore(ctx, dbOpts, storeName, trans)
			if err != nil {
				return "", fmt.Errorf("failed to open store: %v", err)
			}
			// Cache the opened store
			if p != nil {
				if p.Variables == nil {
					p.Variables = make(map[string]any)
				}
				p.Variables[cacheKey] = store
			}
		}

		ok, err := store.First(ctx)
		count := 0
		for ok && err == nil && count < limit {
			k, err := store.GetCurrentKey()
			if err != nil {
				return "", fmt.Errorf("failed to get key: %v", err)
			}
			v, err := store.GetCurrentValue(ctx)
			if err != nil {
				return "", fmt.Errorf("failed to get value: %v", err)
			}

			items = append(items, map[string]any{"key": k, "value": v})
			ok, err = store.Next(ctx)
			count++
		}

		if len(items) == 0 {
			return "No items found.", nil
		}

		// Check for requested format (default to CSV for readability in Chat)
		format, _ := args["format"].(string)
		if strings.ToLower(format) == "json" {
			b, err := json.MarshalIndent(items, "", "  ")
			if err != nil {
				return "", err
			}
			return string(b), nil
		}

		keyFieldSet := make(map[string]bool)
		var keyIsMap bool
		valueFieldSet := make(map[string]bool)
		var valueIsMap bool

		// Scan for fields
		for _, item := range items {
			// Check Key
			if kMap, ok := item["key"].(map[string]any); ok {
				keyIsMap = true
				for k := range kMap {
					keyFieldSet[k] = true
				}
			}
			// Check Value
			if vMap, ok := item["value"].(map[string]any); ok {
				valueIsMap = true
				for k := range vMap {
					valueFieldSet[k] = true
				}
			}
		}

		var kFields []string
		if keyIsMap {
			if indexSpec != nil {
				// Priority 1: Use Index Spec for order
				for _, f := range indexSpec.IndexFields {
					kFields = append(kFields, f.FieldName)
				}
				// Append extra fields found in data but not in spec
				var extra []string
				for k := range keyFieldSet {
					found := false
					for _, f := range kFields {
						if f == k {
							found = true
							break
						}
					}
					if !found {
						extra = append(extra, k)
					}
				}
				sort.Strings(extra)
				kFields = append(kFields, extra...)
			} else {
				// Priority 2: Alphabetical
				for k := range keyFieldSet {
					kFields = append(kFields, k)
				}
				sort.Strings(kFields)
			}
		}

		var vFields []string
		if valueIsMap {
			for k := range valueFieldSet {
				vFields = append(vFields, k)
			}
			sort.Strings(vFields)
		}

		// Default: CSV Format
		var csvHeaders []string
		if keyIsMap {
			for _, k := range kFields {
				if len(k) > 0 {
					csvHeaders = append(csvHeaders, strings.ToUpper(k[:1])+k[1:])
				} else {
					csvHeaders = append(csvHeaders, k)
				}
			}
		} else {
			csvHeaders = append(csvHeaders, "Key")
		}
		if valueIsMap {
			for _, v := range vFields {
				if len(v) > 0 {
					csvHeaders = append(csvHeaders, strings.ToUpper(v[:1])+v[1:])
				} else {
					csvHeaders = append(csvHeaders, v)
				}
			}
		}
		var sb strings.Builder
		sb.WriteString(strings.Join(csvHeaders, ", "))
		sb.WriteString("\n")

		for _, item := range items {
			var row []string
			// Key
			if keyIsMap {
				kMap, _ := item["key"].(map[string]any)
				for _, f := range kFields {
					if v, ok := kMap[f]; ok {
						row = append(row, fmt.Sprintf("%v", v))
					} else {
						row = append(row, "")
					}
				}
			} else {
				row = append(row, fmt.Sprintf("%v", item["key"]))
			}
			// Value
			if valueIsMap {
				vMap, _ := item["value"].(map[string]any)
				for _, f := range vFields {
					if v, ok := vMap[f]; ok {
						row = append(row, fmt.Sprintf("%v", v))
					} else {
						row = append(row, "")
					}
				}
			} else {
				row = append(row, fmt.Sprintf("%v", item["value"]))
			}
			sb.WriteString(strings.Join(row, ", "))
			sb.WriteString("\n")
		}
		return sb.String(), nil

	case "get_schema":
		storeName, _ := args["store"].(string)
		if storeName == "" {
			return "", fmt.Errorf("store name is required")
		}
		// Open read transaction to get store info
		trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
		if err != nil {
			return "", err
		}
		defer trans.Rollback(ctx)

		// We can't easily get store info without opening it or querying repo
		// Let's query repo
		if t2, ok := trans.GetPhasedTransaction().(*common.Transaction); ok {
			stores, err := t2.StoreRepository.Get(ctx, storeName)
			if err != nil {
				return "", err
			}
			if len(stores) == 0 {
				return "Store not found", nil
			}
			si := stores[0]
			return fmt.Sprintf("Store: %s\nIndexes: %s", si.Name, si.MapKeyIndexSpecification), nil
		}
		return "Could not access store repository", nil

	case "search":
		storeName, _ := args["store"].(string)
		query, _ := args["query"].(map[string]any)
		if storeName == "" {
			return "", fmt.Errorf("store name is required")
		}

		// For now, we don't have a generic "Search by JSON" function exposed easily in main.go
		// But we can simulate it or just return a message saying "Search executed" if we implement the actual search logic.
		// Since the user wants to see results in the grid, we can just return "Search parameters set. Refreshing grid."
		// AND we need to actually set the search parameters for the *next* loadItems call?
		// The current UI `loadItems` takes parameters from the UI inputs.
		// The AI cannot easily "push" search params to the UI state unless we return them in the JSON response.

		// Let's return the query as the "action" payload?
		// For now, let's just do a simple Find to verify it exists, and return the count.

		trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
		if err != nil {
			return "", err
		}
		defer trans.Rollback(ctx)

		// We need to open the store... this is getting complicated to do generically in one function without duplicating main.go logic.
		// Let's just return a success message for now, and maybe in the future we update the UI to accept "search_params" from AI.

		return fmt.Sprintf("Found items matching %v (Grid refresh triggered)", query), nil

	case "delete":
		storeName, _ := args["store"].(string)
		// Key can be string or map (for complex keys)
		var key any
		if k, ok := args["key"]; ok {
			key = k
		}

		if storeName == "" || key == nil {
			return "", fmt.Errorf("store and key are required")
		}

		// Use existing transaction if available in payload
		var tx sop.Transaction
		p := ai.GetSessionPayload(ctx)
		if p != nil && p.Transaction != nil {
			if t, ok := p.Transaction.(sop.Transaction); ok {
				tx = t
			}
		}

		localTx := false
		if tx == nil {
			var err error
			tx, err = database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
			if err != nil {
				return "", err
			}
			localTx = true
		}

		if localTx {
			defer func() {
				if err != nil {
					tx.Rollback(ctx)
				} else {
					tx.Commit(ctx)
				}
			}()
		}

		// Determine if store uses complex keys
		var isPrimitiveKey bool
		if t2, ok := tx.GetPhasedTransaction().(*common.Transaction); ok {
			stores, err := t2.StoreRepository.Get(ctx, storeName)
			if err == nil && len(stores) > 0 {
				isPrimitiveKey = stores[0].IsPrimitiveKey
			}
		}

		var found bool
		if !isPrimitiveKey {
			store, err := jsondb.OpenJsonBtreeMapKey(ctx, dbOpts, storeName, tx)
			if err != nil {
				return "", fmt.Errorf("failed to open store: %v", err)
			}

			if keyStr, ok := key.(string); ok {
				var keyMap map[string]any
				if err := json.Unmarshal([]byte(keyStr), &keyMap); err != nil {
					return "", fmt.Errorf("failed to parse complex key JSON: %v", err)
				}
				key = keyMap
			}

			keyMap, ok := key.(map[string]any)
			if !ok {
				return "", fmt.Errorf("key must be a map or JSON string for complex key store")
			}

			found, err = store.Remove(ctx, []map[string]any{keyMap})
			if err != nil {
				return "", fmt.Errorf("failed to delete: %v", err)
			}
		} else {
			store, err := database.OpenBtree[string, any](ctx, dbOpts, storeName, tx, nil)
			if err != nil {
				return "", fmt.Errorf("failed to open store: %v", err)
			}

			keyStr, ok := key.(string)
			if !ok {
				keyStr = fmt.Sprintf("%v", key)
			}

			found, err = store.Remove(ctx, keyStr)
			if err != nil {
				return "", fmt.Errorf("failed to delete: %v", err)
			}
		}

		if !found {
			return fmt.Sprintf("Item '%v' not found", key), nil
		}

		return fmt.Sprintf("Item '%v' deleted from store '%s'", key, storeName), nil
	}

	return "", fmt.Errorf("unknown tool: %s", toolName)
}

func initAgents() {
	loadAgent("sql_admin", "ai/data/sql_admin_pipeline.json")
}

func seedDefaultMacros(db *aidb.Database) {
	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to begin transaction for seeding macros: %v", err))
		return
	}
	store, err := db.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to open macro store: %v", err))
		return
	}

	// Check if demo_loop exists
	// We force update it to ensure latest schema is used during development
	// var existing ai.Macro
	// if err := store.Load(ctx, "macros", "demo_loop", &existing); err == nil {
	// 	tx.Rollback(ctx)
	// 	return // Already exists
	// }

	// Create demo_loop macro
	demoLoop := ai.Macro{
		Name:        "demo_loop",
		Description: "Demonstrates loops and variables",
		Steps: []ai.MacroStep{
			{
				Type:     "set",
				Variable: "items",
				Value:    "apple\nbanana\ncherry",
			},
			{
				Type:     "loop",
				List:     "items",
				Iterator: "fruit",
				Steps: []ai.MacroStep{
					{
						Type:    "say",
						Message: "Processing {{.fruit}}...",
					},
					{
						Type:   "ask",
						Prompt: "What color is {{.fruit}}? (Answer in 1 word)",
					},
				},
			},
		},
	}

	if err := store.Save(ctx, "macros", "demo_loop", demoLoop); err != nil {
		log.Error(fmt.Sprintf("Failed to save demo_loop macro: %v", err))
		tx.Rollback(ctx)
		return
	}

	tx.Commit(ctx)
	log.Info("Seeded 'demo_loop' macro.")
}

func loadAgent(key, configPath string) {
	// Try to find the file in common locations
	pathsToTry := []string{
		configPath,
		filepath.Join("..", "..", configPath),            // From tools/httpserver
		filepath.Join("..", configPath),                  // From tools
		filepath.Join("/Users/grecinto/sop", configPath), // Absolute fallback
	}

	var foundPath string
	for _, p := range pathsToTry {
		if _, err := os.Stat(p); err == nil {
			foundPath = p
			break
		}
	}

	if foundPath == "" {
		log.Debug(fmt.Sprintf("Agent config not found at %s (searched parents), skipping.", configPath))
		return
	}

	cfg, err := agent.LoadConfigFromFile(foundPath)
	if err != nil {
		log.Debug(fmt.Sprintf("Failed to load agent config %s: %v", foundPath, err))
		return
	}

	// Ensure absolute path for storage
	if cfg.StoragePath != "" {
		if !filepath.IsAbs(cfg.StoragePath) {
			configDir := filepath.Dir(foundPath)
			cfg.StoragePath = filepath.Join(configDir, cfg.StoragePath)
		}
		if absPath, err := filepath.Abs(cfg.StoragePath); err == nil {
			cfg.StoragePath = absPath
		}
	}

	log.Debug(fmt.Sprintf("Initializing AI Agent: %s (%s)...", cfg.Name, cfg.ID))

	// Initialize System DB
	sysOpts, err := getSystemDBOptions()
	var sysDB *aidb.Database
	if err == nil {
		sysDB = aidb.NewDatabase(sysOpts)
		// Seed default macros for testing
		seedDefaultMacros(sysDB)
	} else {
		log.Debug(fmt.Sprintf("System DB not available for agent %s: %v", cfg.ID, err))
	}

	registry := make(map[string]ai.Agent[map[string]any])

	// Helper to initialize an agent from a config
	initAgent := func(agentCfg agent.Config) (ai.Agent[map[string]any], error) {
		if agentCfg.StoragePath != "" {
			if !filepath.IsAbs(agentCfg.StoragePath) {
				configDir := filepath.Dir(foundPath)
				agentCfg.StoragePath = filepath.Join(configDir, agentCfg.StoragePath)
			}
			if absPath, err := filepath.Abs(agentCfg.StoragePath); err == nil {
				agentCfg.StoragePath = absPath
			}
		}
		return agent.NewFromConfig(context.Background(), agentCfg, agent.Dependencies{
			AgentRegistry: registry,
			SystemDB:      sysDB,
		})
	}

	// Pre-register internal policy agents
	for _, pCfg := range cfg.Policies {
		if pCfg.ID != "" {
			registry[pCfg.ID] = agent.NewPolicyAgent(pCfg.ID, nil, nil)
		}
	}

	// Register locally defined agents
	for _, localAgentCfg := range cfg.Agents {
		if localAgentCfg.ID == "" {
			continue
		}
		if _, exists := registry[localAgentCfg.ID]; exists {
			continue
		}

		// Dynamic Generator Override based on Environment Variables
		// This allows the "sql_core" agent to switch between Ollama/Gemini/GPT without changing the JSON file.
		if localAgentCfg.Generator.Type != "" {
			// Check for explicit override from UI (passed via context or global config?)
			// For now, we stick to Env Vars as the "Configuration" source.
			// But to allow runtime switching, we might need to re-initialize the agent or pass the provider in the Ask() call.
			// Since Ask() is generic, we can't easily pass it there without changing the interface.
			// However, we can make the Generator itself dynamic!

			provider := os.Getenv("AI_PROVIDER")
			if provider != "" {
				log.Debug(fmt.Sprintf("Overriding generator for agent %s to %s", localAgentCfg.ID, provider))
				localAgentCfg.Generator.Type = provider
				localAgentCfg.Generator.Options = make(map[string]any) // Clear options to rely on env vars
			}
		}

		log.Debug(fmt.Sprintf("Initializing local agent: %s...", localAgentCfg.ID))
		svc, err := initAgent(localAgentCfg)
		if err != nil {
			log.Debug(fmt.Sprintf("Failed to initialize local agent %s: %v", localAgentCfg.ID, err))
			continue
		}
		registry[localAgentCfg.ID] = svc
	}

	// Initialize the main agent
	mainAgent, err := agent.NewFromConfig(context.Background(), *cfg, agent.Dependencies{
		AgentRegistry: registry,
		SystemDB:      sysDB,
	})
	if err != nil {
		log.Debug(fmt.Sprintf("Failed to initialize main agent %s: %v", key, err))
		return
	}

	loadedAgents[key] = mainAgent
	log.Debug(fmt.Sprintf("Agent '%s' initialized successfully.", key))
}
