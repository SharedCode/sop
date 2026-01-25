package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	log "log/slog"
	"sort"
	"strings"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/jsondb"
)

// Database interface for script execution
type Database interface {
	BeginTransaction(ctx context.Context, mode sop.TransactionMode, maxTime ...time.Duration) (sop.Transaction, error)
	Config() sop.DatabaseOptions
}

// ScriptInstruction represents a single operation in the script.
type ScriptInstruction struct {
	Op        string         `json:"op"`         // Operation name
	Args      map[string]any `json:"args"`       // Arguments
	InputVar  string         `json:"input_var"`  // Variable to use as input (optional)
	ResultVar string         `json:"result_var"` // Variable to store result (optional)
}

// ScriptContext holds the state of the script execution.
type ScriptContext struct {
	Variables      map[string]any
	Transactions   map[string]sop.Transaction
	TxToDB         map[sop.Transaction]Database // Mapping from Transaction to its Database
	Stores         map[string]jsondb.StoreAccessor
	Databases      map[string]Database
	LastUpdatedVar string // helper to track the prioritization of variable draining
}

// ScriptCursor represents a streaming iterator for script operations.
type ScriptCursor interface {
	Next(ctx context.Context) (any, bool, error)
	Close() error
}

// OrderedFieldsProvider allows cursors to expose the list of fields in order.
type OrderedFieldsProvider interface {
	GetOrderedFields() []string
}

// SpecProvider allows cursors to expose IndexSpecifications for field ordering.
type SpecProvider interface {
	GetIndexSpecs() map[string]*jsondb.IndexSpecification
}

// StoreCursor wraps a StoreAccessor to provide a ScriptCursor.
type StoreCursor struct {
	store     jsondb.StoreAccessor
	storeName string // Add StoreName to support prefixing
	indexSpec *jsondb.IndexSpecification
	ctx       context.Context
	limit     int
	count     int
	filter    map[string]any
	engine    *ScriptEngine
	isDesc    bool
	prefix    any
	started   bool
	closed    bool
}

func (sc *StoreCursor) GetIndexSpecs() map[string]*jsondb.IndexSpecification {
	if sc.indexSpec != nil {
		return map[string]*jsondb.IndexSpecification{
			"key": sc.indexSpec,
		}
	}
	return nil
}

func (sc *StoreCursor) Next(ctx context.Context) (any, bool, error) {
	if sc.closed {
		return nil, false, nil
	}
	if sc.limit > 0 && sc.count >= sc.limit {
		return nil, false, nil
	}

	var ok bool
	var err error

	// Initial positioning is done in Scan, but we need to advance
	if !sc.started {
		// Scan already positioned the cursor at the first element
		sc.started = true
		// Check if current position is valid
		k := sc.store.GetCurrentKey()
		if k == nil {
			return nil, false, nil
		}
		ok = true
	} else {
		if sc.isDesc {
			ok, err = sc.store.Previous(ctx)
		} else {
			ok, err = sc.store.Next(ctx)
		}
		if err != nil {
			return nil, false, err
		}
	}

	for ok {
		k := sc.store.GetCurrentKey()
		v, _ := sc.store.GetCurrentValue(ctx)

		// Prefix Check
		if sc.prefix != nil {
			if kStr, isStr := k.(string); isStr {
				pStr := fmt.Sprintf("%v", sc.prefix)
				if !strings.HasPrefix(kStr, pStr) {
					return nil, false, nil // End of prefix range
				}
			}
		}

		// Filter Check
		item := renderItem(k, v, nil)

		if sc.filter != nil {
			match, err := sc.engine.evaluateCondition(item, sc.filter)
			if err != nil {
				return nil, false, err
			}
			if !match {
				if sc.isDesc {
					ok, err = sc.store.Previous(ctx)
				} else {
					ok, err = sc.store.Next(ctx)
				}
				continue
			}
		}

		// Architecture Enforcement: Always Prefix Internal Items
		// We modify the item (map[string]any) to include the "storeName." prefix for all keys.
		// "renderItem" logic will strip this later if appropriate.
		// StoreName needs to be available in StoreCursor.
		if m, ok := item.(map[string]any); ok && sc.storeName != "" {
			prefixed := make(map[string]any, len(m))
			for k, val := range m {
				// Avoid double prefixing if somehow already prefixed (unlikely for raw store)
				prefixed[sc.storeName+"."+k] = val
			}
			item = prefixed
		}

		sc.count++
		return item, true, nil
	}

	return nil, false, nil
}

func (sc *StoreCursor) Close() error {
	sc.closed = true
	return nil
}

// Join Strategy Constants
const (
	StrategyUnset     = 0
	StrategyIndexSeek = 1
	StrategyInMemory  = 2
	StrategyFullScan  = 3
)

type JoinPlan struct {
	Strategy     int
	IndexFields  []string // Ordered list of fields in the Index
	PrefixFields []string // Fields from ON clause that match the Index Prefix
	IsComposite  bool     // True if the Store uses a Map Key (Composite)
	Ascending    bool     // True if the first prefix field is Ascending
}

// JoinRightCursor performs a streaming join with probing and scanning support.
// It replaces both JoinCursor (Lookup) and NestedLoopJoinCursor (Scan).
type JoinRightCursor struct {
	left      ScriptCursor
	right     jsondb.StoreAccessor
	joinType  string
	on        map[string]any
	ctx       context.Context
	engine    *ScriptEngine
	currentL  any
	matched   bool
	rightIter bool

	// Execution Plan
	plan      JoinPlan
	planReady bool

	// Dataset info for prefixing
	rightStoreName string
	leftStoreName  string

	// Legacy / Runtime State
	useFallback  bool  // optimization: materialization fallback
	fallbackList []any // fallback: in-memory list
	fallbackIdx  int
	closed       bool
	bloomFilter  *BloomFilter // Optimization: Bloom Filter for Right Store Keys
}

func (jc *JoinRightCursor) Next(ctx context.Context) (any, bool, error) {
	if jc.closed {
		return nil, false, nil
	}
	val, ok, err := jc.NextOptimized(ctx)
	if ok && err == nil {
		// b, _ := json.Marshal(val)

		// Attempt to inspect if the result is an OrderedMap
		var fields []string
		if om, isOm := val.(*OrderedMap); isOm {
			fields = om.keys
		} else if om, isOm := val.(OrderedMap); isOm {
			fields = om.keys
		} else if m, isM := val.(map[string]any); isM {
			// standard map, extract keys but they won't be ordered
			for k := range m {
				fields = append(fields, k)
			}
			sort.Strings(fields)
		}

		// log.Debug("payload contents:",
		// 	"Function", "JoinRightCursor.Next",
		// 	"json", string(b),
		// 	"fields_found_in_result", fields,
		// )
	}
	return val, ok, err
}

func (jc *JoinRightCursor) Close() error {
	jc.closed = true
	return jc.left.Close()
}

// FilterCursor filters a stream.
type FilterCursor struct {
	source ScriptCursor
	filter map[string]any
	engine *ScriptEngine
	closed bool
}

func (fc *FilterCursor) Next(ctx context.Context) (any, bool, error) {
	if fc.closed {
		return nil, false, nil
	}
	for {
		item, ok, err := fc.source.Next(ctx)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}

		match, err := fc.engine.evaluateCondition(item, fc.filter)
		if err != nil {
			return nil, false, err
		}
		if match {
			return item, true, nil
		}
	}
}

func (fc *FilterCursor) Close() error {
	fc.closed = true
	return fc.source.Close()
}

func (fc *FilterCursor) GetOrderedFields() []string {
	if provider, ok := fc.source.(OrderedFieldsProvider); ok {
		return provider.GetOrderedFields()
	}
	return nil
}

// ProjectCursor projects fields from a stream.
type ProjectCursor struct {
	source ScriptCursor
	fields []ProjectionField
	closed bool
}

func (pc *ProjectCursor) Next(ctx context.Context) (any, bool, error) {
	if pc.closed {
		return nil, false, nil
	}
	item, ok, err := pc.source.Next(ctx)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	// Use renderItem to handle projection, aliasing, and wildcard expansion
	result := renderItem(nil, item, pc.fields)
	return result, true, nil
}

func (pc *ProjectCursor) Close() error {
	pc.closed = true
	return pc.source.Close()
}

func (pc *ProjectCursor) GetOrderedFields() []string {
	fields := make([]string, len(pc.fields))
	for i, f := range pc.fields {
		fields[i] = f.Dst
	}
	return fields
}

// LimitCursor limits a stream.
type LimitCursor struct {
	source ScriptCursor
	limit  int
	count  int
	closed bool
}

func (lc *LimitCursor) GetIndexSpecs() map[string]*jsondb.IndexSpecification {
	if provider, ok := lc.source.(SpecProvider); ok {
		return provider.GetIndexSpecs()
	}
	return nil
}

func (lc *LimitCursor) GetOrderedFields() []string {
	if provider, ok := lc.source.(OrderedFieldsProvider); ok {
		return provider.GetOrderedFields()
	}
	return nil
}

func (lc *LimitCursor) Next(ctx context.Context) (any, bool, error) {
	if lc.closed {
		return nil, false, nil
	}
	if lc.count >= lc.limit {
		return nil, false, nil
	}
	item, ok, err := lc.source.Next(ctx)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	lc.count++
	return item, true, nil
}

func (lc *LimitCursor) Close() error {
	lc.closed = true
	return lc.source.Close()
}

func NewScriptContext() *ScriptContext {
	return &ScriptContext{
		Variables:    make(map[string]any),
		Transactions: make(map[string]sop.Transaction),
		TxToDB:       make(map[sop.Transaction]Database),
		Stores:       make(map[string]jsondb.StoreAccessor),
		Databases:    make(map[string]Database),
	}
}

// ScriptEngine executes scripts.
type ScriptEngine struct {
	Context         *ScriptContext
	ResolveDatabase func(name string) (Database, error)
	FunctionHandler func(ctx context.Context, name string, args map[string]any) (any, error)
	LastResult      any
	ReturnValue     any
	HasReturned     bool
	StoreOpener     func(ctx context.Context, dbOpts sop.DatabaseOptions, storeName string, tx sop.Transaction) (jsondb.StoreAccessor, error)
}

func NewScriptEngine(ctx *ScriptContext, dbResolver func(string) (Database, error)) *ScriptEngine {
	if ctx == nil {
		ctx = NewScriptContext()
	}
	return &ScriptEngine{
		Context:         ctx,
		ResolveDatabase: dbResolver,
	}
}

func (a *DataAdminAgent) resolveDatabase(name string) (Database, error) {
	if name == "system" && a.systemDB != nil {
		return a.systemDB, nil
	}
	if opts, ok := a.databases[name]; ok {
		return database.NewDatabase(opts), nil
	}
	return nil, fmt.Errorf("database '%s' not found", name)
}

// toolExecuteScript executes a sequence of instructions.
// Args:
// - script: []ScriptInstruction (JSON array)
func (a *DataAdminAgent) toolExecuteScript(ctx context.Context, args map[string]any) (string, error) {
	log.Debug("toolExecuteScript: Called", "args", args)
	scriptRaw, ok := args["script"]
	if !ok {
		return "", fmt.Errorf("script argument is required")
	}

	// Parse Script first to ensure it's valid and normalized
	var script []ScriptInstruction
	if pStr, ok := scriptRaw.(string); ok {
		if err := json.Unmarshal([]byte(pStr), &script); err != nil {
			return "", fmt.Errorf("failed to parse script JSON: %v", err)
		}
	} else if pSlice, ok := scriptRaw.([]any); ok {
		bytes, _ := json.Marshal(pSlice)
		if err := json.Unmarshal(bytes, &script); err != nil {
			return "", fmt.Errorf("failed to parse script array: %v", err)
		}
	} else {
		return "", fmt.Errorf("script must be a JSON string or array")
	}

	// 1. Sanitize Script (Pre-Execution Cleanup)
	// This acts as a firewall against common LLM mistakes (wildcards, sloppy args)
	script = sanitizeScript(script)

	// Stub Mode Check
	if a.Config.StubMode {
		// Log the NORMALIZED script for debugging
		bytes, err := json.MarshalIndent(script, "", "  ")
		if err != nil {
			return "", fmt.Errorf("failed to marshal script for display: %v", err)
		}

		log.Debug("toolExecuteScript called in STUB MODE", "script", string(bytes))
		return fmt.Sprintf("Script prepared successfully (STUBBED). execution skipped.\nGenerated Script:\n```json\n%s\n```", string(bytes)), nil
	}

	// Initialize Engine
	var scriptCtx *ScriptContext
	if a.sessionContext != nil {
		scriptCtx = a.sessionContext
	} else {
		scriptCtx = NewScriptContext()
	}

	// Wrap resolver to handle default database from context
	resolver := func(name string) (Database, error) {
		if name == "" || name == "@db" || name == "current" {
			if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" {
				name = p.CurrentDB
			}
		}
		return a.resolveDatabase(name)
	}

	engine := NewScriptEngine(scriptCtx, resolver)
	engine.StoreOpener = a.StoreOpener
	engine.FunctionHandler = func(c context.Context, name string, args map[string]any) (any, error) {
		// Bridge to Agent's script execution
		// We need to implement this properly, reusing opCallScript logic or similar
		// For now, we can call a method on Agent if we expose it, or inline the logic.
		// Since opCallScript is being moved to Engine, we can just call Engine.CallFunction?
		// But Engine.CallFunction needs access to SystemDB.
		// So we should pass SystemDB to Engine or handle it here.
		return a.opCallScript(c, engine.Context, map[string]any{"name": name, "params": args})
	}

	// Execute
	compiled, err := engine.Compile(script)
	if err != nil {
		return "", fmt.Errorf("compilation failed: %v", err)
	}
	if err := compiled(ctx, engine); err != nil {
		return "", err
	}

	// Return Result
	// Priority 0: Explicit 'return' instruction
	// This should be handled during execution loop if we supported early exit.
	// Since we execute the whole list, we check if the last instruction was a return or provided a return value?
	// Actually, the engine.Execute loop doesn't handle 'return' control flow yet.
	// But assuming we add a 'return' OP, we would look for that.

	// Priority 1: Check for 'output' variable (Explicit override)
	// We only return it if it is non-nil, because some operations (like commit_tx) might use 'output'
	// as a dummy variable but return nil, shadowing the actual result in 'final_result'.
	if val, ok := scriptCtx.Variables["output"]; ok && val != nil {
		return serializeResult(ctx, val)
	}

	// Priority 2: Check for 'final_result' variable (Deprecated: Standard convention for query chains)
	if val, ok := scriptCtx.Variables["final_result"]; ok {
		return serializeResult(ctx, val)
	}

	// Priority 2.5: Check for 'result' variable (Common AI convention)
	if val, ok := scriptCtx.Variables["result"]; ok {
		return serializeResult(ctx, val)
	}

	// Priority 3: Check the last instruction's result variable (Fallback for simple scripts)
	if len(script) > 0 {
		lastInstr := script[len(script)-1]

		// If last instruction is specifically a "return" or "output" op (hypothetically)
		if lastInstr.Op == "return" {
			// If arguments has "value", return it
			if val, ok := lastInstr.Args["value"]; ok {
				// We need to resolve the value if it's a variable reference
				if sVal, ok := val.(string); ok && strings.HasPrefix(sVal, "@") {
					varName := strings.TrimPrefix(sVal, "@") // Basic variable resolution
					// Handle @variable syntax slightly better if needed, but Engine does it.
					// However, at this point, we are outside engine execution loop.
					// The engine should have surely resolved it if we had a return op.
					// But our engine is simple: it executes list of steps.

					// Let's rely on Context variables.
					if v, found := scriptCtx.Variables[varName]; found {
						return serializeResult(ctx, v)
					}
				}
				return serializeResult(ctx, val)
			}
		}

		if lastInstr.ResultVar != "" {
			if val, ok := scriptCtx.Variables[lastInstr.ResultVar]; ok {
				return serializeResult(ctx, val)
			}
		}
	}

	// Priority 4: Fallback to the last updated variable (e.g. if script ends with commit_tx)
	if scriptCtx.LastUpdatedVar != "" {
		if val, ok := scriptCtx.Variables[scriptCtx.LastUpdatedVar]; ok {
			return serializeResult(ctx, val)
		}
	}

	return "Script executed successfully.", nil
}

// CompiledScript is a function that executes the compiled script against an engine.
type CompiledScript func(ctx context.Context, e *ScriptEngine) error

type CachedScript struct {
	Script CompiledScript
	Hash   string
}

func (e *ScriptEngine) Execute(ctx context.Context, script []ScriptInstruction) error {
	compiled, err := CompileScript(script)
	if err != nil {
		return err
	}
	return compiled(ctx, e)
}

func (e *ScriptEngine) Compile(script []ScriptInstruction) (CompiledScript, error) {
	return CompileScript(script)
}

func CompileScript(script []ScriptInstruction) (CompiledScript, error) {
	var steps []func(context.Context, *ScriptEngine) error

	for i, instr := range script {
		// 1. Pre-process Arguments (Template Parsing)
		argResolvers := make(map[string]func(*ScriptEngine) any)
		for k, v := range instr.Args {
			val := v
			if str, ok := val.(string); ok && strings.Contains(str, "{{") {
				// Simple template parser: {{var.field}}
				argResolvers[k] = func(e *ScriptEngine) any {
					return e.resolveTemplate(str)
				}
			} else {
				argResolvers[k] = func(_ *ScriptEngine) any { return val }
			}
		}

		// 2. Bind Operation
		opFunc, err := bindOperation(instr.Op)
		if err != nil {
			return nil, fmt.Errorf("instruction %d: %v", i, err)
		}

		// 3. Create Step Closure
		step := func(ctx context.Context, e *ScriptEngine) error {
			// Resolve Args
			args := make(map[string]any)
			for k, resolver := range argResolvers {
				args[k] = resolver(e)
			}

			// Resolve Input Var
			var input any
			if instr.InputVar != "" {
				var ok bool
				inputVar := e.resolveVarName(instr.InputVar)
				input, ok = e.Context.Variables[inputVar]
				if !ok {
					return fmt.Errorf("input variable '%s' not found", instr.InputVar)
				}
			} else {
				// Implicit piping: Use result of last operation
				input = e.LastResult
			}

			// Execute Operation
			result, err := opFunc(ctx, e, args, input)
			if err != nil {
				return fmt.Errorf("operation '%s' failed: %v", instr.Op, err)
			}

			// Update LastResult for implicit piping
			e.LastResult = result

			// Store Result
			if instr.ResultVar != "" {
				e.Context.Variables[instr.ResultVar] = result
				e.Context.LastUpdatedVar = instr.ResultVar
				// Type Registration
				if db, ok := result.(Database); ok {
					e.Context.Databases[instr.ResultVar] = db
				}
				if tx, ok := result.(sop.Transaction); ok {
					e.Context.Transactions[instr.ResultVar] = tx
				}
				if store, ok := result.(jsondb.StoreAccessor); ok {
					e.Context.Stores[instr.ResultVar] = store
				}
			}
			return nil
		}
		steps = append(steps, step)
	}

	return func(ctx context.Context, e *ScriptEngine) error {
		for _, step := range steps {
			if err := step(ctx, e); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

func (e *ScriptEngine) resolveTemplate(tmpl string) any {
	// Handle {{var.field}}
	// Remove {{ and }}
	trimmed := strings.TrimSuffix(strings.TrimPrefix(tmpl, "{{"), "}}")
	parts := strings.Split(trimmed, ".")

	var current any
	var ok bool

	// First part is variable name
	if len(parts) > 0 {
		current, ok = e.Context.Variables[parts[0]]
		if !ok {
			log.Debug("resolveTemplate var not found", "var", parts[0])
			return tmpl // Return original if var not found? Or nil?
		}
	}

	// Traverse fields
	for _, part := range parts[1:] {
		if currentMap, ok := current.(map[string]any); ok {
			current, ok = currentMap[part]
			if !ok {
				// Try "value" wrapper if not found directly?
				if valMap, ok := currentMap["value"].(map[string]any); ok {
					current, ok = valMap[part]
				}
			}
			if !ok {
				log.Debug("resolveTemplate field not found", "field", part, "map", currentMap)
				return nil // Field not found
			}
		} else {
			log.Debug("resolveTemplate current is not a map", "current", current)
			return nil // Not a map
		}
	}
	log.Debug("resolveTemplate result", "tmpl", tmpl, "current", current)
	return current
}

func bindOperation(op string) (func(context.Context, *ScriptEngine, map[string]any, any) (any, error), error) {
	switch op {
	case "open_db":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.OpenDB(args)
		}, nil
	case "begin_tx":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.BeginTx(ctx, args)
		}, nil
	case "commit_tx":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return nil, e.CommitTx(ctx, args)
		}, nil
	case "rollback_tx":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return nil, e.RollbackTx(ctx, args)
		}, nil
	case "open_store":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.OpenStore(ctx, args)
		}, nil
	case "scan":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Scan(ctx, args)
		}, nil
	case "filter":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.Filter(ctx, input, args)
		}, nil
	case "sort":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.Sort(ctx, input, args)
		}, nil
	case "project":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.Project(ctx, input, args)
		}, nil
	case "limit":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.Limit(ctx, input, args)
		}, nil
	case "join":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.Join(ctx, input, args)
		}, nil
	case "join_right":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.JoinRight(ctx, input, args)
		}, nil
	case "update":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			if l, ok := input.([]map[string]any); ok {
				return e.Update(ctx, l, args)
			}
			return nil, fmt.Errorf("input must be list of maps")
		}, nil
	case "delete":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			if l, ok := input.([]map[string]any); ok {
				return e.Delete(ctx, l, args)
			}
			return nil, fmt.Errorf("input must be list of maps")
		}, nil
	case "inspect":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Inspect(ctx, args)
		}, nil
	case "assign":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			if val, ok := args["value"]; ok {
				return val, nil
			}
			return input, nil
		}, nil
	case "if":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return nil, e.If(ctx, args)
		}, nil
	case "loop":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return nil, e.Loop(ctx, args)
		}, nil
	case "call_function":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.CallFunction(ctx, args)
		}, nil
	case "list_new":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return make([]any, 0), nil
		}, nil
	case "list_append":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return nil, e.ListAppend(ctx, args)
		}, nil
	case "map_merge":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.MapMerge(ctx, args)
		}, nil
	case "first":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.First(ctx, args)
		}, nil
	case "last":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Last(ctx, args)
		}, nil
	case "next":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Next(ctx, args)
		}, nil
	case "previous":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Previous(ctx, args)
		}, nil
	case "find":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			res, err := e.Find(ctx, args)
			log.Debug("Find result", "result", res, "err", err)
			return res, err
		}, nil
	case "add":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Add(ctx, args)
		}, nil
	case "get_current_key":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			res, err := e.GetCurrentKey(ctx, args)
			log.Debug("GetCurrentKey result", "result", res, "err", err)
			return res, err
		}, nil
	case "get_current_value":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.GetCurrentValue(ctx, args)
		}, nil
	case "return":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			if val, ok := args["value"]; ok {
				return val, nil
			}
			return nil, nil
		}, nil
	default:
		return nil, fmt.Errorf("unknown operation: %s", op)
	}
}

func serializeResult(ctx context.Context, val any) (string, error) {
	if cursor, ok := val.(ScriptCursor); ok {
		var results []any
		defer cursor.Close()

		// Helper to force field order if provider available
		var orderedFields []string
		if provider, ok := cursor.(OrderedFieldsProvider); ok {
			orderedFields = provider.GetOrderedFields()
		}

		/*
			log.Debug("serializeResult: Consuming Cursor",
				"cursorType", fmt.Sprintf("%T", cursor),
				"hasOrderedFields", len(orderedFields) > 0,
				"orderedFields", orderedFields)
		*/

		for {
			itemObj, ok, err := cursor.Next(ctx)
			if err != nil {
				return "", fmt.Errorf("failed to read cursor: %v", err)
			}
			if !ok {
				break
			}

			var itemMap map[string]any
			if m, ok := itemObj.(map[string]any); ok {
				itemMap = m
			} else if om, ok := itemObj.(*OrderedMap); ok && om != nil {
				itemMap = om.m
			} else if om, ok := itemObj.(OrderedMap); ok {
				itemMap = om.m
			}

			// Friendly Output Strategy:
			// If we have a map, we apply "Smart Collapsing" to strip unique prefixes (e.g. s_orders.id -> id).
			// This implements the "Friendliness" requirement at the last mile.
			if itemMap != nil {
				// collapseUniqueKeys modifies map in place and returns it
				itemObj = collapseUniqueKeys(itemMap)
			}

			var item any = itemObj
			// Apply top-level field ordering
			if itemMap != nil && len(orderedFields) > 0 {
				// log.Debug("serializeResult calling filterFields", "orderedFields", orderedFields)
				item = filterFields(itemMap, orderedFields)
				/*
					if om, ok := item.(*OrderedMap); ok {
						log.Debug("serializeResult received OrderedMap", "keys", om.keys)
					} else {
						log.Debug("serializeResult received item", "type", fmt.Sprintf("%T", item))
					}
					// Debug log to trace what we actually got
					if om, ok := item.(OrderedMap); ok {
						b, _ := json.Marshal(om)
						log.Debug("payload contents:", "Function", "serializeResult", "keys", om.keys, "json", string(b))
					} else if om, ok := item.(*OrderedMap); ok {
						b, _ := json.Marshal(om)
						log.Debug("payload contents:", "Function", "serializeResult", "keys", om.keys, "json", string(b))
					} else {
						log.Debug("payload contents:", "Function", "serializeResult", "type", fmt.Sprintf("%T", item), "val", item)
					}
				*/
			}

			results = append(results, item)
		}

		val = results
	}

	// Friendly Output: Collapse unique keys for cleaner JSON
	if list, ok := val.([]any); ok {
		for i, itemObj := range list {
			var itemMap map[string]any
			if m, ok := itemObj.(map[string]any); ok {
				itemMap = m
			} else if om, ok := itemObj.(*OrderedMap); ok && om != nil {
				itemMap = om.m
			} else if om, ok := itemObj.(OrderedMap); ok {
				itemMap = om.m
			}

			if itemMap != nil {
				// collapseUniqueKeys modifies in-place
				collapsed := collapseUniqueKeys(itemMap)
				// Replace with vanilla map to ensure JSON marshaling uses the new keys (and skips deleted ones)
				// This sidesteps checking if it was OrderedMap with stale keys.
				list[i] = collapsed
			}
		}
	}

	bytes, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to serialize result: %v", err)
	}
	resStr := string(bytes)
	// log.Debug("payload contents:", "json", resStr[:min(len(resStr), 500)], "Function", "serializeResult")
	return resStr, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (e *ScriptEngine) CallScript(ctx context.Context, args map[string]any) (any, error) {
	name, _ := args["name"].(string)
	params, _ := args["params"].(map[string]any)

	if e.FunctionHandler != nil {
		return e.FunctionHandler(ctx, name, params)
	}
	return nil, fmt.Errorf("function handler not available")
}

// Deprecated: Use Compile/Execute instead
func (e *ScriptEngine) Dispatch(ctx context.Context, instr ScriptInstruction) error {
	var input any
	if instr.InputVar != "" {
		var ok bool
		input, ok = e.Context.Variables[instr.InputVar]
		if !ok {
			return fmt.Errorf("input variable '%s' not found", instr.InputVar)
		}
	}

	var result any
	var err error

	switch instr.Op {
	case "open_db":
		result, err = e.OpenDB(instr.Args)
	case "begin_tx":
		result, err = e.BeginTx(ctx, instr.Args)
	case "commit_tx":
		err = e.CommitTx(ctx, instr.Args)
	case "rollback_tx":
		err = e.RollbackTx(ctx, instr.Args)
	case "open_store":
		result, err = e.OpenStore(ctx, instr.Args)
	case "scan":
		result, err = e.Scan(ctx, instr.Args)
	case "filter":
		result, err = e.Filter(ctx, input, instr.Args)
	case "sort":
		result, err = e.Sort(ctx, input, instr.Args)
	case "project":
		result, err = e.Project(ctx, input, instr.Args)
	case "limit":
		result, err = e.Limit(ctx, input, instr.Args)
	case "join":
		result, err = e.Join(ctx, input, instr.Args)
	case "update":
		result, err = e.Update(ctx, input, instr.Args)
	case "delete":
		result, err = e.Delete(ctx, input, instr.Args)
	case "inspect":
		result, err = e.Inspect(ctx, instr.Args)
	case "assign":
		// Simple assignment or literal
		if val, ok := instr.Args["value"]; ok {
			result = val
		} else {
			result = input
		}
	case "if":
		err = e.If(ctx, instr.Args)
	case "loop":
		err = e.Loop(ctx, instr.Args)
	case "call_script", "script":
		result, err = e.CallScript(ctx, instr.Args)
	// List / Map Operations (Memory)
	case "list_new":
		result = make([]any, 0)
	case "list_append":
		err = e.ListAppend(ctx, instr.Args)
	case "map_merge":
		result, err = e.MapMerge(ctx, instr.Args)
	// Cursor / Store Operations
	case "first":
		result, err = e.First(ctx, instr.Args)
	case "last":
		result, err = e.Last(ctx, instr.Args)
	case "next":
		result, err = e.Next(ctx, instr.Args)
	case "previous":
		result, err = e.Previous(ctx, instr.Args)
	case "find":
		result, err = e.Find(ctx, instr.Args)
		log.Debug("Find result", "result", result, "err", err)
	case "add":
		result, err = e.Add(ctx, instr.Args)
	case "get_current_key":
		result, err = e.GetCurrentKey(ctx, instr.Args)
		log.Debug("GetCurrentKey result", "result", result, "err", err)
	case "get_current_value":
		result, err = e.GetCurrentValue(ctx, instr.Args)
	default:
		return fmt.Errorf("unknown operation: %s", instr.Op)
	}

	if err != nil {
		return err
	}

	if instr.ResultVar != "" {
		e.Context.Variables[instr.ResultVar] = result
		// Also register specific types if needed
		if db, ok := result.(Database); ok {
			e.Context.Databases[instr.ResultVar] = db
		}
		if tx, ok := result.(sop.Transaction); ok {
			e.Context.Transactions[instr.ResultVar] = tx
		}
		if store, ok := result.(jsondb.StoreAccessor); ok {
			e.Context.Stores[instr.ResultVar] = store
		}
	}

	return nil
}

// --- Operations ---

// resolveVarName strips the optional '@' prefix from a variable name.
func (e *ScriptEngine) resolveVarName(name string) string {
	return strings.TrimPrefix(name, "@")
}

func (e *ScriptEngine) OpenDB(args map[string]any) (Database, error) {
	name, _ := args["name"].(string)
	if name == "" {
		return nil, fmt.Errorf("database name required")
	}
	if e.ResolveDatabase == nil {
		return nil, fmt.Errorf("database resolver not configured")
	}
	return e.ResolveDatabase(name)
}

func (e *ScriptEngine) BeginTx(ctx context.Context, args map[string]any) (sop.Transaction, error) {
	dbName, _ := args["database"].(string) // Variable name of DB
	dbName = e.resolveVarName(dbName)
	modeStr, _ := args["mode"].(string)

	var db Database
	if dbObj, ok := e.Context.Databases[dbName]; ok {
		db = dbObj
	} else {
		// Try to resolve by name if not a variable
		if e.ResolveDatabase == nil {
			return nil, fmt.Errorf("database resolver not configured")
		}
		var err error
		db, err = e.ResolveDatabase(dbName)
		if err != nil {
			return nil, fmt.Errorf("database '%s' not found in context or registry", dbName)
		}
	}

	mode := sop.ForReading
	if modeStr == "write" {
		mode = sop.ForWriting
	}

	tx, err := db.BeginTransaction(ctx, mode)
	if err == nil {
		if e.Context.TxToDB == nil {
			e.Context.TxToDB = make(map[sop.Transaction]Database)
		}
		e.Context.TxToDB[tx] = db
	}
	return tx, err
}

func (e *ScriptEngine) CommitTx(ctx context.Context, args map[string]any) error {
	txName, _ := args["transaction"].(string)
	txName = e.resolveVarName(txName)
	tx, ok := e.Context.Transactions[txName]
	if !ok {
		return fmt.Errorf("transaction '%s' not found", txName)
	}

	// Trigger: Commit Transaction
	// Action: Materialize all active cursors to ensure they are captured before transaction closes.

	drain := func(name string, cursor ScriptCursor) error {
		results := make([]any, 0)

		// Helper to wrap fields if spec available
		var specs map[string]*jsondb.IndexSpecification
		if provider, ok := cursor.(SpecProvider); ok {
			specs = provider.GetIndexSpecs()
		}

		// Drain cursor
		for {
			itemObj, ok, err := cursor.Next(ctx)
			if err != nil {
				return fmt.Errorf("failed to materialize cursor '%s' before commit: %v", name, err)
			}
			if !ok {
				break
			}

			var itemMap map[string]any
			if m, ok := itemObj.(map[string]any); ok {
				itemMap = m
			} else if om, ok := itemObj.(*OrderedMap); ok && om != nil {
				itemMap = om.m
			} else if om, ok := itemObj.(OrderedMap); ok {
				itemMap = om.m
			}

			// Apply ordering if specs available
			if itemMap != nil && len(specs) > 0 {
				for fieldName, spec := range specs {
					if val, ok := itemMap[fieldName]; ok {
						if m, ok := val.(map[string]any); ok {
							itemMap[fieldName] = OrderedKey{m: m, spec: spec}
						}
					}
				}
			}
			results = append(results, itemObj)
		}
		cursor.Close()
		e.Context.Variables[name] = results
		return nil
	}

	// Prioritize LastUpdatedVar to ensure downstream cursors (limit, etc) get first dibs on consuming shared inputs
	if e.Context.LastUpdatedVar != "" {
		if val, ok := e.Context.Variables[e.Context.LastUpdatedVar]; ok {
			if cursor, ok := val.(ScriptCursor); ok {
				if err := drain(e.Context.LastUpdatedVar, cursor); err != nil {
					return err
				}
			}
		}
	}

	for name, val := range e.Context.Variables {
		if name == e.Context.LastUpdatedVar {
			continue
		}
		if cursor, ok := val.(ScriptCursor); ok {
			if err := drain(name, cursor); err != nil {
				return err
			}
		}
	}

	return tx.Commit(ctx)
}

func (e *ScriptEngine) RollbackTx(ctx context.Context, args map[string]any) error {
	txName, _ := args["transaction"].(string)
	txName = e.resolveVarName(txName)
	tx, ok := e.Context.Transactions[txName]
	if !ok {
		return fmt.Errorf("transaction '%s' not found", txName)
	}
	return tx.Rollback(ctx)
}

func (e *ScriptEngine) OpenStore(ctx context.Context, args map[string]any) (jsondb.StoreAccessor, error) {
	txName, _ := args["transaction"].(string)
	txName = e.resolveVarName(txName)
	storeName, _ := args["name"].(string)

	var tx sop.Transaction
	var ok bool

	if txName == "" {
		// Try to find a default transaction
		if len(e.Context.Transactions) == 1 {
			for _, t := range e.Context.Transactions {
				tx = t
				ok = true
				break
			}
		} else if len(e.Context.Transactions) > 1 {
			return nil, fmt.Errorf("transaction name required (multiple active transactions)")
		} else {
			return nil, fmt.Errorf("no active transaction found")
		}
	} else {
		tx, ok = e.Context.Transactions[txName]
	}

	if !ok {
		return nil, fmt.Errorf("transaction '%s' not found", txName)
	}

	// Resolve Database
	var db Database
	dbName, _ := args["database"].(string)
	dbName = e.resolveVarName(dbName)

	if dbName != "" {
		// Explicit database argument
		var found bool
		db, found = e.Context.Databases[dbName]
		if !found {
			if e.ResolveDatabase != nil {
				var err error
				db, err = e.ResolveDatabase(dbName)
				if err != nil {
					return nil, fmt.Errorf("database '%s' not found", dbName)
				}
			} else {
				return nil, fmt.Errorf("database '%s' not found", dbName)
			}
		}
	} else {
		// Implicit database resolution
		// 1. Try mapping from transaction
		if associatedDB, found := e.Context.TxToDB[tx]; found {
			db = associatedDB
		} else if len(e.Context.Databases) == 1 {
			// 2. Fallback to single active database
			for _, d := range e.Context.Databases {
				db = d
				break
			}
		} else if len(e.Context.Databases) > 1 {
			return nil, fmt.Errorf("database argument required (multiple open databases)")
		} else {
			return nil, fmt.Errorf("database argument required")
		}
	}

	create, _ := args["create"].(bool)

	if e.StoreOpener != nil {
		// Mock/Override doesn't standardized on Create yet?
		// We assume StoreOpener handles it or we update signature?
		// StoreOpener signature: func(ctx, opts, name, tx) (StoreAccessor, error)
		// It doesn't take 'create' flag.
		// We can't change signature easily as it might be used elsewhere?
		// But e.StoreOpener is defined in this file.
		return e.StoreOpener(ctx, db.Config(), storeName, tx)
	}

	if create {
		// If Scripts store is not available yet, we will create the Script store here.
		return jsondb.CreateObjectStore(ctx, db.Config(), storeName, tx)
	}
	return jsondb.OpenStore(ctx, db.Config(), storeName, tx)
}

func (e *ScriptEngine) Scan(ctx context.Context, args map[string]any) (any, error) {
	fmt.Printf("DEBUG: Scan Called with args: %+v\n", args)
	storeVarName, _ := args["store"].(string) // Variable name
	storeVarName = e.resolveVarName(storeVarName)
	store, ok := e.Context.Stores[storeVarName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeVarName)
	}

	// Determine logical Store Name for prefixing
	// 1. If we can get the underlying store name from StoreInfo, use that.
	// 2. Fallback to variable name (might be "users_store" instead of "users").
	// Ideally, we want the "business name" of the table.
	storeName := storeVarName
	info := store.GetStoreInfo()
	if info.Name != "" {
		storeName = info.Name
	}

	limit, _ := args["limit"].(float64)
	if limit <= 0 {
		limit = 1000
	}
	direction, _ := args["direction"].(string)
	isDesc := strings.ToLower(direction) == "desc" || strings.ToLower(direction) == "backward"
	startKey := args["start_key"]
	prefix := args["prefix"]
	filter := args["filter"]
	fmt.Printf("DEBUG: Scan Filter extracted: %+v\n", filter)
	stream, _ := args["stream"].(bool)

	var okIter bool
	var err error

	// Inspect store for IndexSpecification
	var indexSpec *jsondb.IndexSpecification
	info = store.GetStoreInfo()
	if info.MapKeyIndexSpecification != "" {
		var spec jsondb.IndexSpecification
		if err := json.Unmarshal([]byte(info.MapKeyIndexSpecification), &spec); err == nil {
			indexSpec = &spec
		}
	}

	// Positioning
	if startKey != nil {
		if isDesc {
			okIter, err = store.FindInDescendingOrder(ctx, startKey)
		} else {
			okIter, err = store.FindOne(ctx, startKey, true)
		}
	} else if prefix != nil {
		if isDesc {
			okIter, err = store.FindInDescendingOrder(ctx, prefix)
		} else {
			okIter, err = store.FindOne(ctx, prefix, true)
		}
	} else {
		if isDesc {
			okIter, err = store.Last(ctx)
		} else {
			okIter, err = store.First(ctx)
		}
	}

	if err != nil {
		return nil, err
	}
	fmt.Printf("DEBUG: Scan Store '%s' okIter=%v\n", storeName, okIter)

	if !okIter {
		if stream {
			// Return empty cursor
			return &StoreCursor{
				store:     store,
				storeName: storeName, // Pass store name
				indexSpec: indexSpec,
				ctx:       ctx,
				limit:     int(limit),
				started:   true, // Started but empty
			}, nil
		}
		return []map[string]any{}, nil
	}

	var filterMap map[string]any
	if filter != nil {
		if m, ok := filter.(map[string]any); ok {
			filterMap = m
		}
	}

	if stream {
		return &StoreCursor{
			store:     store,
			storeName: storeName, // Pass store name
			indexSpec: indexSpec,
			ctx:       ctx,
			limit:     int(limit),
			filter:    filterMap,
			engine:    e,
			isDesc:    isDesc,
			prefix:    prefix,
			started:   false, // Not yet advanced past the first element (Scan positions at first)
		}, nil
	}

	var results []map[string]any
	count := 0
	for okIter && count < int(limit) {
		k := store.GetCurrentKey()
		v, _ := store.GetCurrentValue(ctx)

		// Prefix Check
		if prefix != nil {
			if kStr, isStr := k.(string); isStr {
				pStr := fmt.Sprintf("%v", prefix)
				if !strings.HasPrefix(kStr, pStr) {
					break
				}
			}
		}

		// Filter Check (Push Down)
		itemAny := renderItem(k, v, nil)
		// Since renderItem returns any (possibly OrderedMap pointer), we cast to map if needed or keep as any
		// But Scan Loop expects []map[string]any for results var?
		// results is []map[string]any.
		// renderItem with nil fields returns flattenItem result which is map[string]any.

		item, _ := itemAny.(map[string]any)

		if filter != nil {
			// Debugging schema validation issues
			fmt.Printf("DEBUG: Scan Filter Check. ItemType=%T Item=%+v\n", itemAny, item)
			fmt.Printf("DEBUG: Scan Filter=%+v\n", filter)

			match, err := e.evaluateCondition(item, filter.(map[string]any))
			if err != nil {
				// Log error but continue? Or fail?
				// Fail for now
				return nil, fmt.Errorf("filter evaluation failed: %v", err)
			}
			if !match {
				// Skip this item
				if isDesc {
					okIter, _ = store.Previous(ctx)
				} else {
					okIter, _ = store.Next(ctx)
				}
				continue
			}
		}

		// Architecture Enforcement: Always Prefix Internal Items (Buffered Mode)
		if storeName != "" {
			prefixed := make(map[string]any, len(item))
			for k, val := range item {
				prefixed[storeName+"."+k] = val
			}
			item = prefixed
		}

		results = append(results, item)
		count++

		if isDesc {
			okIter, _ = store.Previous(ctx)
		} else {
			okIter, _ = store.Next(ctx)
		}
	}

	return results, nil
}

func (e *ScriptEngine) evaluateCondition(item any, condition any) (bool, error) {
	// CEL Filter
	if _, ok := condition.(string); ok {
		return false, fmt.Errorf("CEL filter expressions not supported yet")
	}

	// Simple Map Match
	if matchMap, ok := condition.(map[string]any); ok {
		// Validating fields against schema in item
		// If item is map[string]any, we can check if keys in condition exist in item.
		// However, item might be partial if projected?
		// Typically filtering happens before projection, so item should be full.
		if itemMap, ok := item.(map[string]any); ok {
			for k := range matchMap {
				if _, exists := itemMap[k]; !exists {
					// Check for fuzzy match
					if suggested := findSimilarKey(k, itemMap); suggested != "" {
						return false, fmt.Errorf("field '%s' not found in item. Did you mean '%s'?", k, suggested)
					}
					// If no fuzzy match, it might be a valid non-existent field (evaluates to non-match usually),
					// BUT for LLM correctness, explicitly failing on hallucinated fields is better.
					// Unless condition operator is $exists: false?
					// matchMap is key -> condition for that key.
					// Let's assume strict schema enforcement for now to guide LLM.
					return false, fmt.Errorf("field '%s' not found in item. Available fields: %v", k, getKeys(itemMap))
				}
			}
		}

		return matchesMap(item, matchMap), nil
	}

	return false, fmt.Errorf("unsupported filter condition type")
}

func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func findSimilarKey(target string, m map[string]any) string {
	// Simple fuzzy match: case insensitive or contains
	targetLower := strings.ToLower(target)
	for k := range m {
		if strings.ToLower(k) == targetLower {
			return k
		}
		if strings.Contains(strings.ToLower(k), targetLower) || strings.Contains(targetLower, strings.ToLower(k)) {
			return k
		}
	}
	return ""
}

// Reuse the logic from pipeline for Filter, Sort, Project, Limit
// But adapt to take 'any' input which is expected to be []map[string]any

func (e *ScriptEngine) Filter(ctx context.Context, input any, args map[string]any) (any, error) {
	conditionRaw := args["condition"]
	if conditionRaw == nil {
		return input, nil
	}

	if cursor, ok := input.(ScriptCursor); ok {
		if condMap, ok := conditionRaw.(map[string]any); ok {
			return &FilterCursor{
				source: cursor,
				filter: condMap,
				engine: e,
			}, nil
		}
		// Fallback for non-map execution if needed, or error
		return nil, fmt.Errorf("cursor filter currently passes map conditions only")
	}

	var list []any
	if l, ok := input.([]any); ok {
		list = l
	} else if lMap, ok := input.([]map[string]any); ok {
		list = make([]any, len(lMap))
		for i, v := range lMap {
			list[i] = v
		}
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}
	return e.stageFilter(list, args)
}

func (e *ScriptEngine) Sort(ctx context.Context, input any, args map[string]any) (any, error) {
	// Sort requires materialization
	var list []any

	if cursor, ok := input.(ScriptCursor); ok {
		// Materialize
		for {
			item, ok, err := cursor.Next(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			list = append(list, item)
		}
	} else if l, ok := input.([]any); ok {
		list = l
	} else if lMap, ok := input.([]map[string]any); ok {
		list = make([]any, len(lMap))
		for i, v := range lMap {
			list[i] = v
		}
	} else {
		return nil, fmt.Errorf("input must be a list or cursor")
	}

	return e.stageSort(list, args)
}

func (e *ScriptEngine) Project(ctx context.Context, input any, args map[string]any) (any, error) {
	log.Debug("Project input", "type", fmt.Sprintf("%T", input))

	// Use common parsing logic
	parsedFields := parseProjectionFields(args["fields"])

	if cursor, ok := input.(ScriptCursor); ok {
		log.Debug("Project returning ProjectCursor")
		return &ProjectCursor{
			source: cursor,
			fields: parsedFields,
		}, nil
	}

	log.Debug("Project falling back to stageProject (List)", "input_type", fmt.Sprintf("%T", input))

	var list []any
	if l, ok := input.([]any); ok {
		list = l
	} else if lMap, ok := input.([]map[string]any); ok {
		list = make([]any, len(lMap))
		for i, v := range lMap {
			list[i] = v
		}
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}
	return e.stageProject(list, args)
}

func (e *ScriptEngine) Limit(ctx context.Context, input any, args map[string]any) (any, error) {
	log.Debug("Limit input", "type", fmt.Sprintf("%T", input))
	limitVal, _ := args["limit"].(float64)
	limit := int(limitVal)
	if limit <= 0 {
		limit = 1000 // Default?
	}

	if cursor, ok := input.(ScriptCursor); ok {
		log.Debug("Limit returning LimitCursor")
		return &LimitCursor{
			source: cursor,
			limit:  limit,
		}, nil
	}

	log.Debug("Limit falling back to stageLimit (List)", "input_type", fmt.Sprintf("%T", input))

	var list []any
	if l, ok := input.([]any); ok {
		list = l
	} else if lMap, ok := input.([]map[string]any); ok {
		list = make([]any, len(lMap))
		for i, v := range lMap {
			list[i] = v
		}
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}
	return e.stageLimit(list, args)
}

// ListCursor wraps a slice of maps.
type ListCursor struct {
	items []any
	index int
}

func (lc *ListCursor) Next(ctx context.Context) (any, bool, error) {
	if lc.index >= len(lc.items) {
		return nil, false, nil
	}
	item := lc.items[lc.index]
	lc.index++
	return item, true, nil
}

func (lc *ListCursor) Close() error {
	return nil
}

// MultiCursor chains multiple cursors.
type MultiCursor struct {
	cursors []ScriptCursor
	current int
}

func (mc *MultiCursor) Next(ctx context.Context) (any, bool, error) {
	for mc.current < len(mc.cursors) {
		item, ok, err := mc.cursors[mc.current].Next(ctx)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return item, true, nil
		}
		// Current cursor exhausted, move to next
		mc.current++
	}
	return nil, false, nil
}

func (mc *MultiCursor) Close() error {
	var firstErr error
	for _, c := range mc.cursors {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// handleInto checks for 'into' argument and drains cursor/list to a store.
func (e *ScriptEngine) handleInto(ctx context.Context, input any, args map[string]any) (any, error) {
	intoStoreName, _ := args["into"].(string)
	if intoStoreName == "" {
		return input, nil
	}

	// Resolve store name
	intoStoreName = e.resolveVarName(intoStoreName)

	// Create/Open the store. We assume "create" is needed if explicit 'into' is used for a temp store.
	// But it might be an existing store? Let's check if user requested create behavior.
	// If it's a "temp" store for pipeline, we likely want to create it.
	// Let's assume we create/open it.
	store, err := e.OpenStore(ctx, map[string]any{"name": intoStoreName, "create": true})
	if err != nil {
		return nil, fmt.Errorf("failed to open/create store '%s': %v", intoStoreName, err)
	}

	// Drain Input into Store
	cursor, isCursor := input.(ScriptCursor)
	list, isList := input.([]any)

	if !isCursor && !isList {
		// Try casting list of maps?
		if lMap, ok := input.([]map[string]any); ok {
			for _, v := range lMap {
				list = append(list, v)
			}
			isList = true
		} else {
			return nil, fmt.Errorf("cannot pour result of type %T into store (must be cursor or list)", input)
		}
	}

	count := 0

	addToStore := func(item any) error {
		count++
		// Generate Key?
		// We use a simple counter key or UUID?
		// User didn't specify key field.
		// "into" implies a table structure. We just dump items.
		// Use UUID or simple sequence.
		key := fmt.Sprintf("row_%d_%d", time.Now().UnixNano(), count)
		_, err := store.Add(ctx, key, item)
		return err
	}

	if isCursor {
		defer cursor.Close()
		for {
			item, ok, err := cursor.Next(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			if err := addToStore(item); err != nil {
				return nil, err
			}
		}
	} else {
		for _, item := range list {
			if err := addToStore(item); err != nil {
				return nil, err
			}
		}
	}

	// Return the Store Accessor? Or just the name?
	// The next step in pipeline would use `store: "temp1"`.
	// ScriptInstruction returns result to ResultVar.
	// If we return the store object, it gets put into generic var.
	// But `Join` expects store name or variable with list/cursor.
	// If `Join` gets a StoreAccessor variable, it doesn't handle it directly as Input.
	// It handles it as `args["store"]`.
	// So we should return the StoreAccessor, so user can say `result_var: "myStore"`.
	return store, nil
}

func (e *ScriptEngine) Join(ctx context.Context, input any, args map[string]any) (any, error) {
	rightVar, _ := args["with"].(string)
	if rightVar == "" {
		// Try 'store' arg (alias for join_right)
		rightVar, _ = args["store"].(string)
	}
	rightVar = e.resolveVarName(rightVar)

	joinType, _ := args["type"].(string)
	if joinType == "" {
		joinType = "inner"
	} else {
		joinType = strings.ToLower(strings.TrimSpace(joinType))
		if strings.Contains(joinType, "outer") {
			joinType = strings.ReplaceAll(joinType, " outer", "")
		}
		if strings.Contains(joinType, "join") {
			joinType = strings.ReplaceAll(joinType, " join", "")
		}
		joinType = strings.TrimSpace(joinType)
	}

	on, _ := args["on"].(map[string]any)
	if on == nil {
		if onList, ok := args["on"].([]any); ok {
			on = make(map[string]any)
			for _, v := range onList {
				if s, ok := v.(string); ok {
					on[s] = s
				}
			}
		}
	}

	// Prepare Left Cursor
	var leftCursor ScriptCursor
	if lc, ok := input.(ScriptCursor); ok {
		leftCursor = lc
	} else if list, ok := input.([]map[string]any); ok {
		var anyList []any
		for _, x := range list {
			anyList = append(anyList, x)
		}
		leftCursor = &ListCursor{items: anyList}
	} else if anyList, ok := input.([]any); ok {
		leftCursor = &ListCursor{items: anyList}
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}

	// Check if Right is a Store
	rightStore, isRightStore := e.Context.Stores[rightVar]

	// Determine Aliases
	rightAlias, _ := args["right_alias"].(string)
	if rightAlias == "" {
		if a, ok := args["store"].(string); ok && a != "" {
			rightAlias = strings.TrimPrefix(a, "@")
		} else if a, ok := args["right_dataset"].(string); ok && a != "" {
			rightAlias = strings.TrimPrefix(a, "@")
		} else if isRightStore && rightStore != nil {
			// Prefer Store Name as alias over variable name to support natural projection (e.g. "users.*")
			rightAlias = rightStore.GetStoreInfo().Name
		} else {
			rightAlias = rightVar
		}
	}

	leftAlias, _ := args["left_alias"].(string)

	log.Debug("Join", "RightVar", rightVar, "IsStore", isRightStore, "JoinType", joinType)

	var result any
	var err error

	// --- FULL OUTER JOIN SUPPORT ---
	if joinType == "full" && isRightStore {
		// Materialize Left Input locally (needed for Right Anti-Join part)
		var leftList []any
		if lc, ok := leftCursor.(*ListCursor); ok {
			leftList = lc.items
		} else {
			// Drain if not already a list
			for {
				item, ok, err := leftCursor.Next(ctx)
				if err != nil {
					return nil, err
				}
				if !ok {
					break
				}
				leftList = append(leftList, item)
			}
			// Reset leftCursor to a ListCursor over the same data for the Left Join part
			leftCursor = &ListCursor{items: leftList}
		}

		// Invert ON clause: {LeftKey: RightKey} -> {RightKey: LeftKey}
		invertedOn := make(map[string]any)
		for k, v := range on {
			invertedOn[fmt.Sprintf("%v", v)] = k
		}

		// 1. Left Join (Standard)
		leftJoinCursor := &JoinRightCursor{
			left:           leftCursor,
			right:          rightStore,
			joinType:       "left",
			on:             on,
			ctx:            ctx,
			engine:         e,
			rightStoreName: rightAlias,
			leftStoreName:  leftAlias,
		}

		// 2. Right Anti-Join (Suppressed Matches)
		rightAntiJoinCursor := &RightOuterJoinStoreCursor{
			rightStore:      rightStore,
			leftList:        leftList,
			on:              invertedOn,
			ctx:             ctx,
			engine:          e,
			rightAlias:      rightAlias,
			suppressMatches: true,
		}

		multi := &MultiCursor{cursors: []ScriptCursor{leftJoinCursor, rightAntiJoinCursor}}
		return e.handleInto(ctx, multi, args)
	}

	// --- RIGHT OUTER JOIN SUPPORT (Swapped Left Join) ---
	if joinType == "right" {
		// Invert ON clause: {LeftKey: RightKey} -> {RightKey: LeftKey}
		invertedOn := make(map[string]any)
		for k, v := range on {
			invertedOn[fmt.Sprintf("%v", v)] = k
		}

		// Materialize Left Input locally (as it mimics "Look up" table in this reversed join)
		// We pass the cursor to RightOuterJoinStoreCursor to let it handle materialization (or spilling) lazily.
		var leftList []any
		if lc, ok := leftCursor.(*ListCursor); ok {
			leftList = lc.items
		}

		if isRightStore {
			// Right Side is Store: Use Streaming RightOuterJoinStoreCursor
			// This scans the Right Store and probes the Left Cursor (which builds an index internally).
			// We pass the Engine to allow creating temp stores if needed for spilling.
			result, err = &RightOuterJoinStoreCursor{
				rightStore: rightStore,
				// Pass Cursor if list is not fully available, otherwise pass list
				leftCursor: leftCursor,
				leftList:   leftList,
				on:         invertedOn,
				ctx:        ctx,
				engine:     e,
				rightAlias: rightAlias, // Pass alias
				leftAlias:  leftAlias,
			}, nil
		} else {
			// Right Side is Value: Use In-Memory stageJoin with swapped arguments
			// We must materialize left side fully here if it wasn't already
			if len(leftList) == 0 && leftCursor != nil {
				// Drain cursor
				for {
					item, ok, err := leftCursor.Next(ctx)
					if err != nil {
						return nil, err
					}
					if !ok {
						break
					}
					leftList = append(leftList, item)
				}
			}

			// Resolve Right List
			rightInput, ok := e.Context.Variables[rightVar]
			if !ok {
				return nil, fmt.Errorf("right input variable '%s' not found", rightVar)
			}
			var rightList []any
			if l, ok := rightInput.([]map[string]any); ok {
				for _, x := range l {
					rightList = append(rightList, x)
				}
			} else if l, ok := rightInput.([]any); ok {
				rightList = l
			} else {
				return nil, fmt.Errorf("right input must be a list of items")
			}

			// 2. Scan Right, Probe Left (Swapped)
			// Arguments: Left=RightList, Right=LeftList
			// This effectively executes a Left Join where Driver is Right.
			// Result keys: "rightAlias.k" (Driver) and "leftAlias.k" (Probed)
			// Swapped Aliases mapping:
			// Left List came from leftCursor -> use leftAlias
			// Right List came from input var -> use rightAlias

			// We pass: left=rightList (alias=rightAlias), right=leftList (alias=leftAlias)
			// The stageJoin signature is (left, right, type, on, rightAlias, leftAlias)
			// Wait, if we swap lists, we must match aliases to the list content.
			// stageJoin calls the first list "Left" and second list "Right" for iteration logic.
			// But for key prefixing, we want:
			// Items from first list (rightList) -> prefix with rightAlias
			// Items from second list (leftList) -> prefix with leftAlias
			//
			// stageJoin implementation:
			// merged[leftAlias+"."+k] = v  (where v is from first list argument)
			// merged[rightAlias+"."+k] = v (where v is from second list argument)

			// So we must pass: leftAlias=rightAlias, rightAlias=leftAlias
			lAlias := leftAlias
			if lAlias == "" {
				lAlias = "Left"
			}
			rAlias := rightAlias
			if rAlias == "" {
				rAlias = "Right"
			}
			// stageJoin(driverList, probeList, ..., probeAlias, driverAlias)
			result, err = e.stageJoin(rightList, leftList, "left", invertedOn, lAlias, rAlias)
		}

		if err == nil {
			return e.handleInto(ctx, result, args)
		}
		return result, err
	}

	// DECISION: Prefer Lookup Join (JoinRightCursor) for Stores
	// We rely on JoinRightCursor's internal "Smart Mode":
	// 1. It attempts to find a Key Field (Prefix Match) to do fast Lookups.
	// 2. If it fails to identifying a Probe Key, it falls back to In-Memory Materialization.
	// This covers both "Indexed Join" (Fast) and "Non-Indexed Join" (Safe, No Hang).
	useLookupJoin := false
	if isRightStore {
		useLookupJoin = true
	}

	if useLookupJoin {
		// Use JoinRightCursor for optimized lookup join (or adaptive fallback)
		// Alias is already resolved
		cursor := &JoinRightCursor{
			left:           leftCursor,
			right:          rightStore,
			joinType:       joinType,
			on:             on,
			ctx:            ctx,
			engine:         e,
			rightStoreName: rightAlias, // Use resolved alias
		}

		return e.handleInto(ctx, cursor, args)
	}

	// Fallback to In-Memory Nested Loop Join (For Variables, not Stores)
	rightInput, ok := e.Context.Variables[rightVar]
	if !ok {
		return nil, fmt.Errorf("right input variable '%s' not found", rightVar)
	}

	var rightList []any
	if l, ok := rightInput.([]map[string]any); ok {
		for _, x := range l {
			rightList = append(rightList, x)
		}
	} else if l, ok := rightInput.([]any); ok {
		rightList = l
	} else {
		return nil, fmt.Errorf("right input must be a list of items")
	}

	// Materialize Left Cursor
	var leftList []any
	if lc, ok := leftCursor.(*ListCursor); ok {
		leftList = lc.items
	} else {
		// Drain cursor
		for {
			item, ok, err := leftCursor.Next(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			leftList = append(leftList, item)
		}
	}

	result, err = e.stageJoin(leftList, rightList, joinType, on, rightAlias, leftAlias)
	if err == nil {
		// Handle Into (Wait, result is []any. We need to support handleInto for List too)
		return e.handleInto(ctx, result, args)
	}
	return result, err
}

func (e *ScriptEngine) stageJoin(left, right []any, joinType string, on map[string]any, rightAlias string, leftAlias string) ([]any, error) {
	var results []any
	if rightAlias == "" {
		rightAlias = "Right"
	}
	if leftAlias == "" {
		leftAlias = "Left"
	}

	// Track matched Right indices for Full Join
	matchedRight := make(map[int]bool)

	for _, lItem := range left {
		matched := false
		for rIdx, rItem := range right {
			match := true
			for lField, rFieldRaw := range on {
				rField := fmt.Sprintf("%v", rFieldRaw)
				lVal := getField(lItem, lField)
				rVal := getField(rItem, rField)
				if fmt.Sprintf("%v", lVal) != fmt.Sprintf("%v", rVal) {
					match = false
					break
				}
			}

			if match {
				matched = true
				if joinType == "full" {
					matchedRight[rIdx] = true
				}
				merged := make(map[string]any)

				if m, ok := lItem.(map[string]any); ok {
					for k, v := range m {
						merged[leftAlias+"."+k] = v
					}
				} else if om, ok := lItem.(*OrderedMap); ok && om != nil {
					for k, v := range om.m {
						merged[leftAlias+"."+k] = v
					}
				} else if om, ok := lItem.(OrderedMap); ok {
					for k, v := range om.m {
						merged[leftAlias+"."+k] = v
					}
				}

				if m, ok := rItem.(map[string]any); ok {
					for k, v := range m {
						merged[rightAlias+"."+k] = v
					}
				} else if om, ok := rItem.(*OrderedMap); ok && om != nil {
					for k, v := range om.m {
						merged[rightAlias+"."+k] = v
					}
				} else if om, ok := rItem.(OrderedMap); ok {
					for k, v := range om.m {
						merged[rightAlias+"."+k] = v
					}
				}

				results = append(results, merged)
			}
		}
		if !matched && (joinType == "left" || joinType == "full") {
			results = append(results, lItem)
		}
	}

	if joinType == "full" {
		for i, rItem := range right {
			if !matchedRight[i] {
				// Emit unmatched right item (Left is null)
				// Respect aliasing for consistency?
				merged := make(map[string]any)
				if m, ok := rItem.(map[string]any); ok {
					for k, v := range m {
						// Since "Left" is missing, we decide if we use alias or not.
						// In match case: merged[rightAlias+"."+k] = v (if collision).
						// Here, no collision. So standard is output raw k.
						// BUT if user expects "B.id", they might be disappointed.
						// Let's stick to raw k for now, consistent with "no match in Left".
						// Actually, better: if rightAlias is set explicitly, we could honor it?
						// But existing code only honors it if collision `matched[k]`.
						// So we keep raw k.
						merged[k] = v
					}
				}
				results = append(results, merged)
			}
		}
	}

	return results, nil
}

// JoinRight is a pipeline-friendly alias for Join.
// It expects the input to be the Left stream, and the 'store' argument to be the Right store.
func (e *ScriptEngine) JoinRight(ctx context.Context, input any, args map[string]any) (any, error) {
	// Map 'store' to 'with' for Join compatibility
	// Also attempt to capture alias if provided in args (e.g. from SQL FROM clause)
	if store, ok := args["store"]; ok {
		args["with"] = store
		// If we wanted to enforce alias passthrough, we assume "store" name is the default alias
		// unless overridden by "alias" key.
		// note: Join() method above looks at "with" or "variables".
	}

	// FIX: Explicitly map 'alias' to 'right_alias' so the underlying Join operation usage
	// of JoinRightCursor respects the user-defined alias.
	if alias, ok := args["alias"]; ok {
		args["right_alias"] = alias
	}

	return e.Join(ctx, input, args)
}

// handleInto handles the 'into' argument for JoinResult.
func (e *ScriptEngine) Update(ctx context.Context, input any, args map[string]any) ([]any, error) {
	var list []any
	if cursor, ok := input.(ScriptCursor); ok {
		for {
			item, ok, err := cursor.Next(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			list = append(list, item)
		}
	} else if l, ok := input.([]map[string]any); ok {
		for _, x := range l {
			list = append(list, x)
		}
	} else if l, ok := input.([]any); ok {
		list = l
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}

	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}

	return e.stageUpdate(ctx, list, store, args)
}

func (e *ScriptEngine) Delete(ctx context.Context, input any, args map[string]any) ([]any, error) {
	var list []any
	if cursor, ok := input.(ScriptCursor); ok {
		for {
			item, ok, err := cursor.Next(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			list = append(list, item)
		}
	} else if l, ok := input.([]map[string]any); ok {
		for _, x := range l {
			list = append(list, x)
		}
	} else if l, ok := input.([]any); ok {
		list = l
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}

	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}

	return e.stageDelete(ctx, list, store, args)
}

func (e *ScriptEngine) Inspect(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}

	si := store.GetStoreInfo()

	// Auto-Infer Schema from first record
	var schema map[string]string
	if ok, _ := store.First(ctx); ok {
		k := store.GetCurrentKey()
		v, _ := store.GetCurrentValue(ctx)
		flat := flattenItem(k, v)
		schema = inferSchema(flat)
	}

	return map[string]any{
		"name":                          si.Name,
		"count":                         si.Count,
		"slot_length":                   si.SlotLength,
		"is_unique":                     si.IsUnique,
		"is_value_data_in_node_segment": si.IsValueDataInNodeSegment,
		"leaf_load_balancing":           si.LeafLoadBalancing,
		"description":                   si.Description,
		"schema":                        schema,
	}, nil
}

// --- Stage Implementations (In-Memory) ---

func (e *ScriptEngine) stageFilter(input []any, args map[string]any) ([]any, error) {
	conditionRaw := args["condition"]
	if conditionRaw == nil {
		return input, nil
	}

	var output []any

	// CEL Filter
	if _, ok := conditionRaw.(string); ok {
		for _, item := range input {
			match, err := e.evaluateCondition(item, conditionRaw)
			if err == nil && match {
				output = append(output, item)
			}
		}
		return output, nil
	}

	// Simple Map Match
	if matchMap, ok := conditionRaw.(map[string]any); ok {
		for _, item := range input {
			match, err := e.evaluateCondition(item, matchMap)
			if err != nil {
				return nil, err
			}
			if match {
				output = append(output, item)
			}
		}
		return output, nil
	}

	return input, fmt.Errorf("unsupported filter condition type")
}

func (e *ScriptEngine) stageSort(input []any, args map[string]any) ([]any, error) {
	fieldsRaw, ok := args["fields"].([]any)
	if !ok {
		return input, nil
	}
	var fields []string
	for _, f := range fieldsRaw {
		if s, ok := f.(string); ok {
			fields = append(fields, s)
		}
	}

	if len(fields) == 0 {
		return input, nil
	}

	sort.Slice(input, func(i, j int) bool {
		for _, field := range fields {
			parts := strings.Fields(field)
			fieldName := parts[0]
			desc := len(parts) > 1 && strings.ToLower(parts[1]) == "desc"

			valA := getField(input[i], fieldName)
			valB := getField(input[j], fieldName)

			// Compare
			cmp := compare(valA, valB)
			if cmp != 0 {
				if desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return input, nil
}

func (e *ScriptEngine) stageProject(input []any, args map[string]any) ([]any, error) {
	// Pre-parse fields for performance
	fields := parseProjectionFields(args["fields"])

	log.Debug("stageProject called", "input_len", len(input), "input_fields", fields)

	var output []any
	for i, item := range input {
		newItem := renderItem(nil, item, fields)
		output = append(output, newItem)

		// Log sample of what we are creating
		if i == 0 {
			if om, ok := newItem.(*OrderedMap); ok {
				log.Debug("stageProject first item (OrderedMap)", "keys", om.keys)
			} else if om, ok := newItem.(OrderedMap); ok {
				log.Debug("stageProject first item (OrderedMap Value)", "keys", om.keys)
			} else {
				log.Debug("stageProject first item (Map/Other)", "type", fmt.Sprintf("%T", newItem))
			}
		}
	}

	return output, nil
}

func (e *ScriptEngine) stageLimit(input []any, args map[string]any) ([]any, error) {
	var limit int
	if l, ok := args["limit"].(float64); ok {
		limit = int(l)
	} else if l, ok := args["count"].(float64); ok {
		limit = int(l)
	} else {
		// Default or error?
		limit = 1000
	}

	log.Debug("stageLimit called", "input_len", len(input), "limit", limit)

	if limit < len(input) {
		return input[:limit], nil
	}
	return input, nil
}

func (e *ScriptEngine) stageUpdate(ctx context.Context, input []any, store jsondb.StoreAccessor, args map[string]any) ([]any, error) {
	if store == nil {
		return nil, fmt.Errorf("update stage requires an active store from a previous scan")
	}
	values, _ := args["values"].(map[string]any)
	if len(values) == 0 {
		return input, nil
	}

	for _, itemObj := range input {
		var item map[string]any
		if m, ok := itemObj.(map[string]any); ok {
			item = m
		} else if om, ok := itemObj.(*OrderedMap); ok && om != nil {
			item = om.m
		} else if om, ok := itemObj.(OrderedMap); ok {
			item = om.m
		} else {
			continue
		}

		key := item["key"]
		// Merge values
		currentVal := item["value"]
		var newVal any
		if currMap, ok := currentVal.(map[string]any); ok {
			// Clone to avoid side effects?
			newMap := make(map[string]any)
			for k, v := range currMap {
				newMap[k] = v
			}
			for k, v := range values {
				newMap[k] = v
			}
			newVal = newMap
		} else {
			newVal = values // Overwrite if not a map?
		}

		if _, err := store.Update(ctx, key, newVal); err != nil {
			return nil, err
		}
		// Update item in memory too
		item["value"] = newVal
	}
	return input, nil
}

func (e *ScriptEngine) stageDelete(ctx context.Context, input []any, store jsondb.StoreAccessor, args map[string]any) ([]any, error) {
	if store == nil {
		return nil, fmt.Errorf("delete stage requires an active store from a previous scan")
	}

	for _, itemObj := range input {
		key := getField(itemObj, "key")
		if _, err := store.Remove(ctx, key); err != nil {
			return nil, err
		}
	}
	return []any{}, nil
}

// --- Control Flow Operations ---

func (e *ScriptEngine) If(ctx context.Context, args map[string]any) error {
	conditionRaw, ok := args["condition"]
	if !ok {
		return fmt.Errorf("if operation requires 'condition'")
	}

	// Evaluate Condition
	var result bool
	if _, ok := conditionRaw.(string); ok {
		return fmt.Errorf("CEL if conditions not supported yet")
	} else if condBool, ok := conditionRaw.(bool); ok {
		result = condBool
	}

	if result {
		if thenBlock, ok := args["then"].([]any); ok {
			// Convert []any to []ScriptInstruction
			var thenScript []ScriptInstruction
			bytes, _ := json.Marshal(thenBlock)
			if err := json.Unmarshal(bytes, &thenScript); err != nil {
				return fmt.Errorf("failed to parse 'then' block: %v", err)
			}
			return e.Execute(ctx, thenScript)
		}
	} else {
		if elseBlock, ok := args["else"].([]any); ok {
			var elseScript []ScriptInstruction
			bytes, _ := json.Marshal(elseBlock)
			if err := json.Unmarshal(bytes, &elseScript); err != nil {
				return fmt.Errorf("failed to parse 'else' block: %v", err)
			}
			return e.Execute(ctx, elseScript)
		}
	}

	return nil
}

func (e *ScriptEngine) Loop(ctx context.Context, args map[string]any) error {
	// ForEach loop over a list variable
	collectionVar, _ := args["collection"].(string)
	itemVar, _ := args["item_var"].(string)
	if itemVar == "" {
		itemVar = "item"
	}

	bodyBlock, ok := args["body"].([]any)
	if !ok {
		return fmt.Errorf("loop operation requires 'body' block")
	}

	var bodyScript []ScriptInstruction
	bytes, _ := json.Marshal(bodyBlock)
	if err := json.Unmarshal(bytes, &bodyScript); err != nil {
		return fmt.Errorf("failed to parse loop 'body': %v", err)
	}

	// Get Collection
	collection, ok := e.Context.Variables[collectionVar]
	if !ok {
		return fmt.Errorf("collection variable '%s' not found", collectionVar)
	}

	// Iterate
	// Support []map[string]any or []any
	if list, ok := collection.([]map[string]any); ok {
		for _, item := range list {
			e.Context.Variables[itemVar] = item
			if err := e.Execute(ctx, bodyScript); err != nil {
				return err
			}
		}
	} else if list, ok := collection.([]any); ok {
		for _, item := range list {
			e.Context.Variables[itemVar] = item
			if err := e.Execute(ctx, bodyScript); err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("variable '%s' is not a list", collectionVar)
	}

	return nil
}

func (e *ScriptEngine) CallFunction(ctx context.Context, args map[string]any) (any, error) {
	functionName, _ := args["name"].(string)
	if functionName == "" {
		return nil, fmt.Errorf("function name required")
	}

	if e.FunctionHandler == nil {
		return nil, fmt.Errorf("function handler not configured")
	}

	// Handle Parameters (Scoped Variable Injection)
	// We save current values of variables that are being overwritten, and restore them after.
	savedVars := make(map[string]any)
	if params, ok := args["params"].(map[string]any); ok {
		for k, v := range params {
			if oldVal, exists := e.Context.Variables[k]; exists {
				savedVars[k] = oldVal
			}
			e.Context.Variables[k] = v
		}
	}

	// Call Handler
	var params map[string]any
	if p, ok := args["params"].(map[string]any); ok {
		params = p
	}
	res, err := e.FunctionHandler(ctx, functionName, params)

	// Restore Variables
	for k, v := range savedVars {
		e.Context.Variables[k] = v
	}

	return res, err
}

// --- Cursor Operations ---

func (e *ScriptEngine) First(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.First(ctx)
}

func (e *ScriptEngine) Last(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.Last(ctx)
}

func (e *ScriptEngine) Next(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.Next(ctx)
}

func (e *ScriptEngine) Previous(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.Previous(ctx)
}

func (e *ScriptEngine) Find(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	key := args["key"]
	desc, _ := args["desc"].(bool)

	if desc {
		return store.FindInDescendingOrder(ctx, key)
	}
	return store.FindOne(ctx, key, true)
}

func (e *ScriptEngine) Add(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}
	key := args["key"]
	value := args["value"]
	return store.Add(ctx, key, value)
}

func (e *ScriptEngine) GetCurrentKey(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}
	k := store.GetCurrentKey()
	return k, nil
}

func (e *ScriptEngine) GetCurrentValue(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}
	v, err := store.GetCurrentValue(ctx)
	return v, err
}

// --- List / Map Operations ---

func (e *ScriptEngine) ListAppend(ctx context.Context, args map[string]any) error {
	listVar, _ := args["list"].(string)
	item := args["item"]

	list, ok := e.Context.Variables[listVar]
	if !ok {
		return fmt.Errorf("list variable '%s' not found", listVar)
	}

	// Append
	if l, ok := list.([]any); ok {
		e.Context.Variables[listVar] = append(l, item)
	} else if l, ok := list.([]map[string]any); ok {
		// If item is map, ok.
		if m, ok := item.(map[string]any); ok {
			e.Context.Variables[listVar] = append(l, m)
		} else {
			// Convert to []any? Or fail?
			// Let's upgrade the list to []any
			newList := make([]any, len(l))
			for i, v := range l {
				newList[i] = v
			}
			e.Context.Variables[listVar] = append(newList, item)
		}
	} else {
		return fmt.Errorf("variable '%s' is not a list", listVar)
	}
	return nil
}

func (e *ScriptEngine) MapMerge(ctx context.Context, args map[string]any) (map[string]any, error) {
	map1Raw := args["map1"]
	map2Raw := args["map2"]

	result := make(map[string]any)

	if m1, ok := map1Raw.(map[string]any); ok {
		for k, v := range m1 {
			result[k] = v
		}
	}
	if m2, ok := map2Raw.(map[string]any); ok {
		for k, v := range m2 {
			result[k] = v
		}
	}
	return result, nil
}

func (a *DataAdminAgent) opCallScript(ctx context.Context, scriptCtx *ScriptContext, args map[string]any) (any, error) {
	scriptName, _ := args["name"].(string)
	if scriptName == "" {
		return nil, fmt.Errorf("script name required")
	}

	if a.systemDB == nil {
		return nil, fmt.Errorf("system database not available")
	}

	sysTx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return nil, fmt.Errorf("failed to begin system tx: %v", err)
	}
	defer sysTx.Rollback(ctx)

	funcStore, err := jsondb.OpenStore(ctx, a.systemDB.Config(), "scripts", sysTx)
	if err != nil {
		return nil, fmt.Errorf("failed to open scripts store: %v", err)
	}

	found, err := funcStore.FindOne(ctx, scriptName, true)
	if err != nil {
		return nil, fmt.Errorf("failed to find script: %v", err)
	}
	if !found {
		return nil, fmt.Errorf("script '%s' not found", scriptName)
	}

	val, err := funcStore.GetCurrentValue(ctx)
	if err != nil {
		return nil, err
	}

	funcMap, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid script format")
	}

	stepsRaw, ok := funcMap["steps"].([]any)
	if !ok {
		return nil, nil // Empty script
	}

	// Detect Script Type (Legacy vs Atomic)
	isAtomic := false
	if len(stepsRaw) > 0 {
		if firstStep, ok := stepsRaw[0].(map[string]any); ok {
			if _, hasOp := firstStep["op"]; hasOp {
				isAtomic = true
			}
		}
	}

	if isAtomic {
		// Serialize to detect changes
		bytes, err := json.Marshal(stepsRaw)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal script steps: %v", err)
		}

		sum := sha256.Sum256(bytes)
		currentHash := hex.EncodeToString(sum[:])

		// Check Cache
		a.compiledScriptsMu.RLock()
		cachedEntry, cached := a.compiledScripts[scriptName]
		a.compiledScriptsMu.RUnlock()

		var compiled CompiledScript

		if !cached || cachedEntry.Hash != currentHash {
			var script []ScriptInstruction
			if err := json.Unmarshal(bytes, &script); err != nil {
				return nil, fmt.Errorf("failed to parse script: %v", err)
			}

			compiled, err = CompileScript(script)
			if err != nil {
				return nil, fmt.Errorf("failed to compile script '%s': %v", scriptName, err)
			}

			// Update Cache
			a.compiledScriptsMu.Lock()
			a.compiledScripts[scriptName] = CachedScript{Script: compiled, Hash: currentHash}
			a.compiledScriptsMu.Unlock()
		} else {
			compiled = cachedEntry.Script
		}

		// Execute in CURRENT context (inherits variables, txs)
		// Wrap resolver to handle default database from context
		resolver := func(name string) (Database, error) {
			if name == "" {
				if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" {
					name = p.CurrentDB
				}
			}
			return a.resolveDatabase(name)
		}
		engine := NewScriptEngine(scriptCtx, resolver)
		engine.FunctionHandler = func(c context.Context, name string, args map[string]any) (any, error) {
			return a.opCallScript(c, engine.Context, map[string]any{"name": name, "params": args})
		}

		if err := compiled(ctx, engine); err != nil {
			return nil, err
		}

		// Return Result
		// Priority 1: Check for 'output' variable (Explicit override)
		if val, ok := engine.Context.Variables["output"]; ok {
			return val, nil
		}

		return nil, nil
	}

	// Legacy Tool Script
	for _, step := range stepsRaw {
		stepMap, _ := step.(map[string]any)
		toolName, _ := stepMap["tool"].(string)
		toolArgs, _ := stepMap["args"].(map[string]any)

		if a.registry == nil {
			return nil, fmt.Errorf("registry not initialized")
		}

		toolDef, ok := a.registry.Get(toolName)
		if !ok {
			return nil, fmt.Errorf("tool '%s' not found", toolName)
		}
		_, err := toolDef.Handler(ctx, toolArgs)
		if err != nil {
			return nil, fmt.Errorf("script step '%s' failed: %v", toolName, err)
		}
	}

	return nil, nil
}

// --- Helper Functions ---

func matchesMap(item any, condition map[string]any) bool {
	for k, v := range condition {
		itemVal := getField(item, k)
		fmt.Printf("DEBUG: matchesMap Check Key=%s, CondVal=%v, ItemVal=%v\n", k, v, itemVal)

		// Check for operators map
		if ops, ok := v.(map[string]any); ok {
			isOperator := false
			for op := range ops {
				if strings.HasPrefix(op, "$") {
					isOperator = true
					break
				}
			}

			if isOperator {
				for op, opVal := range ops {
					switch op {
					case "$eq":
						if compare(itemVal, opVal) != 0 {
							return false
						}
					case "$gt":
						if compare(itemVal, opVal) <= 0 {
							return false
						}
					case "$gte":
						if compare(itemVal, opVal) < 0 {
							return false
						}
					case "$lt":
						if compare(itemVal, opVal) >= 0 {
							return false
						}
					case "$lte":
						if compare(itemVal, opVal) > 0 {
							return false
						}
					case "$ne":
						if compare(itemVal, opVal) == 0 {
							return false
						}
					case "$in":
						found := false
						if list, ok := opVal.([]any); ok {
							for _, val := range list {
								if compare(itemVal, val) == 0 {
									found = true
									break
								}
							}
						}
						if !found {
							return false
						}
					case "$nin":
						if list, ok := opVal.([]any); ok {
							for _, val := range list {
								if compare(itemVal, val) == 0 {
									return false
								}
							}
						}
					}
				}
				continue
			}
		}

		// Direct Equality check
		if compare(itemVal, v) != 0 {
			return false
		}
	}
	return true
}

func getField(itemObj any, field string) any {
	var item map[string]any
	if m, ok := itemObj.(map[string]any); ok {
		item = m
	} else if om, ok := itemObj.(OrderedMap); ok {
		item = om.m
	} else if om, ok := itemObj.(*OrderedMap); ok && om != nil {
		item = om.m
	} else {
		return nil
	}

	// Support "value.age" or just "age" (implicit value)
	if v, ok := item[field]; ok {
		return v
	}

	// Case-insensitive fallback (SQL behavior)
	for k, v := range item {
		if strings.EqualFold(k, field) {
			return v
		}
	}

	// Support relaxed aliased lookup (e.g. "a.name" -> "name") provided the alias didn't match
	if idx := strings.Index(field, "."); idx != -1 {
		suffix := field[idx+1:]
		if v, ok := item[suffix]; ok {
			return v
		}
	}

	// Reverse Alias Lookup: Request "name", Match "users.name" if unique
	// This mimics SQL unqualified column resolution.
	if !strings.Contains(field, ".") {
		var candidate any
		count := 0
		suffix := "." + field
		for k, v := range item {
			if strings.HasSuffix(k, suffix) {
				candidate = v
				count++
			}
		}
		if count == 1 {
			return candidate
		}
	}

	// Check for key field (alias or direct)
	if field == "key" {
		if v, ok := item["key"]; ok {
			return v
		}
	}

	// Check inside key if it's a map (Composite Key support)
	if keyMap, ok := item["key"].(map[string]any); ok {
		if v, ok := keyMap[field]; ok {
			return v
		}
	}

	// Implicit check in "value" map
	if valMap, ok := item["value"].(map[string]any); ok {
		if v, ok := valMap[field]; ok {
			return v
		}
	}

	// Implicit check in "key" if it is a map (Composite Key)
	if keyMap, ok := item["key"].(map[string]any); ok {
		if v, ok := keyMap[field]; ok {
			return v
		}
	}

	// Helper: Reconstruct object from flattened dot-notation keys
	// e.g. Request "orders", Input has "orders.id", "orders.total" -> Return map
	reconstructed := make(map[string]any)
	prefix := field + "."
	hasReconstructed := false

	for k, v := range item {
		if strings.HasPrefix(k, prefix) {
			suffix := k[len(prefix):]
			if suffix != "" {
				reconstructed[suffix] = v
				hasReconstructed = true
			}
		}
	}
	if hasReconstructed {
		return reconstructed
	}

	return nil
}

// Helper: Compare two values
func compare(a, b any) int {
	// Handle nil
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Handle Strings
	sa, okA := a.(string)
	sb, okB := b.(string)
	if okA && okB {
		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		return 0
	}

	// Handle Numbers
	fa, okA := toFloat(a)
	fb, okB := toFloat(b)
	if okA && okB {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}

	// Fallback to string representation
	sa = fmt.Sprintf("%v", a)
	sb = fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}

func toFloat(v any) (float64, bool) {
	switch i := v.(type) {
	case int:
		return float64(i), true
	case int64:
		return float64(i), true
	case float64:
		return i, true
	case float32:
		return float64(i), true
	case int32:
		return float64(i), true
	case int16:
		return float64(i), true
	case int8:
		return float64(i), true
	case uint:
		return float64(i), true
	case uint64:
		return float64(i), true
	case uint32:
		return float64(i), true
	case uint16:
		return float64(i), true
	case uint8:
		return float64(i), true
	}
	return 0, false
}

// sanitizeScript performs a pass over the script instructions to clean up common LLM errors.
// This is the "Compiler/Linter" phase before execution.
func sanitizeScript(script []ScriptInstruction) []ScriptInstruction {
	for i := range script {
		instr := &script[i]

		// 1. PROJECT Op: Normalize "fields"
		if instr.Op == "project" {
			if fieldsRaw, ok := instr.Args["fields"]; ok {
				// Convert Maps/Sloppy lists to Clean List<ProjectionField>
				// We reuse parseProjectionFields to do the heavy lifting
				parsed := parseProjectionFields(fieldsRaw)

				// Re-serialize back to a clean list of strings (or structured objects)
				// Actually, the Engine expects "fields" to be consumable by parseProjectionFields again.
				// But we can simplify it here to ensure it's normalized.

				// Let's replace the raw args with the parsed struct slice?
				// Engine uses: parsedFields := parseProjectionFields(args["fields"])
				// So if we put []ProjectionField back into args["fields"], it works (parseProjectionFields handles it).
				instr.Args["fields"] = parsed
			}
		}

		// 2. JOIN Op: Normalize "type"
		if instr.Op == "join" || instr.Op == "join_right" {
			if t, ok := instr.Args["type"].(string); ok {
				instr.Args["type"] = strings.ToLower(strings.TrimSpace(t))
			}
			// Default to inner if missing
			if _, ok := instr.Args["type"]; !ok {
				instr.Args["type"] = "inner"
			}

			// 3. Infer Alias from Projection Usage (Heuristic)
			// If the user projects "b.name" from the result of this join (variable "b"),
			// strongly imply that they expect the join to be aliased as "b".
			if _, hasAlias := instr.Args["alias"]; !hasAlias {
				if _, hasRightAlias := instr.Args["right_alias"]; !hasRightAlias {
					// Check result var
					resultVar := instr.ResultVar
					if resultVar != "" {
						// Look ahead for usage
						for j := i + 1; j < len(script); j++ {
							future := &script[j]
							if future.Op == "project" && future.InputVar == resultVar {
								// Check fields for prefix matching resultVar
								// Note: fields might have been normalized to []ProjectionField already in step 1,
								// BUT step 1 runs on the same pass.
								// If future is ahead, it hasn't been processed by step 1 yet (loop is i, future is j).
								// So we must handle raw or parsed fields.

								var candidates []string
								if fRaw, ok := future.Args["fields"]; ok {
									p := parseProjectionFields(fRaw)
									for _, field := range p {
										candidates = append(candidates, field.Src)
									}
								}

								// Check for prefix "resultVar."
								prefix := resultVar + "."
								found := false
								for _, c := range candidates {
									if strings.HasPrefix(c, prefix) {
										found = true
										break
									}
								}

								if found {
									// INFER ALIAS
									// Inject alias into Current Instruction
									instr.Args["alias"] = resultVar
									break // Stop looking
								}
							}
						}
					}
				}
			}
		}
	}
	return script
}
