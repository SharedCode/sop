package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	log "log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/jsondb"
)

// Join Strategy Constants
const (
	StrategyUnset     = 0
	StrategyIndexSeek = 1
	StrategyInMemory  = 2
	StrategyFullScan  = 3
)

// Database interface for script execution
type Database interface {
	BeginTransaction(ctx context.Context, mode sop.TransactionMode, maxTime ...time.Duration) (sop.Transaction, error)
	Config() sop.DatabaseOptions
}

func (e *ScriptEngine) getStore(name string) (jsondb.StoreAccessor, bool) {
	if s, ok := e.Context.Stores[name]; ok {
		return s, true
	}
	for k, s := range e.Context.Stores {
		if strings.EqualFold(k, name) {
			return s, true
		}
		if s != nil && strings.EqualFold(strings.TrimSpace(s.GetStoreInfo().Name), strings.TrimSpace(name)) {
			return s, true
		}
	}
	return nil, false
}

func (e *ScriptEngine) getDatabase(name string) (Database, bool) {
	if d, ok := e.Context.Databases[name]; ok {
		return d, true
	}
	for k, d := range e.Context.Databases {
		if strings.EqualFold(k, name) {
			return d, true
		}
	}
	return nil, false
}

// ScriptInstruction represents a single operation in the script.
type ScriptInstruction struct {
	Name      string         `json:"name"`       // User-defined name for the step
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
	Deferred        []func(context.Context, *ScriptEngine) error
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

func (a *CopilotAgent) resolveDatabase(name string) (Database, error) {
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
func (a *CopilotAgent) toolExecuteScript(ctx context.Context, args map[string]any) (string, error) {
	log.Debug("toolExecuteScript: Called", "args", args)
	scriptRaw, ok := args["script"]
	if !ok {
		return "", fmt.Errorf("script argument is required")
	}

	// Parse Script first to ensure it's valid and normalized
	var rawSteps []map[string]any
	var script []ScriptInstruction

	if pStr, ok := scriptRaw.(string); ok {
		if err := json.Unmarshal([]byte(pStr), &rawSteps); err != nil {
			return "", fmt.Errorf("failed to parse script JSON: %v", err)
		}
	} else if pSlice, ok := scriptRaw.([]any); ok {
		bytes, _ := json.Marshal(pSlice)
		if err := json.Unmarshal(bytes, &rawSteps); err != nil {
			return "", fmt.Errorf("failed to parse script array: %v", err)
		}
	} else {
		return "", fmt.Errorf("script must be a JSON string or array")
	}

	for _, step := range rawSteps {
		normalizeScriptStepForCompatibility(step)

		if _, hasOp := step["op"]; !hasOp {
			if cmd, ok := step["command"].(string); ok && cmd != "" {
				step["op"] = cmd
			}
		}

		// Auto-correct flat structures: move extra fields into "args"
		argsObj, hasArgs := step["args"].(map[string]any)
		if !hasArgs {
			argsObj = make(map[string]any)
			step["args"] = argsObj
		}

		for k, v := range step {
			if k != "op" && k != "command" && k != "name" && k != "input_var" && k != "result_var" && k != "args" {
				argsObj[k] = v
			}
		}

	}

	bytes, _ := json.Marshal(rawSteps)
	if err := json.Unmarshal(bytes, &script); err != nil {
		return "", fmt.Errorf("failed to re-parse normalized script: %v", err)
	}

	if err := validateExecuteScriptPlaceholders(ctx, script); err != nil {
		return "", err
	}

	script = sanitizeScript(script)

	if a.Config.StubMode {

		bytes, err := json.MarshalIndent(script, "", "  ")
		if err != nil {
			return "", fmt.Errorf("failed to marshal script for display: %v", err)
		}

		log.Debug("toolExecuteScript called in STUB MODE", "script", string(bytes))
		return fmt.Sprintf("Script prepared successfully (STUBBED). execution skipped.\nGenerated Script:\n```json\n%s\n```", string(bytes)), nil
	}

	// Initialize Engine
	var scriptCtx *ScriptContext

	scriptCtx = getOrInitScriptContext(ctx)

	if scriptCtx.Variables != nil {
		delete(scriptCtx.Variables, "output")
		delete(scriptCtx.Variables, "final_result")
		delete(scriptCtx.Variables, "result")
	}

	scriptCtx.LastUpdatedVar = ""

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

		return a.opCallScript(c, engine.Context, map[string]any{"name": name, "params": args})
	}

	compiled, err := engine.Compile(script)
	if err != nil {
		return "", fmt.Errorf("compilation failed: %v", err)
	}
	if err := compiled(ctx, engine); err != nil {
		return "", err
	}

	log.Debug("toolExecuteScript: Checking for return value candidates...")
	choice := selectExecuteScriptReturn(engine, scriptCtx, script)
	log.Debug("toolExecuteScript: Selected return value",
		"source", choice.Source,
		"detail", choice.Detail,
		"type", fmt.Sprintf("%T", choice.Value),
		"last_updated_var", scriptCtx.LastUpdatedVar,
	)
	if choice.SuccessMessage != "" {
		return choice.SuccessMessage, nil
	}
	return serializeResult(ctx, choice.Value)
}

type executeScriptReturnChoice struct {
	Source         string
	Detail         string
	Value          any
	SuccessMessage string
}

func selectExecuteScriptReturn(engine *ScriptEngine, scriptCtx *ScriptContext, script []ScriptInstruction) executeScriptReturnChoice {
	if val, ok := scriptCtx.Variables["output"]; ok && val != nil {
		return executeScriptReturnChoice{Source: "output", Value: val}
	}

	if val, ok := scriptCtx.Variables["final_result"]; ok && val != nil {
		return executeScriptReturnChoice{Source: "final_result", Value: val}
	}

	if val, ok := scriptCtx.Variables["result"]; ok && val != nil {
		return executeScriptReturnChoice{Source: "result", Value: val}
	}

	if len(script) > 0 {
		lastInstr := script[len(script)-1]

		if lastInstr.Op == "return" {
			if engine.ReturnValue != nil {
				return executeScriptReturnChoice{Source: "return_value", Value: engine.ReturnValue}
			}
			if engine.LastResult == nil {
				return executeScriptReturnChoice{Source: "success_message", Detail: "explicit_return_nil", SuccessMessage: "Script executed successfully."}
			}
			return executeScriptReturnChoice{Source: "return_last_result", Value: engine.LastResult}
		}

		if lastInstr.ResultVar != "" {
			if val, ok := scriptCtx.Variables[lastInstr.ResultVar]; ok && val != nil {
				return executeScriptReturnChoice{Source: "last_instruction_result_var", Detail: lastInstr.ResultVar, Value: val}
			}
		}
	}

	if scriptCtx.LastUpdatedVar != "" {
		if val, ok := scriptCtx.Variables[scriptCtx.LastUpdatedVar]; ok && val != nil {
			if !isInternalScriptHandle(val) {
				return executeScriptReturnChoice{Source: "last_updated_var", Detail: scriptCtx.LastUpdatedVar, Value: val}
			}
		}
	}

	if engine.LastResult != nil {
		return executeScriptReturnChoice{Source: "last_result", Value: engine.LastResult}
	}

	return executeScriptReturnChoice{Source: "success_message", Detail: "no_result", SuccessMessage: "Script executed successfully."}
}

func validateExecuteScriptPlaceholders(ctx context.Context, script []ScriptInstruction) error {
	var currentQuery string
	if p := ai.GetSessionPayload(ctx); p != nil {
		currentQuery = strings.ToLower(strings.TrimSpace(p.CurrentUserQuery))
	}

	for _, instr := range script {
		if (instr.Op == "join" || instr.Op == "join_right") && instr.Args != nil {
			if onMap, ok := instr.Args["on"].(map[string]any); ok {
				for leftField, rightField := range onMap {
					if placeholder, ok := rightField.(bool); ok {
						return newExecuteScriptValidationError(
							"invalid_join_on_placeholder",
							fmt.Sprintf("invalid type for join.on[%q]: got boolean placeholder %t; expected a field path string such as %q", leftField, placeholder, "key"),
							fmt.Sprintf(`{"op":"%s","args":{"store":"users_orders","on":{"%s":"key"}}}`, instr.Op, leftField),
						)
					}
				}
			}
		}

		if instr.Op == "filter" && instr.Args != nil {
			if condition, ok := instr.Args["condition"].(map[string]any); ok {
				if err := validateFilterConditionPlaceholders(condition, currentQuery); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func validateFilterConditionPlaceholders(condition map[string]any, currentQuery string) error {
	for field, raw := range condition {
		if strings.HasPrefix(field, "$") {
			switch nested := raw.(type) {
			case []any:
				for _, item := range nested {
					if nestedMap, ok := item.(map[string]any); ok {
						if err := validateFilterConditionPlaceholders(nestedMap, currentQuery); err != nil {
							return err
						}
					}
				}
			case map[string]any:
				if err := validateFilterConditionPlaceholders(nested, currentQuery); err != nil {
					return err
				}
			}
			continue
		}

		if placeholder, ok := raw.(bool); ok {
			if isLikelyBooleanFieldName(field) {
				continue
			}
			queryHint := ""
			if currentQuery != "" {
				queryHint = fmt.Sprintf(" for current query %q", currentQuery)
			}
			return newExecuteScriptValidationError(
				"invalid_filter_placeholder",
				fmt.Sprintf("invalid type for filter condition field %q: got boolean placeholder %t; expected an operator/value predicate such as {\"$eq\": value}%s", field, placeholder, queryHint),
				fmt.Sprintf(`{"op":"filter","args":{"condition":{"%s":{"$eq":"<value>"}}}}`, field),
			)
		}

		// CEL remains supported when condition itself is a string expression.
		// This only rejects malformed map-based AST placeholders like {"field":"$eq"}.
		if placeholder, ok := raw.(string); ok {
			trimmed := strings.TrimSpace(strings.Trim(placeholder, "\"'"))
			if strings.HasPrefix(trimmed, "$") && !strings.Contains(trimmed, ":") {
				queryHint := ""
				if currentQuery != "" {
					queryHint = fmt.Sprintf(" for current query %q", currentQuery)
				}
				return newExecuteScriptValidationError(
					"invalid_filter_operator_placeholder",
					fmt.Sprintf("invalid filter condition field %q: got operator placeholder %q without a comparison value; expected a predicate object such as {\"%s\": value}%s", field, trimmed, trimmed, queryHint),
					fmt.Sprintf(`{"op":"filter","args":{"condition":{"%s":{"%s":"<value>"}}}}`, field, trimmed),
				)
			}
		}

		if nested, ok := raw.(map[string]any); ok {
			if err := validateFilterConditionPlaceholders(nested, currentQuery); err != nil {
				return err
			}
		}
	}

	return nil
}

type executeScriptValidationError struct {
	Category string
	Message  string
	Example  string
}

func (e *executeScriptValidationError) Error() string {
	if e == nil {
		return ""
	}
	if e.Example == "" {
		return fmt.Sprintf("execute_script validation error [%s]: %s", e.Category, e.Message)
	}
	return fmt.Sprintf("execute_script validation error [%s]: %s Example fix: %s", e.Category, e.Message, e.Example)
}

func newExecuteScriptValidationError(category, message, example string) error {
	return &executeScriptValidationError{
		Category: category,
		Message:  message,
		Example:  example,
	}
}

func isLikelyBooleanFieldName(field string) bool {
	field = strings.ToLower(strings.TrimSpace(field))
	field = strings.TrimPrefix(field, "!")
	if idx := strings.LastIndex(field, "."); idx >= 0 {
		field = field[idx+1:]
	}

	if strings.HasPrefix(field, "is_") || strings.HasPrefix(field, "has_") || strings.HasPrefix(field, "can_") || strings.HasPrefix(field, "should_") {
		return true
	}

	switch field {
	case "active", "enabled", "disabled", "deleted", "archived", "visible", "public", "private", "verified", "locked", "done", "complete", "completed":
		return true
	default:
		return false
	}
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
	type compiledStep struct {
		Name      string
		Op        string
		Args      map[string]any
		InputVar  string
		ResultVar string
		Func      func(context.Context, *ScriptEngine) error
	}
	var steps []compiledStep

	for _, instr := range script {

		instr := instr

		argResolvers := make(map[string]func(*ScriptEngine) any)
		for k, v := range instr.Args {
			val := v
			if str, ok := val.(string); ok && strings.Contains(str, "{{") {

				argResolvers[k] = func(e *ScriptEngine) any {
					return e.resolveTemplate(str)
				}
			} else {
				argResolvers[k] = func(_ *ScriptEngine) any { return val }
			}
		}

		opFunc, err := bindOperation(instr.Op)
		if err != nil {
			return nil, fmt.Errorf("instruction '%s': %v", instr.Op, err)
		}

		stepFn := func(ctx context.Context, e *ScriptEngine) error {

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

				input = e.LastResult
			}

			result, err := opFunc(ctx, e, args, input)
			if err != nil {
				return fmt.Errorf("operation '%s' failed: %v", instr.Op, err)
			}

			if result != nil || !preserveLastResultOnNil(instr.Op) {
				e.LastResult = result
			}

			if instr.ResultVar != "" {
				e.Context.Variables[instr.ResultVar] = result
				e.Context.LastUpdatedVar = instr.ResultVar

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
		steps = append(steps, compiledStep{
			Name:      instr.Name,
			Op:        instr.Op,
			Args:      instr.Args,
			InputVar:  instr.InputVar,
			ResultVar: instr.ResultVar,
			Func:      stepFn,
		})
	}

	return func(ctx context.Context, e *ScriptEngine) error {

		defer func() {
			if attachDeferredCleanupCursor(ctx, e) {
				return
			}

			for len(e.Deferred) > 0 {
				i := len(e.Deferred) - 1
				task := e.Deferred[i]
				e.Deferred = e.Deferred[:i]

				log.Debug("Executing deferred operation", "index", i)
				if err := task(ctx, e); err != nil {
					log.Error("Deferred execution failed", "error", err)
				}
			}
		}()

		for i, step := range steps {
			if e.HasReturned {
				log.Debug("Script returned early", "step", i)
				break
			}
			log.Debug("ExecuteStep", "step", i+1, "op", step.Op)
			log.Info("Script Engine Tool Call",
				"step", i+1,
				"op", step.Op,
				"name", step.Name,
				"input_var", step.InputVar,
				"result_var", step.ResultVar,
				"arg_keys", summarizeScriptArgKeys(step.Args),
			)

			// Streaming Setup
			var stepStreamer interface {
				WriteItem(any)
				Close()
			}

			if streamer, ok := ctx.Value(CtxKeyJSONStreamer).(*JSONStreamer); ok {
				isVerbose, _ := ctx.Value("verbose").(bool)
				suppressInternalStepStart, _ := ctx.Value(CtxKeySuppressInternalStepStart).(bool)

				isSystemOp := false
				switch step.Op {
				case "open_db", "begin_tx", "commit_tx", "rollback_tx", "open_store":
					isSystemOp = true
				}

				if isVerbose && !isSystemOp && !suppressInternalStepStart {

					streamer.SetSuppressStepStart(false)

					displayName := step.Op
					if step.Name != "" {
						displayName = step.Name
					} else if step.Op == "execute_script" {
						if n, ok := step.Args["name"].(string); ok && n != "" {
							displayName = n
						}
					}

					stepStreamer = streamer.StartStreamingStep("step_start", displayName, "", i+1)
				}
			}

			if err := step.Func(ctx, e); err != nil {
				log.Debug("ExecuteStep failed", "step", i+1, "err", err)
				if stepStreamer != nil {
					stepStreamer.Close()
				}
				return err
			}

			log.Debug("ExecuteStep success", "step", i+1, "lastResultType", fmt.Sprintf("%T", e.LastResult))

			if stepStreamer != nil {
				result := e.LastResult

				if _, ok := result.(ScriptCursor); !ok {

					if list, ok := result.([]any); ok {
						for _, item := range list {
							stepStreamer.WriteItem(item)
						}
					} else if list, ok := result.([]map[string]any); ok {
						for _, item := range list {
							stepStreamer.WriteItem(item)
						}
					} else if result != nil {

						stepStreamer.WriteItem(result)
					}
				}
				stepStreamer.Close()
			}
		}
		return nil
	}, nil
}

func attachDeferredCleanupCursor(ctx context.Context, e *ScriptEngine) bool {
	if len(e.Deferred) == 0 {
		return false
	}

	var cursor ScriptCursor
	if sc, ok := e.ReturnValue.(ScriptCursor); ok {
		cursor = sc
	} else if sc, ok := e.LastResult.(ScriptCursor); ok {
		cursor = sc
	} else if sc, ok := e.Context.Variables["output"].(ScriptCursor); ok {
		cursor = sc
	} else if sc, ok := e.Context.Variables["final_result"].(ScriptCursor); ok {
		cursor = sc
	} else if sc, ok := e.Context.Variables["result"].(ScriptCursor); ok {
		cursor = sc
	} else if e.Context.LastUpdatedVar != "" {
		if sc, ok := e.Context.Variables[e.Context.LastUpdatedVar].(ScriptCursor); ok {
			cursor = sc
		}
	}

	if cursor == nil {
		return false
	}

	log.Debug("Transferring deferred cleanup to returned cursor")
	wrapper := &DeferredCleanupCursor{
		source:  cursor,
		cleanup: e.Deferred,
		ctx:     ctx,
		engine:  e,
	}

	if current, ok := e.ReturnValue.(ScriptCursor); ok && current == cursor {
		e.ReturnValue = wrapper
	}
	if current, ok := e.LastResult.(ScriptCursor); ok && current == cursor {
		e.LastResult = wrapper
	}
	if current, ok := e.Context.Variables["output"].(ScriptCursor); ok && current == cursor {
		e.Context.Variables["output"] = wrapper
	}
	if current, ok := e.Context.Variables["final_result"].(ScriptCursor); ok && current == cursor {
		e.Context.Variables["final_result"] = wrapper
	}
	if current, ok := e.Context.Variables["result"].(ScriptCursor); ok && current == cursor {
		e.Context.Variables["result"] = wrapper
	}
	if e.Context.LastUpdatedVar != "" {
		if current, ok := e.Context.Variables[e.Context.LastUpdatedVar].(ScriptCursor); ok && current == cursor {
			e.Context.Variables[e.Context.LastUpdatedVar] = wrapper
		}
	}

	e.Deferred = nil
	return true
}

func (e *ScriptEngine) resolveTemplate(tmpl string) any {

	trimmed := strings.TrimSuffix(strings.TrimPrefix(tmpl, "{{"), "}}")
	parts := strings.Split(trimmed, ".")

	var current any
	var ok bool

	if len(parts) > 0 {
		current, ok = e.Context.Variables[parts[0]]
		if !ok {
			log.Debug("resolveTemplate var not found", "var", parts[0])
			return tmpl
		}
	}

	for _, part := range parts[1:] {
		if currentMap, ok := current.(map[string]any); ok {
			current, ok = currentMap[part]
			if !ok {

				if valMap, ok := currentMap["value"].(map[string]any); ok {
					current, ok = valMap[part]
				}
			}
			if !ok {
				log.Debug("resolveTemplate field not found", "field", part, "map", currentMap)
				return nil
			}
		} else {
			log.Debug("resolveTemplate current is not a map", "current", current)
			return nil
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
	case "scan", "select":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.Scan(ctx, args, input)
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
	case "defer":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			// Extract command to defer
			// We support simplified syntax where args IS the command, or args contains "command"
			var cmdToDefer map[string]any

			if cmd, ok := args["command"].(map[string]any); ok {
				cmdToDefer = cmd
			} else if cmd, ok := args["execute"].(map[string]any); ok {
				cmdToDefer = cmd
			} else {

				if _, ok := args["op"].(string); ok {
					cmdToDefer = args
				} else {
					return nil, fmt.Errorf("defer requires 'op' or nested 'command'")
				}
			}

			script := []ScriptInstruction{
				{
					Op:   cmdToDefer["op"].(string),
					Args: cmdToDefer,
				},
			}

			compiledCmd, err := CompileScript(script)
			if err != nil {
				return nil, fmt.Errorf("failed to compile deferred command: %v", err)
			}

			e.Deferred = append(e.Deferred, func(c context.Context, se *ScriptEngine) error {

				wasReturned := se.HasReturned
				se.HasReturned = false
				defer func() { se.HasReturned = wasReturned }()

				return compiledCmd(c, se)
			})

			return nil, nil
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
	case "call_script", "script":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.CallScript(ctx, args)
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
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.ListAppend(ctx, input, args)
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
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			if val, ok := args["value"]; ok {

				// Helper to resolve a value (string or structure)
				var resolve func(v any) any
				resolve = func(v any) any {
					if strVal, ok := v.(string); ok && e.Context != nil && e.Context.Variables != nil {
						varName := e.resolveVarName(strVal)
						if res, found := e.Context.Variables[varName]; found {
							return res
						}
					}
					if sliceVal, ok := v.([]any); ok {
						newSlice := make([]any, len(sliceVal))
						for i, item := range sliceVal {
							newSlice[i] = resolve(item)
						}
						return newSlice
					}

					return v
				}

				res := resolve(val)
				e.ReturnValue = res
				e.HasReturned = true
				return res, nil
			}

			if input != nil {
				e.ReturnValue = input
				e.HasReturned = true
				return input, nil
			}

			return nil, nil
		}, nil
	case "search_space", "upsert_space_items", "delete_space_categories", "delete_space_items", "vectorize_space", "vectorize_space_categories", "vectorize_space_items", "list_space_categories", "list_space_items":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.ExecuteKBManagement(ctx, op, args, input)
		}, nil
	default:
		return nil, fmt.Errorf("unknown operation: %s", op)
	}
}

func summarizeScriptArgKeys(args map[string]any) string {
	if len(args) == 0 {
		return ""
	}
	keys := make([]string, 0, len(args))
	for key := range args {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func serializeResult(ctx context.Context, val any) (string, error) {

	if _, ok := val.(Database); ok {
		return "Database object (success)", nil
	}
	if _, ok := val.(sop.Transaction); ok {
		return "Transaction object (success)", nil
	}
	if _, ok := val.(jsondb.StoreAccessor); ok {
		return "Store object (success)", nil
	}

	alreadyCollapsed := false

	if cursor, ok := val.(ScriptCursor); ok {
		results := make([]any, 0)
		defer cursor.Close()

		// Helper to force field order if provider available
		var orderedFields []string
		if provider, ok := cursor.(OrderedFieldsProvider); ok {
			orderedFields = provider.GetOrderedFields()
		}

		// Setup Streaming for Final Result (Cursor)
		var resultStreamer interface {
			WriteItem(any)
			Close()
			SetMetadata(map[string]any)
		}

		if streamer, ok := ctx.Value(CtxKeyJSONStreamer).(*JSONStreamer); ok && streamer != nil {

			streamer.SetSuppressStepStart(true)
			resultStreamer = streamer.StartStreamingStep("result_stream", "final_result", "", 0)

			if len(orderedFields) > 0 {
				cols := make(map[string]any)
				cols["columns"] = orderedFields
				resultStreamer.SetMetadata(cols)
			}
		}

		for {
			itemObj, ok, err := cursor.Next(ctx)
			if err != nil {
				return "", fmt.Errorf("failed to read cursor: %v", err)
			}
			if !ok {
				break
			}
			if itemObj == nil {
				continue
			}

			var itemMap map[string]any

			isOrdered := false
			if om, ok := itemObj.(*OrderedMap); ok {
				if !om.isImplicit {
					isOrdered = true
				} else {
					itemMap = om.m
				}
			} else if om, ok := itemObj.(OrderedMap); ok {
				if !om.isImplicit {
					isOrdered = true
				} else {
					itemMap = om.m
				}
			}

			if !isOrdered {
				if m, ok := itemObj.(map[string]any); ok {
					itemMap = m
				}
			} else {

				itemMap = nil
			}

			if itemMap != nil {

				itemObj = collapseUniqueKeys(itemMap)
			}

			var item any = itemObj

			if itemMap != nil && len(orderedFields) > 0 {
				item = filterFields(itemMap, orderedFields)
			}

			if resultStreamer != nil {
				resultStreamer.WriteItem(item)
			}

			results = append(results, item)
		}

		if resultStreamer != nil {
			resultStreamer.Close()
		}

		val = results
		alreadyCollapsed = true
	}

	if !alreadyCollapsed {
		// Friendly Output: Collapse unique keys for cleaner JSON
		var list []any
		if l, ok := val.([]any); ok {
			list = l
		} else if lMap, ok := val.([]map[string]any); ok {
			list = make([]any, len(lMap))
			for i, v := range lMap {
				list[i] = v
			}
			val = list
		}

		if list != nil {
			newList := make([]any, 0, len(list))
			for _, itemObj := range list {
				if itemObj == nil {
					continue
				}

				var itemMap map[string]any

				isOrdered := false
				if om, ok := itemObj.(*OrderedMap); ok {
					if !om.isImplicit {
						isOrdered = true
					} else {
						itemMap = om.m
					}
				} else if om, ok := itemObj.(OrderedMap); ok {
					if !om.isImplicit {
						isOrdered = true
					} else {
						itemMap = om.m
					}
				}

				if !isOrdered {
					if itemMap == nil {
						if m, ok := itemObj.(map[string]any); ok {
							itemMap = m
						}
					}
				}

				finalItem := itemObj
				if itemMap != nil {

					collapsed := collapseUniqueKeys(itemMap)

					finalItem = collapsed
				}
				newList = append(newList, finalItem)
			}
			val = newList
		}
	}

	bytes, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to serialize result: %v", err)
	}
	resStr := string(bytes)

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
	case "scan", "select":
		result, err = e.Scan(ctx, instr.Args, input)
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

	case "list_new":
		result = make([]any, 0)
	case "list_append":
		result, err = e.ListAppend(ctx, input, instr.Args)
	case "map_merge":
		result, err = e.MapMerge(ctx, instr.Args)

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

func normalizeScriptStepForCompatibility(step map[string]any) {
	op, _ := step["op"].(string)
	if op == "" {
		if cmd, ok := step["command"].(string); ok {
			op = cmd
		}
	}

	argsObj, hasArgs := step["args"].(map[string]any)
	if !hasArgs {
		argsObj = make(map[string]any)
		step["args"] = argsObj
	}

	switch {
	case strings.EqualFold(op, "sort"):
		normalizeCompatibilitySortStep(step, argsObj)
	case strings.EqualFold(op, "filter"):
		if condition, ok := argsObj["condition"].(map[string]any); ok {
			argsObj["condition"] = normalizeCompatibilityConditionMap(condition)
		}
	case strings.EqualFold(op, "join"), strings.EqualFold(op, "join_right"):
		if onMap, ok := argsObj["on"].(map[string]any); ok {
			argsObj["on"] = normalizeCompatibilityJoinOn(onMap)
		}
	}
}

func normalizeCompatibilitySortStep(step map[string]any, argsObj map[string]any) {
	if step == nil || argsObj == nil {
		return
	}

	if inputVar, _ := step["input_var"].(string); inputVar == "" {
		if pipe, ok := argsObj["pipe"].(string); ok && strings.TrimSpace(pipe) != "" {
			step["input_var"] = strings.TrimSpace(pipe)
			delete(argsObj, "pipe")
		}
	}

	if _, hasFields := argsObj["fields"]; hasFields {
		return
	}

	key, _ := argsObj["key"].(string)
	if strings.TrimSpace(key) == "" {
		key, _ = argsObj["field"].(string)
	}
	if strings.TrimSpace(key) == "" {
		return
	}
	field := strings.TrimSpace(key)
	desc, _ := argsObj["desc"].(bool)
	if !desc {
		desc, _ = argsObj["descending"].(bool)
	}
	if desc {
		field += " desc"
	}
	argsObj["fields"] = []any{field}
	delete(argsObj, "key")
	delete(argsObj, "field")
	delete(argsObj, "desc")
	delete(argsObj, "descending")
}

func normalizeCompatibilityConditionMap(condition map[string]any) map[string]any {
	normalized := make(map[string]any, len(condition))
	for field, raw := range condition {
		field = normalizeCompatibilityFieldPath(field)
		if strings.HasPrefix(strings.TrimSpace(field), "$") {
			normalized[field] = raw
			continue
		}

		if nested, ok := raw.(map[string]any); ok {
			normalized[field] = normalizeCompatibilityConditionMap(nested)
			continue
		}

		if rawStr, ok := raw.(string); ok {
			newField, newValue, changed := normalizeCompatibilityConditionEntry(field, rawStr)
			if changed {
				normalized[newField] = newValue
				continue
			}
		}

		normalized[field] = raw
	}
	return normalized
}

func normalizeCompatibilityConditionEntry(fieldHint string, raw string) (string, any, bool) {
	raw = strings.TrimSpace(strings.Trim(raw, "\"'"))
	fieldHint = strings.TrimSpace(strings.Trim(fieldHint, "\"'"))
	if raw == "" || fieldHint == "" {
		return fieldHint, raw, false
	}

	if strings.HasPrefix(raw, "$") {
		parts := strings.SplitN(raw, ":", 2)
		if len(parts) == 2 {
			op := strings.TrimSpace(parts[0])
			value := parseCompatibilityLiteral(parts[1])
			if op != "" {
				return fieldHint, map[string]any{op: value}, true
			}
		}
	}

	re := regexp.MustCompile(`^([a-zA-Z0-9_.]+)\s*(==|=|!=|>=|<=|>|<)\s*(.+)$`)
	if parts := re.FindStringSubmatch(raw); len(parts) == 4 {
		field := strings.TrimSpace(parts[1])
		op := strings.TrimSpace(parts[2])
		value := parseCompatibilityLiteral(parts[3])
		if field != "" {
			if !strings.Contains(field, ".") && isAliasPlaceholderField(fieldHint) {
				field = fieldHint + "." + field
			}
			return field, map[string]any{comparisonOperatorToAST(op): value}, true
		}
	}

	return fieldHint, raw, false
}

func normalizeCompatibilityJoinOn(onMap map[string]any) map[string]any {
	normalized := make(map[string]any, len(onMap))
	for left, raw := range onMap {
		left = normalizeCompatibilityFieldPath(left)
		if rawStr, ok := raw.(string); ok {
			rawStr = strings.TrimSpace(strings.Trim(rawStr, "\"'"))
			if strings.Contains(rawStr, "=") {
				parts := strings.SplitN(rawStr, "=", 2)
				lhs := strings.TrimSpace(parts[0])
				rhs := strings.TrimSpace(parts[1])
				if lhs != "" && rhs != "" {
					if !strings.Contains(lhs, ".") && isAliasPlaceholderField(left) {
						lhs = left + "." + lhs
					}
					normalized[lhs] = rhs
					continue
				}
			}
		}
		normalized[left] = raw
	}
	return normalized
}

func normalizeCompatibilityFieldPath(field string) string {
	field = strings.TrimSpace(strings.Trim(field, "\"'"))
	if field == "" || strings.Contains(field, ".") || !strings.Contains(field, "_") {
		return field
	}

	aliases := []string{"users_orders", "users", "orders", "products", "customers", "items", "payments", "invoices", "details"}
	for _, alias := range aliases {
		prefix := alias + "_"
		if strings.HasPrefix(field, prefix) {
			remainder := strings.TrimPrefix(field, prefix)
			if remainder != "" {
				return alias + "." + remainder
			}
		}
	}

	return field
}

func comparisonOperatorToAST(op string) string {
	switch strings.TrimSpace(op) {
	case "=", "==":
		return "$eq"
	case "!=":
		return "$ne"
	case ">":
		return "$gt"
	case ">=":
		return "$gte"
	case "<":
		return "$lt"
	case "<=":
		return "$lte"
	default:
		return "$eq"
	}
}

func parseCompatibilityLiteral(raw string) any {
	raw = strings.TrimSpace(strings.Trim(raw, "\"'"))
	if raw == "" {
		return ""
	}
	if b, err := strconv.ParseBool(raw); err == nil {
		return b
	}
	if i, err := strconv.Atoi(raw); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f
	}
	return raw
}

func isAliasPlaceholderField(field string) bool {
	field = strings.ToLower(strings.TrimSpace(field))
	if field == "" || strings.Contains(field, ".") {
		return false
	}
	switch field {
	case "users", "orders", "users_orders", "products", "customers", "items", "payments", "invoices", "details":
		return true
	default:
		return strings.HasPrefix(field, "store_") || strings.HasPrefix(field, "s_")
	}
}

func preserveLastResultOnNil(op string) bool {
	switch strings.ToLower(strings.TrimSpace(op)) {
	case "commit_tx", "rollback_tx":
		return true
	default:
		return false
	}
}

func isInternalScriptHandle(v any) bool {
	if v == nil {
		return false
	}
	switch v.(type) {
	case Database, sop.Transaction, jsondb.StoreAccessor:
		return true
	default:
		return false
	}
}

// resolveVarName strips the optional '@' or '$' prefix from a variable name.
func (e *ScriptEngine) resolveVarName(name string) string {
	name = strings.TrimPrefix(name, "@")
	return strings.TrimPrefix(name, "$")
}

func normalizeOpenDBName(args map[string]any) string {
	for _, key := range []string{"name", "database", "db", "db_name", "database_name", "current_db", "currentDatabase"} {
		if value, ok := args[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return "current"
}

func (e *ScriptEngine) OpenDB(args map[string]any) (Database, error) {
	name := normalizeOpenDBName(args)
	if e.ResolveDatabase == nil {
		return nil, fmt.Errorf("database resolver not configured")
	}
	return e.ResolveDatabase(name)
}

func (e *ScriptEngine) BeginTx(ctx context.Context, args map[string]any) (sop.Transaction, error) {
	dbName, _ := args["database"].(string)
	dbName = e.resolveVarName(dbName)
	modeStr, _ := args["mode"].(string)

	var db Database
	if dbObj, ok := e.getDatabase(dbName); ok {
		db = dbObj
	} else {

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

	drain := func(name string, cursor ScriptCursor) error {
		results := make([]any, 0)

		// Helper to wrap fields if spec available
		var specs map[string]*jsondb.IndexSpecification
		if provider, ok := cursor.(SpecProvider); ok {
			specs = provider.GetIndexSpecs()
		}

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

	if _, ok := e.Context.Variables["output"]; !ok && e.LastResult != nil && !isInternalScriptHandle(e.LastResult) {
		e.Context.Variables["output"] = e.LastResult
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
	if storeName == "" {
		storeName, _ = args["store"].(string)
	}
	if storeName == "" {
		storeName, _ = args["store_name"].(string)
	}

	var tx sop.Transaction
	var ok bool

	if txName == "" {

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
		db, found = e.getDatabase(dbName)
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

		if associatedDB, found := e.Context.TxToDB[tx]; found {
			db = associatedDB
		} else if len(e.Context.Databases) == 1 {

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

		return e.StoreOpener(ctx, db.Config(), storeName, tx)
	}

	if create {

		return jsondb.CreateObjectStore(ctx, db.Config(), storeName, tx)
	}
	return jsondb.OpenStore(ctx, db.Config(), storeName, tx)
}

func (e *ScriptEngine) Scan(ctx context.Context, args map[string]any, input any) (any, error) {

	storeVarName, _ := args["store"].(string)
	storeVarName = e.resolveVarName(storeVarName)

	var store jsondb.StoreAccessor
	var ok bool

	if inputStore, isStore := input.(jsondb.StoreAccessor); isStore && storeVarName == "" {
		store = inputStore
		ok = true
	} else if inputStr, isStr := input.(string); isStr && storeVarName == "" {
		storeVarName = inputStr
		store, ok = e.getStore(storeVarName)
	} else {
		store, ok = e.getStore(storeVarName)
	}

	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeVarName)
	}

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
	dirLower := strings.ToLower(direction)
	isDesc := dirLower == "desc" || dirLower == "descending"
	startKey := args["start_key"]
	prefix := args["prefix"]
	filter := args["filter"]

	stream, _ := args["stream"].(bool)

	var okIter bool
	var err error
	var indexSpec *jsondb.IndexSpecification
	info = store.GetStoreInfo()
	if info.MapKeyIndexSpecification != "" {
		var spec jsondb.IndexSpecification
		if err := json.Unmarshal([]byte(info.MapKeyIndexSpecification), &spec); err == nil {
			indexSpec = &spec
		}
	}

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

	if !okIter {
		if stream {

			return &StoreCursor{
				store:     store,
				storeName: storeName,
				indexSpec: indexSpec,
				ctx:       ctx,
				limit:     int(limit),
				started:   true,
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
			storeName: storeName,
			indexSpec: indexSpec,
			ctx:       ctx,
			limit:     int(limit),
			filter:    filterMap,
			engine:    e,
			isDesc:    isDesc,
			prefix:    prefix,
			started:   false,
		}, nil
	}

	results := make([]map[string]any, 0)
	count := 0
	for okIter && count < int(limit) {
		k := store.GetCurrentKey()
		v, _ := store.GetCurrentValue(ctx)

		if prefix != nil {
			if kStr, isStr := k.(string); isStr {
				pStr := fmt.Sprintf("%v", prefix)
				if !strings.HasPrefix(kStr, pStr) {
					break
				}
			}
		}

		itemAny := renderItem(k, v, nil)

		item, _ := itemAny.(map[string]any)

		if filter != nil {

			match, err := e.evaluateCondition(item, filter.(map[string]any))
			if err != nil {

				return nil, fmt.Errorf("filter evaluation failed: %v", err)
			}
			if !match {

				if isDesc {
					okIter, _ = store.Previous(ctx)
				} else {
					okIter, _ = store.Next(ctx)
				}
				continue
			}
		}

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

func TranslateASTToCEL(ast any) (string, bool) {
	switch v := ast.(type) {
	case string:
		return fmt.Sprintf("%q", v), true
	case float64, int, bool:
		return fmt.Sprintf("%v", v), true
	case map[string]any:
		if varName, ok := v["var"].(string); ok {
			if !strings.Contains(varName, ".") {
				return "value." + varName, true
			}
			return varName, true
		}
		for op, argsRAW := range v {
			if args, ok := argsRAW.([]any); ok {
				if len(args) == 2 {
					switch op {
					case "==", "!=", ">", ">=", "<", "<=":
						left, leftOk := TranslateASTToCEL(args[0])
						right, rightOk := TranslateASTToCEL(args[1])
						if leftOk && rightOk {
							return fmt.Sprintf("(%s %s %s)", left, op, right), true
						}
					}
				}
				if op == "and" || op == "or" || op == "&&" || op == "||" {
					opStr := op
					if op == "and" {
						opStr = "&&"
					}
					if op == "or" {
						opStr = "||"
					}
					var parts []string
					for _, arg := range args {
						part, ok := TranslateASTToCEL(arg)
						if ok {
							parts = append(parts, part)
						}
					}
					if len(parts) > 0 {
						return fmt.Sprintf("(%s)", strings.Join(parts, " "+opStr+" ")), true
					}
				}
			}
		}
	}
	return "", false
}

func (e *ScriptEngine) evaluateCondition(item any, condition any) (bool, error) {

	// Automatically translate map-based proprietary AST to CEL string expression natively.
	if _, ok := condition.(map[string]any); ok {
		if celStr, ok := TranslateASTToCEL(condition); ok {
			condition = celStr
		}
	}

	if expr, ok := condition.(string); ok {
		// Extract variables dynamically to satisfy CEL strict compilation
		identRegex := regexp.MustCompile(`[a-zA-Z_][a-zA-Z0-9_]*`)
		matches := identRegex.FindAllString(expr, -1)

		var celVars []cel.EnvOption
		declared := map[string]bool{
			"key": true, "value": true, "item": true, "true": true, "false": true, "null": true,
		}
		celVars = append(celVars, cel.Variable("key", cel.AnyType))
		celVars = append(celVars, cel.Variable("value", cel.AnyType))
		celVars = append(celVars, cel.Variable("item", cel.AnyType))

		for _, m := range matches {
			if !declared[m] {
				declared[m] = true
				celVars = append(celVars, cel.Variable(m, cel.AnyType))
			}
		}

		env, err := cel.NewEnv(celVars...)
		if err != nil {
			return false, err
		}
		ast, issues := env.Compile(expr)
		if issues != nil && issues.Err() != nil {
			return false, issues.Err()
		}
		prg, err := env.Program(ast)
		if err != nil {
			return false, err
		}

		itemMap, isMap := item.(map[string]any)
		if !isMap {
			if om, ok := item.(*OrderedMap); ok && om != nil {
				itemMap = om.m
			} else if om, ok := item.(OrderedMap); ok {
				itemMap = om.m
			} else {
				return false, fmt.Errorf("item is not a map, got %T", item)
			}
		}

		// Build a robust context map that supports "value.x", "orders.x", or just "x"
		ctxMap := make(map[string]any)
		unprefixed := make(map[string]any)

		for k, v := range itemMap {
			ctxMap[k] = v // literal prefix map

			if idx := strings.Index(k, "."); idx != -1 {
				base := k[idx+1:]
				unprefixed[base] = v

				prefix := k[:idx]
				if _, ok := unprefixed[prefix]; !ok {
					unprefixed[prefix] = make(map[string]any)
				}
				unprefixed[prefix].(map[string]any)[base] = v
			} else {
				unprefixed[k] = v
			}
		}

		for k, v := range unprefixed {
			ctxMap[k] = v
		}
		ctxMap["item"] = itemMap
		ctxMap["value"] = unprefixed
		ctxMap["key"] = unprefixed["key"]

		out, _, err := prg.Eval(ctxMap)
		if err != nil {
			return false, err
		}
		if b, ok := out.Value().(bool); ok {
			return b, nil
		}
		return false, fmt.Errorf("expression did not return a boolean")
	}

	if matchMap, ok := condition.(map[string]any); ok {

		if itemMap, ok := item.(map[string]any); ok {
			for k := range matchMap {

				if _, found := resolveKey(itemMap, k); !found {

					if suggested := findSimilarKey(k, itemMap); suggested != "" {
						return false, fmt.Errorf("field '%s' not found in item. Did you mean '%s'?", k, suggested)
					}

					return false, fmt.Errorf("field '%s' not found in item. Available fields: %v", k, getKeys(itemMap))
				}
			}
		}

		return matchesMap(item, matchMap), nil
	}

	return false, fmt.Errorf("unsupported filter condition type")
}

func (e *ScriptEngine) Filter(ctx context.Context, input any, args map[string]any) (any, error) {
	conditionRaw := args["condition"]
	if conditionRaw == nil {
		return input, nil
	}

	if cursor, ok := input.(ScriptCursor); ok {
		return &FilterCursor{
			source: cursor,
			filter: conditionRaw,
			engine: e,
		}, nil
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
		limit = 1000
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

// handleInto checks for 'into' argument and drains cursor/list to a store.
func (e *ScriptEngine) handleInto(ctx context.Context, input any, args map[string]any) (any, error) {
	intoStoreName, _ := args["into"].(string)
	if intoStoreName == "" {
		return input, nil
	}

	intoStoreName = e.resolveVarName(intoStoreName)

	store, err := e.OpenStore(ctx, map[string]any{"name": intoStoreName, "create": true})
	if err != nil {
		return nil, fmt.Errorf("failed to open/create store '%s': %v", intoStoreName, err)
	}

	cursor, isCursor := input.(ScriptCursor)
	list, isList := input.([]any)

	if !isCursor && !isList {

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

	return store, nil
}

func (e *ScriptEngine) Join(ctx context.Context, input any, args map[string]any) (any, error) {
	rightVar, _ := args["with"].(string)
	if rightVar == "" {
		rightVar, _ = args["store"].(string)
	}
	if rightVar == "" {
		rightVar, _ = args["right_store"].(string)
	}
	if rightVar == "" {
		rightVar, _ = args["right"].(string)
	}

	rightVar = e.resolveVarName(rightVar)

	if rightVar == "" {
		return nil, fmt.Errorf("operation 'join' failed: right input variable '' not found")
	}

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

	if on != nil {
		newOn := make(map[string]any)
		changed := false
		for k, v := range on {
			parts := strings.SplitN(k, ".", 2)
			if len(parts) == 2 {
				prefix := parts[0]
				if val, ok := e.Context.Variables[prefix]; ok {
					// Variable exists
					var realName string
					if s, ok := val.(jsondb.StoreAccessor); ok {
						realName = s.GetStoreInfo().Name
					}

					if realName != "" && realName != prefix {
						newKey := realName + "." + parts[1]
						newOn[newKey] = v
						changed = true
						continue
					}
				}
			}
			newOn[k] = v
		}
		if changed {
			on = newOn
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

	rightStore, isRightStore := e.getStore(rightVar)

	rightAlias, _ := args["right_alias"].(string)
	if rightAlias == "" {
		rightAlias, _ = args["alias"].(string)
	}
	if rightAlias == "" {

		if isRightStore && rightStore != nil {
			rightAlias = rightStore.GetStoreInfo().Name
			if rightAlias == "" {
				rightAlias = rightVar
			}
		} else if a, ok := args["store"].(string); ok && a != "" {
			rightAlias = strings.TrimPrefix(a, "@")
		} else if a, ok := args["right_dataset"].(string); ok && a != "" {
			rightAlias = strings.TrimPrefix(a, "@")
		} else {
			rightAlias = rightVar
		}
	}

	leftAlias, _ := args["left_alias"].(string)

	log.Debug("Join", "RightVar", rightVar, "IsStore", isRightStore, "JoinType", joinType)

	var result any
	var err error

	if joinType == "full" && isRightStore {
		// Materialize Left Input locally (needed for Right Anti-Join part)
		var leftList []any
		if lc, ok := leftCursor.(*ListCursor); ok {
			leftList = lc.items
		} else {

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

			leftCursor = &ListCursor{items: leftList}
		}

		invertedOn := make(map[string]any)
		for k, v := range on {
			invertedOn[fmt.Sprintf("%v", v)] = k
		}

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

	if joinType == "right" {

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

			result, err = &RightOuterJoinStoreCursor{
				rightStore: rightStore,

				leftCursor: leftCursor,
				leftList:   leftList,
				on:         invertedOn,
				ctx:        ctx,
				engine:     e,
				rightAlias: rightAlias,
				leftAlias:  leftAlias,
			}, nil
		} else {

			if len(leftList) == 0 && leftCursor != nil {

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

			lAlias := leftAlias
			if lAlias == "" {
				lAlias = "Left"
			}
			rAlias := rightAlias
			if rAlias == "" {
				rAlias = "Right"
			}

			result, err = e.stageJoin(rightList, leftList, "left", invertedOn, lAlias, rAlias)
		}

		if err == nil {
			return e.handleInto(ctx, result, args)
		}
		return result, err
	}

	useLookupJoin := false
	if isRightStore {
		useLookupJoin = true
	}

	if useLookupJoin {

		cursor := &JoinRightCursor{
			left:           leftCursor,
			right:          rightStore,
			joinType:       joinType,
			on:             on,
			ctx:            ctx,
			engine:         e,
			rightStoreName: rightAlias,
		}

		return e.handleInto(ctx, cursor, args)
	}

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

				merged := make(map[string]any)
				if m, ok := rItem.(map[string]any); ok {
					for k, v := range m {

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

	if store, ok := args["store"]; ok {
		args["with"] = store

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
	store, ok := e.getStore(storeName)
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
	store, ok := e.getStore(storeName)
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}

	return e.stageDelete(ctx, list, store, args)
}

func (e *ScriptEngine) Inspect(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.getStore(storeName)
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

func (e *ScriptEngine) stageFilter(input []any, args map[string]any) ([]any, error) {
	conditionRaw := args["condition"]
	if conditionRaw == nil {
		return input, nil
	}

	output := make([]any, 0)

	if _, ok := conditionRaw.(string); ok {
		for _, item := range input {
			match, err := e.evaluateCondition(item, conditionRaw)
			if err == nil && match {
				output = append(output, item)
			}
		}
		return output, nil
	}

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

	fields := parseProjectionFields(args["fields"])

	log.Debug("stageProject called", "input_len", len(input), "input_fields", fields)

	var output []any
	for i, item := range input {
		newItem := renderItem(nil, item, fields)
		output = append(output, newItem)

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
		if key == nil {

			if info := store.GetStoreInfo(); info.Name != "" {
				key = item[info.Name+".key"]
			}
		}

		currentVal := item["value"]
		var newVal any
		if currMap, ok := currentVal.(map[string]any); ok {

			newMap := make(map[string]any)
			for k, v := range currMap {
				newMap[k] = v
			}
			for k, v := range values {
				newMap[k] = v
			}
			newVal = newMap
		} else {
			newVal = values
		}

		if _, err := store.Update(ctx, key, newVal); err != nil {
			return nil, err
		}

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
		if key == nil {

			if info := store.GetStoreInfo(); info.Name != "" {
				key = getField(itemObj, info.Name+".key")
			}
		}

		if _, err := store.Remove(ctx, key); err != nil {
			return nil, err
		}
	}
	return []any{}, nil
}

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

	collection, ok := e.Context.Variables[collectionVar]
	if !ok {
		return fmt.Errorf("collection variable '%s' not found", collectionVar)
	}

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

func normalizeCallFunctionName(args map[string]any) string {
	for _, key := range []string{"name", "function", "function_name", "functionName", "tool", "tool_name", "toolName", "command"} {
		if value, ok := args[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func normalizeCallFunctionParams(args map[string]any) map[string]any {
	for _, key := range []string{"params", "arguments", "args", "input"} {
		if value, ok := args[key].(map[string]any); ok {
			return value
		}
	}
	return nil
}

func (e *ScriptEngine) CallFunction(ctx context.Context, args map[string]any) (any, error) {
	functionName := normalizeCallFunctionName(args)
	if functionName == "" {
		return nil, fmt.Errorf("function name required")
	}

	if e.FunctionHandler == nil {
		return nil, fmt.Errorf("function handler not configured")
	}

	savedVars := make(map[string]any)
	params := normalizeCallFunctionParams(args)
	if params != nil {
		for k, v := range params {
			if oldVal, exists := e.Context.Variables[k]; exists {
				savedVars[k] = oldVal
			}
			e.Context.Variables[k] = v
		}
	}

	// Call Handler
	res, err := e.FunctionHandler(ctx, functionName, params)

	for k, v := range savedVars {
		e.Context.Variables[k] = v
	}

	return res, err
}

func (e *ScriptEngine) First(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.getStore(storeName)
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.First(ctx)
}

func (e *ScriptEngine) Last(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.getStore(storeName)
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.Last(ctx)
}

func (e *ScriptEngine) Next(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.getStore(storeName)
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.Next(ctx)
}

func (e *ScriptEngine) Previous(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.getStore(storeName)
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.Previous(ctx)
}

func (e *ScriptEngine) Find(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.getStore(storeName)
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
	store, ok := e.getStore(storeName)
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}
	key := args["key"]
	value := args["value"]
	return store.Add(ctx, key, value)
}

func (e *ScriptEngine) GetCurrentKey(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.getStore(storeName)
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}
	k := store.GetCurrentKey()
	return k, nil
}

func (e *ScriptEngine) GetCurrentValue(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.getStore(storeName)
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}
	v, err := store.GetCurrentValue(ctx)
	return v, err
}

func (e *ScriptEngine) ListAppend(ctx context.Context, input any, args map[string]any) (any, error) {
	listVar, _ := args["list"].(string)
	item := args["item"]
	if item == nil {
		item = args["value"]
	}

	var list any

	if listVar != "" {
		var ok bool
		list, ok = e.Context.Variables[listVar]
		if !ok {
			return nil, fmt.Errorf("list variable '%s' not found", listVar)
		}
	} else {

		if input == nil {
			return nil, fmt.Errorf("list variable not specified and no input provided")
		}
		list = input
	}

	var resultList []any

	if l, ok := list.([]any); ok {

		resultList = append(l, item)
	} else if l, ok := list.([]map[string]any); ok {

		resultList = make([]any, len(l)+1)
		for i, v := range l {
			resultList[i] = v
		}
		resultList[len(l)] = item
	} else {
		return nil, fmt.Errorf("variable (or input) is not a list")
	}

	if listVar != "" {
		e.Context.Variables[listVar] = resultList
	}

	return resultList, nil
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

func (a *CopilotAgent) opCallScript(ctx context.Context, scriptCtx *ScriptContext, args map[string]any) (any, error) {
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

	funcStore, err := a.systemDB.OpenModelStore(ctx, "scripts", sysTx)
	if err != nil {
		return nil, fmt.Errorf("failed to open scripts store: %v", err)
	}

	category, _ := args["category"].(string)
	if category == "" {
		category = ai.DefaultScriptCategory
	}

	var funcMap map[string]any
	if err := funcStore.Load(ctx, category, scriptName, &funcMap); err != nil {
		return nil, fmt.Errorf("script '%s/%s' not found: %v", category, scriptName, err)
	}

	stepsRaw, ok := funcMap["steps"].([]any)
	if !ok {
		return nil, nil
	}

	isAtomic := false
	if len(stepsRaw) > 0 {
		if firstStep, ok := stepsRaw[0].(map[string]any); ok {
			_, hasOp := firstStep["op"]
			_, hasType := firstStep["type"]

			if hasOp && !hasType {
				isAtomic = true
			}
		}
	}

	if isAtomic {

		bytes, err := json.Marshal(stepsRaw)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal script steps: %v", err)
		}

		sum := sha256.Sum256(bytes)
		currentHash := hex.EncodeToString(sum[:])

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

			a.compiledScriptsMu.Lock()
			a.compiledScripts[scriptName] = CachedScript{Script: compiled, Hash: currentHash}
			a.compiledScriptsMu.Unlock()
		} else {
			compiled = cachedEntry.Script
		}

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

		if val, ok := engine.Context.Variables["output"]; ok {
			return val, nil
		}

		if engine.HasReturned {
			return engine.ReturnValue, nil
		}

		return engine.LastResult, nil
	}

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
