package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"text/template"

	log "log/slog"

	"golang.org/x/sync/errgroup"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

type contextKey string

const CtxKeyJSONStreamer contextKey = "json_streamer"
const CtxKeyUseNDJSON contextKey = "use_ndjson"
const CtxKeyCurrentScriptCategory contextKey = "current_script_category"

type StepExecutionResult struct {
	Type      string `json:"type"`
	Command   string `json:"command,omitempty"`
	Prompt    string `json:"prompt,omitempty"`
	StepIndex int    `json:"step_index"`
	Result    any    `json:"result,omitempty"`
	Record    any    `json:"record,omitempty"`
	Error     string `json:"error,omitempty"`
}

type Flusher interface {
	Flush()
}

// JSONStreamer handles streaming JSON array elements
type JSONStreamer struct {
	w                 io.Writer
	mu                *sync.Mutex
	first             bool
	isNDJSON          bool
	shouldFlush       bool
	suppressStepStart bool
}

func (s *JSONStreamer) SetFlush(flush bool) {
	s.shouldFlush = flush
}

func (s *JSONStreamer) SetSuppressStepStart(suppress bool) {
	s.suppressStepStart = suppress
}

func NewJSONStreamer(w io.Writer) *JSONStreamer {
	return &JSONStreamer{
		w:     w,
		mu:    &sync.Mutex{},
		first: true,
	}
}

func NewNDJSONStreamer(w io.Writer) *JSONStreamer {
	return &JSONStreamer{
		w:        w,
		mu:       &sync.Mutex{},
		first:    true,
		isNDJSON: true,
	}
}

func (s *JSONStreamer) Write(step StepExecutionResult) {
	if s.suppressStepStart && step.Type == "step_start" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isNDJSON {
		bytes, _ := json.Marshal(step)
		fmt.Fprintln(s.w, string(bytes))
		if s.shouldFlush {
			if f, ok := s.w.(Flusher); ok {
				f.Flush()
			}
		}
		return
	}

	if !s.first {
		fmt.Fprint(s.w, ",\n")
	}
	s.first = false

	bytes, _ := json.MarshalIndent(step, "  ", "  ")
	fmt.Fprint(s.w, string(bytes))
	if s.shouldFlush {
		if f, ok := s.w.(Flusher); ok {
			f.Flush()
		}
	}
}

// StartStreamingStep starts a new step in the JSON stream and returns a StepStreamer.
// Note: The header is written lazily when the first item is written.
func (s *JSONStreamer) StartStreamingStep(stepType, command, prompt string, stepIndex int) *StepStreamer {
	return &StepStreamer{
		parent:    s,
		first:     true,
		stepType:  stepType,
		command:   command,
		prompt:    prompt,
		stepIndex: stepIndex,
	}
}

// StepStreamer implements ai.ResultStreamer to stream result items.
type StepStreamer struct {
	parent    *JSONStreamer
	first     bool
	closed    bool
	used      bool
	stepType  string
	command   string
	prompt    string
	stepIndex int
	metadata  map[string]any
}

func (ss *StepStreamer) SetMetadata(meta map[string]any) {
	ss.metadata = meta
}

func (ss *StepStreamer) writeHeader() {
	ss.parent.mu.Lock()
	defer ss.parent.mu.Unlock()

	// Check suppression flag on parent
	if ss.parent.suppressStepStart {
		// If metadata is present (e.g. columns), we must write the header to include it.
		// Otherwise, suppress wrapper for records/results to keep output clean.
		if len(ss.metadata) == 0 && (ss.stepType == "record" || ss.stepType == "tool_result") {
			return
		}
	}

	if ss.parent.isNDJSON {
		res := StepExecutionResult{
			Type:      "step_start",
			Command:   ss.command,
			Prompt:    ss.prompt,
			StepIndex: ss.stepIndex,
		}
		b, _ := json.Marshal(res)
		fmt.Fprintln(ss.parent.w, string(b))

		if f, ok := ss.parent.w.(Flusher); ok {
			f.Flush()
		}
		return
	}

	if !ss.parent.first {
		fmt.Fprint(ss.parent.w, ",\n")
	}
	ss.parent.first = false

	fmt.Fprintf(ss.parent.w, `{"type":%q`, ss.stepType)
	if ss.command != "" {
		fmt.Fprintf(ss.parent.w, `,"command":%q`, ss.command)
	}
	if ss.prompt != "" {
		fmt.Fprintf(ss.parent.w, `,"prompt":%q`, ss.prompt)
	}
	if len(ss.metadata) > 0 {
		b, _ := json.Marshal(ss.metadata)
		fmt.Fprintf(ss.parent.w, `,"metadata":%s`, string(b))
	}
	fmt.Fprint(ss.parent.w, `,"result":`)
}

func (ss *StepStreamer) BeginArray() {
	// Lazy Start: Do nothing here.
	// We wait for the first WriteItem to detect columns.
	// OR if EndArray is called without items, we handle empty array there.

	// Exception: NDJSON might need immediate handling if supported?
	// Existing NDJSON logic in BeginArray: if ss.parent.isNDJSON return.
	// So safe to do nothing for JSON streamer.
}

func (ss *StepStreamer) WriteItem(item any) {
	if !ss.used {
		// Auto-detect columns if not set
		if ss.metadata == nil {
			ss.metadata = make(map[string]any)
		}
		if _, ok := ss.metadata["columns"]; !ok {
			var cols []string
			if m, ok := item.(map[string]any); ok {
				for k := range m {
					cols = append(cols, k)
				}
				sort.Strings(cols)
			} else if om, ok := item.(OrderedMap); ok {
				cols = om.keys
			} else if om, ok := item.(*OrderedMap); ok {
				cols = om.keys
			}

			if len(cols) > 0 {
				ss.metadata["columns"] = cols
			}
		}

		ss.writeHeader()
		ss.used = true

		// Start Array
		ss.parent.mu.Lock()
		fmt.Fprint(ss.parent.w, "[")
		ss.parent.mu.Unlock()
	}

	ss.parent.mu.Lock()
	defer ss.parent.mu.Unlock()

	if ss.parent.isNDJSON {
		res := StepExecutionResult{
			Type:   "record",
			Record: item,
		}
		b, _ := json.Marshal(res)
		fmt.Fprintln(ss.parent.w, string(b))
		if f, ok := ss.parent.w.(Flusher); ok {
			f.Flush()
		}
		return
	}

	if !ss.first {
		fmt.Fprint(ss.parent.w, ",")
	}
	ss.first = false

	b, _ := json.Marshal(item)
	ss.parent.w.Write(b)
}

func (ss *StepStreamer) EndArray() {
	if ss.parent.isNDJSON {
		return
	}

	if !ss.used {
		// Empty results case
		// Write Header
		ss.writeHeader()
		ss.used = true
		// Write Empty Array
		ss.parent.mu.Lock()
		fmt.Fprint(ss.parent.w, "[]")
		ss.parent.mu.Unlock()
		return
	}

	ss.parent.mu.Lock()
	defer ss.parent.mu.Unlock()
	fmt.Fprint(ss.parent.w, "]")
}

func (ss *StepStreamer) Close() {
	if ss.closed {
		return
	}
	ss.parent.mu.Lock()
	defer ss.parent.mu.Unlock()

	if ss.parent.isNDJSON {
		ss.closed = true
		return
	}

	fmt.Fprint(ss.parent.w, "}")
	ss.closed = true
}

func (s *Service) runStep(ctx context.Context, step ai.ScriptStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	switch step.Type {
	case "ask":
		return s.runStepAsk(ctx, step, scope, scopeMu, sb, db)
	case "command":
		return s.runStepCommand(ctx, step, scope, scopeMu, sb)
	case "set", "assignment":
		return s.runStepSet(ctx, step, scope, scopeMu, sb)
	case "if", "condition":
		return s.runStepIf(ctx, step, scope, scopeMu, sb, db)
	case "loop":
		return s.runStepLoop(ctx, step, scope, scopeMu, sb, db)
	case "fetch":
		return s.runStepFetch(ctx, step, scope, scopeMu, sb, db)
	case "say", "print":
		return s.runStepSay(ctx, step, scope, scopeMu, sb)
	case "call_script", "script":
		return s.runStepScript(ctx, step, scope, scopeMu, sb, db)
	case "block":
		return s.runStepBlock(ctx, step, scope, scopeMu, sb, db)
	}
	return nil
}

func (s *Service) runStepBlock(ctx context.Context, step ai.ScriptStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	return s.runSteps(ctx, step.Steps, scope, scopeMu, sb, db)
}

func (s *Service) runStepAsk(ctx context.Context, step ai.ScriptStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	w, _ := ctx.Value(ai.CtxKeyWriter).(io.Writer)

	prompt := step.Prompt
	prompt = s.resolveTemplate(prompt, scope, scopeMu)

	// Only print prompt if NOT using streamer (standard output mode)
	if ctx.Value(CtxKeyJSONStreamer) == nil {
		msg := fmt.Sprintf("> %s\n", prompt)
		sb.WriteString(msg)
		if w != nil {
			fmt.Fprint(w, msg)
		}
	}

	var opts []ai.Option
	if db != nil {
		opts = append(opts, ai.WithDatabase(db))
	}

	// SOP Design Principle: Explicit Control
	// An 'ask' step in a script is an imperative instruction to query the LLM.
	// It is never skipped unless explicitly overridden by an external "replay" context
	// (e.g. recovering session history).
	// This ensures the script engine remains "dumb and obedient," allowing complex
	// agentic workflows (Setup -> Ask -> Act) to function reliably.

	resp, err := s.Ask(ctx, prompt, opts...)
	if err != nil {
		msg := fmt.Sprintf("Error: %v\n", err)
		sb.WriteString(msg)
		if w != nil {
			fmt.Fprint(w, msg)
		}
		// Continue on error, but log it.
	} else {
		// Check if resp is a tool call
		var toolCall struct {
			Tool string         `json:"tool"`
			Args map[string]any `json:"args"`
		}
		if json.Unmarshal([]byte(resp), &toolCall) == nil && toolCall.Tool != "" {
			// It's a tool call! Execute it.
			if executor, ok := ctx.Value(ai.CtxKeyExecutor).(ai.ToolExecutor); ok && executor != nil {
				toolResp, err := executor.Execute(ctx, toolCall.Tool, toolCall.Args)
				if err != nil {
					resp = fmt.Sprintf("Error executing tool '%s': %v", toolCall.Tool, err)
				} else {
					resp = toolResp
				}
			}
		}

		if streamer, ok := ctx.Value(CtxKeyJSONStreamer).(*JSONStreamer); ok {
			var resultAny any = resp
			// Try to unmarshal if it looks like JSON
			trimmed := strings.TrimSpace(resp)
			if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
				// Optimization: Use RawMessage
				resultAny = json.RawMessage(resp)
			}

			stepIndex, _ := ctx.Value("step_index").(int)
			streamer.Write(StepExecutionResult{
				Type:      "ask",
				Prompt:    prompt,
				Result:    resultAny,
				StepIndex: stepIndex,
			})
		} else {
			msg := fmt.Sprintf("%s\n", resp)
			sb.WriteString(msg)
			if w != nil {
				fmt.Fprint(w, msg)
			}
		}

		// Support "Variable" field for backward compatibility
		outVar := step.OutputVariable
		if outVar == "" {
			outVar = step.Variable
		}

		if outVar != "" {
			if scopeMu != nil {
				scopeMu.Lock()
				defer scopeMu.Unlock()
			}
			scope[outVar] = strings.TrimSpace(resp)
		}
	}
	return nil
}

func (s *Service) runStepCommand(ctx context.Context, step ai.ScriptStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder) error {
	w, _ := ctx.Value(ai.CtxKeyWriter).(io.Writer)

	// Resolve templates in Args
	resolvedArgs := make(map[string]any)
	for k, v := range step.Args {
		if strVal, ok := v.(string); ok {
			resolvedArgs[k] = s.resolveTemplate(strVal, scope, scopeMu)
		} else {
			resolvedArgs[k] = v
		}
	}

	// Capture step for /last-tool support
	if s.session != nil {
		recordedStep := step
		recordedStep.Args = resolvedArgs
		s.session.LastInteractionToolCalls = append(s.session.LastInteractionToolCalls, recordedStep)
		// Also update LastStep for robustness (some logic checks LastStep)
		s.session.LastStep = &recordedStep
	}

	// Execute Tool
	val := ctx.Value(ai.CtxKeyExecutor)
	if val == nil {
		// If no executor, but command is empty, we can skip it.
		if step.Command == "" {
			return nil
		}
		return fmt.Errorf("no tool executor available (ctx value is nil)")
	}

	executor, ok := val.(ai.ToolExecutor)
	if !ok {
		return fmt.Errorf("no tool executor available (type assertion failed, type: %T)", val)
	}

	if executor != nil {
		// Skip empty commands
		if step.Command == "" {
			return nil
		}

		// Prepare streamer if available
		var stepStreamer *StepStreamer
		if streamer, ok := ctx.Value(CtxKeyJSONStreamer).(*JSONStreamer); ok {
			stepIndex, _ := ctx.Value("step_index").(int)
			log.Debug("runStepCommand: Got step_index", "index", stepIndex)
			stepStreamer = streamer.StartStreamingStep("command", step.Command, "", stepIndex)
			ctx = context.WithValue(ctx, ai.CtxKeyResultStreamer, stepStreamer)
		}

		resp, err := executor.Execute(ctx, step.Command, resolvedArgs)

		// Check if tool streamed the result
		if stepStreamer != nil && stepStreamer.used {
			stepStreamer.Close()
			if err != nil {
				return fmt.Errorf("command execution failed (streamed): %w", err)
			}
			// If streamed, we don't capture output to variable currently
			return nil
		}

		if err != nil {
			return fmt.Errorf("command execution failed: %w", err)
		}

		// Stream result if streamer is present (and wasn't used by tool)
		if streamer, ok := ctx.Value(CtxKeyJSONStreamer).(*JSONStreamer); ok {
			var resultAny any = resp

			// Optimization/UX: If the result is a JSON array (list of records),
			// we want to stream them individually or at least separate the command metadata from data.
			// This matches the "Pattern 3" request: Header -> Row -> Row ...

			stepIndex, _ := ctx.Value("step_index").(int)

			trimmed := strings.TrimSpace(resp)
			if strings.HasPrefix(trimmed, "[") {
				// Parse array
				var list []json.RawMessage
				if err := json.Unmarshal([]byte(trimmed), &list); err == nil {
					// 1. Emit Step Header
					streamer.Write(StepExecutionResult{
						Type:      "step_start",
						Command:   step.Command,
						StepIndex: stepIndex,
					})

					// 2. Emit Records
					for _, item := range list {
						streamer.Write(StepExecutionResult{
							Type:   "record",
							Record: item,
						})
					}
					return nil
				}
			}

			// Fallback: Single Object or Primitive or Failed Array Parse
			// Try to unmarshal if it looks like JSON object to avoid escaping
			if strings.HasPrefix(trimmed, "{") {
				resultAny = json.RawMessage(resp)
			}

			// Emit as single result (default behavior for non-list outputs)
			// But maybe we should still wrap in step_start?
			// Let's emit header + 1 record
			log.Debug("runStepCommand: Emitting fallback step_start", "index", stepIndex)
			streamer.Write(StepExecutionResult{
				Type:      "step_start",
				Command:   step.Command,
				StepIndex: stepIndex,
			})
			streamer.Write(StepExecutionResult{
				Type:   "record",
				Record: resultAny,
			})
		} else {
			// Fallback to string builder if no streamer (standard output mode)
			msg := fmt.Sprintf("%s\n", resp)
			sb.WriteString(msg)
			if w != nil {
				fmt.Fprint(w, msg)
			}
		}

		// Output Variable
		if step.OutputVariable != "" {
			if scopeMu != nil {
				scopeMu.Lock()
				defer scopeMu.Unlock()
			}
			scope[step.OutputVariable] = strings.TrimSpace(resp)
		}
	} else {
		// Should be unreachable due to checks above
		return fmt.Errorf("no tool executor available")
	}
	return nil
}

func (s *Service) runStepSet(ctx context.Context, step ai.ScriptStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder) error {
	val := step.Value
	val = s.resolveTemplate(val, scope, scopeMu)

	if step.Variable != "" {
		if scopeMu != nil {
			scopeMu.Lock()
			defer scopeMu.Unlock()
		}
		scope[step.Variable] = val
	}
	return nil
}

func (s *Service) runStepIf(ctx context.Context, step ai.ScriptStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	w, _ := ctx.Value(ai.CtxKeyWriter).(io.Writer)

	cond := step.Condition

	// Evaluate Expression using template: {{ if .Expression }}true{{end}}
	condTmpl := fmt.Sprintf("{{if %s}}true{{end}}", cond)

	if scopeMu != nil {
		scopeMu.RLock()
		defer scopeMu.RUnlock()
	}

	if tmpl, err := template.New("cond").Parse(condTmpl); err == nil {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, scope); err == nil {
			if buf.String() == "true" {
				thenSteps := step.Then
				return s.runSteps(ctx, thenSteps, scope, scopeMu, sb, db)
			} else {
				if len(step.Else) > 0 {
					return s.runSteps(ctx, step.Else, scope, scopeMu, sb, db)
				}
			}
		} else {
			if ctx.Value(CtxKeyJSONStreamer) == nil {
				msg := fmt.Sprintf("Error evaluating condition '%s': %v\n", cond, err)
				sb.WriteString(msg)
				if w != nil {
					fmt.Fprint(w, msg)
				}
			}
		}
	}
	return nil
}

func (s *Service) runStepLoop(ctx context.Context, step ai.ScriptStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	listExpr := step.List
	iterator := step.Iterator
	body := step.Steps

	if scopeMu != nil {
		scopeMu.RLock()
	}
	val, ok := scope[listExpr]
	if scopeMu != nil {
		scopeMu.RUnlock()
	}

	if ok {
		// Attempt to parse string as JSON array if it looks like one
		if strVal, ok := val.(string); ok {
			trimmed := strings.TrimSpace(strVal)
			if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
				var list []any
				if err := json.Unmarshal([]byte(strVal), &list); err == nil {
					// It's a valid JSON array, switch to slice iteration
					val = list
				}
			}
		}

		if strVal, ok := val.(string); ok {
			lines := strings.Split(strVal, "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				if scopeMu != nil {
					scopeMu.Lock()
				}
				scope[iterator] = line
				if scopeMu != nil {
					scopeMu.Unlock()
				}
				if err := s.runSteps(ctx, body, scope, scopeMu, sb, db); err != nil {
					return err
				}
			}
		} else if sliceVal, ok := val.([]any); ok {
			for _, item := range sliceVal {
				if scopeMu != nil {
					scopeMu.Lock()
				}
				scope[iterator] = item
				if scopeMu != nil {
					scopeMu.Unlock()
				}
				if err := s.runSteps(ctx, body, scope, scopeMu, sb, db); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Service) runStepFetch(ctx context.Context, step ai.ScriptStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, currentDB *database.Database) error {
	w, _ := ctx.Value(ai.CtxKeyWriter).(io.Writer)

	resource := step.Resource
	outVar := step.Variable // Fetch uses Variable for output in both schemas

	if step.Source == "btree" && resource != "" {
		var db *database.Database

		// 1. Resolve Database
		if step.Database != "" {
			// Use Service configuration for resolution
			if (step.Database == "system" || step.Database == "SystemDB") && s.systemDB != nil {
				db = s.systemDB
			} else if opts, ok := s.databases[step.Database]; ok {
				db = database.NewDatabase(opts)
			} else {
				return fmt.Errorf("database '%s' not found in agent configuration", step.Database)
			}
		} else {
			// Use current context database
			db = currentDB
		}

		if db == nil {
			if ctx.Value(CtxKeyJSONStreamer) == nil {
				msg := "Error: No database configured for fetch operation.\n"
				sb.WriteString(msg)
				if w != nil {
					fmt.Fprint(w, msg)
				}
			}
			return fmt.Errorf("no database provided")
		}

		// 2. Fetch Data
		// Check if we have an active transaction in the session payload
		var tx sop.Transaction
		var err error

		if p := ai.GetSessionPayload(ctx); p != nil && p.Transaction != nil {
			if t, ok := p.Transaction.(sop.Transaction); ok {
				tx = t
			}
		}

		// If no active transaction, start a local one (Read-Only)
		localTx := false
		if tx == nil {
			tx, err = db.BeginTransaction(ctx, sop.ForReading)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			localTx = true
		}

		if localTx {
			defer tx.Rollback(ctx)
		}

		store, err := db.OpenBtreeCursor(ctx, resource, tx)
		if err != nil {
			return fmt.Errorf("failed to open store '%s': %w", resource, err)
		}

		// 3. Iterate and Collect
		var items []any
		if ok, err := store.First(ctx); ok && err == nil {
			for {
				k := store.GetCurrentKey()
				v, _ := store.GetCurrentValue(ctx)
				items = append(items, fmt.Sprintf("%v: %v", k, v))
				if ok, _ := store.Next(ctx); !ok {
					break
				}
				if len(items) >= 10 {
					break
				}
			}
		}

		if outVar != "" {
			if scopeMu != nil {
				scopeMu.Lock()
				defer scopeMu.Unlock()
			}
			scope[outVar] = items
		}
	}
	return nil
}

func (s *Service) runStepSay(ctx context.Context, step ai.ScriptStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder) error {
	w, _ := ctx.Value(ai.CtxKeyWriter).(io.Writer)

	msgText := step.Message
	msgText = s.resolveTemplate(msgText, scope, scopeMu)

	if streamer, ok := ctx.Value(CtxKeyJSONStreamer).(*JSONStreamer); ok {
		streamer.Write(StepExecutionResult{
			Type:   "say",
			Result: msgText,
		})
	} else {
		msg := fmt.Sprintf("%s\n", msgText)
		sb.WriteString(msg)
		if w != nil {
			fmt.Fprint(w, msg)
		}
	}
	return nil
}

func (s *Service) runStepScript(ctx context.Context, step ai.ScriptStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	name := step.ScriptName
	if name == "" {
		return fmt.Errorf("script name required")
	}

	// Load the script
	scriptDB := s.getScriptDB()
	if scriptDB == nil {
		return fmt.Errorf("no database configured")
	}

	tx, err := scriptDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}

	store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("error opening store: %v", err)
	}

	var script ai.Script
	// Parse category from script name (e.g. "finance.compute_monthly")
	targetCategory := "general"
	scriptName := name
	if idx := strings.Index(name, "."); idx > 0 {
		targetCategory = name[:idx]
		scriptName = name[idx+1:]
	} else {
		// No category specified, try to inherit from current context
		if curr, ok := ctx.Value(CtxKeyCurrentScriptCategory).(string); ok && curr != "" {
			targetCategory = curr
		}
	}

	// Try loading from target category
	if err := store.Load(ctx, targetCategory, scriptName, &script); err != nil {
		// If not found and we were looking in a specific category (not general), try general
		if targetCategory != "general" {
			if errFallback := store.Load(ctx, "general", scriptName, &script); errFallback == nil {
				targetCategory = "general" // Found in general
				err = nil
			}
		}

		if err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("error loading script '%s': %v", name, err)
		}
	}
	tx.Commit(ctx)

	// Prepare scope for the nested script
	// We inherit the current scope, but we might want to override with ScriptArgs
	nestedScope := make(map[string]any)
	if scopeMu != nil {
		scopeMu.RLock()
	}
	for k, v := range scope {
		nestedScope[k] = v
	}
	if scopeMu != nil {
		scopeMu.RUnlock()
	}

	for k, v := range step.ScriptArgs {
		// Resolve template in args
		val := s.resolveTemplate(v, scope, scopeMu)
		nestedScope[k] = val
	}

	sb.WriteString(fmt.Sprintf("Running nested script '%s'...\n", name))

	// Prepared Streamer for Nested Script (Output Handling)
	// We want to suppress 'step_start' events from the nested script so they don't clutter the UI.
	// We emit the PARENT step start (Script Step) here, then create a suppressive streamer for children.
	if streamer, ok := ctx.Value(CtxKeyJSONStreamer).(*JSONStreamer); ok {
		stepIndex, _ := ctx.Value("step_index").(int)

		// 1. Emit Parent Step Start (The "Run Script" step)
		// We use the script name as the command label
		ss := streamer.StartStreamingStep("script", name, "", stepIndex)
		ss.writeHeader()
		// We do not close ss because the step persists while children run

		// 2. Create Suppressed Streamer for Child
		// We share the mutex and writer to ensure thread safety
		childStreamer := &JSONStreamer{
			w:  streamer.w,
			mu: streamer.mu, // Share the mutex pointer
			// first:             streamer.first,
			// isNDJSON:          streamer.isNDJSON, // Always NDJSON
			shouldFlush:       streamer.shouldFlush,
			suppressStepStart: true, // Suppress child step events
		}

		// Override streamer in context
		ctx = context.WithValue(ctx, CtxKeyJSONStreamer, childStreamer)
	}

	// Nested script gets its own mutex because it has its own scope map
	var nestedMu sync.RWMutex

	// Handle Database Switching for Nested Script
	// Priority: step.Database > script.Database > inherited db

	// Handle Database Override from Step
	if step.Database != "" {
		script.Database = step.Database
		script.Portable = false // Enforce the step's DB
	}

	// Update context with the category where the script was found/resolved
	ctx = context.WithValue(ctx, CtxKeyCurrentScriptCategory, targetCategory)

	return s.executeScript(ctx, &script, nestedScope, &nestedMu, sb, db)
}

// ToolProvider interface for agents that can execute tools
type ToolProvider interface {
	Execute(ctx context.Context, toolName string, args map[string]any) (string, error)
}

// ServiceToolExecutor delegates tool execution to registered agents
type ServiceToolExecutor struct {
	s *Service
}

func (e *ServiceToolExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	// Handle "gettoolinfo" explicitly as it's a meta-tool served by Service from LLM Knowledge
	if toolName == "gettoolinfo" {
		targetTool, _ := args["tool"].(string)
		if targetTool == "" {
			// Try "tool_name" or just looking for the first string arg if not named standardly
			targetTool, _ = args["tool_name"].(string)
		}
		if targetTool == "" {
			return "", fmt.Errorf("tool argument required for gettoolinfo")
		}
		return e.s.getToolInfo(ctx, targetTool)
	}

	for _, agent := range e.s.registry {
		if provider, ok := agent.(ToolProvider); ok {
			resp, err := provider.Execute(ctx, toolName, args)
			if err == nil {
				return resp, nil
			}
			// If error is "unknown tool", continue to next agent.
			// Otherwise return error.
			// Since we don't have a standard error type for "unknown tool", we check string.
			// DataAdminAgent returns "unknown tool: %s".
			if strings.Contains(err.Error(), "unknown tool") {
				continue
			}
			return "", err
		}
	}
	return "", fmt.Errorf("tool '%s' not found in any registered agent", toolName)
}

func (e *ServiceToolExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return nil, nil
}

func (s *Service) executeScript(ctx context.Context, script *ai.Script, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	// ScriptRecorder must be preserved in context so that tools executed by the script
	// are recorded in the session state (Session.LastStep / Session.LastInteractionToolCalls).
	// This ensures commands like /last-tool work correctly after script execution.
	// ctx = context.WithValue(ctx, ai.CtxKeyScriptRecorder, nil)

	// Ensure we have a tool executor
	if ctx.Value(ai.CtxKeyExecutor) == nil {
		executor := &ServiceToolExecutor{s: s}
		ctx = context.WithValue(ctx, ai.CtxKeyExecutor, executor)
	}

	// Explicit Execution Mode:
	// In SOP Architecture, scripts are explicit execution plans.
	// There is no implicit "inference" or "compilation" based on content.
	// If a script contains mixing of 'ask' (LLM) and 'command' (Deterministic) steps,
	// they are executed exactly as defined. This allows for "Hybrid Scripts" that
	// can reason, act, and reason again within a single run-loop.
	//
	// The 'isCompiled' flag is only used for explicit external replay mechanisms
	// (like restoring session state), never guessed from script content.
	isCompiled := false
	if v, ok := ctx.Value("force_compile_mode").(bool); ok {
		isCompiled = v
	}
	ctx = context.WithValue(ctx, "is_compiled", isCompiled)

	// Set Playback flag
	// wasPlayback := s.session.Playback
	s.session.Playback = true
	// defer func() { s.session.Playback = wasPlayback }()

	return s.runSteps(ctx, script.Steps, scope, scopeMu, sb, db)
}

// runSteps runs a sequence of script steps with support for control flow and variables.
func (s *Service) runSteps(ctx context.Context, steps []ai.ScriptStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	// Create cancellable context for this script execution scope to support stopping on error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Use errgroup for managing concurrency and error propagation
	g, groupCtx := errgroup.WithContext(ctx)

	var syncErr error

	// Initialize currentDBName from payload
	var currentDBName string
	if p := ai.GetSessionPayload(ctx); p != nil {
		currentDBName = p.CurrentDB
	}

	for i, step := range steps {
		// Check if context is already cancelled (by errgroup or sync error)
		if groupCtx.Err() != nil {
			break
		}

		// Determine effective database for this step
		stepDBName := step.Database
		if stepDBName != "" {
			// Step specifies DB -> Update currentDBName (sticky)
			currentDBName = stepDBName
		} else {
			// Step does not specify -> Fallback to currentDBName
			stepDBName = currentDBName
		}

		log.Debug("Determined effective database for step", "step_type", step.Type, "db", stepDBName)

		// Prepare Context and DB for this step
		stepCtx := groupCtx
		log.Debug("runSteps: Setting step_index", "index", i+1, "command", step.Command)
		stepCtx = context.WithValue(stepCtx, "step_index", i+1)
		stepDB := db

		// Check for dead transaction (committed or rolled back)
		// This prevents "running cmd on a dead transaction" errors if a previous step manually closed it.
		p := ai.GetSessionPayload(groupCtx)
		if p != nil && p.Transaction != nil {
			if t, ok := p.Transaction.(sop.Transaction); ok && !t.HasBegun() {
				// Transaction is dead. Clear it from the payload for this step.
				// This forces the step to start a new local transaction (or session one if we could, but local is safer fallback).
				newPayload := *p
				newPayload.Transaction = nil
				stepCtx = context.WithValue(stepCtx, "session_payload", &newPayload)
				// Update p to point to newPayload so subsequent logic uses the clean state
				p = &newPayload
			}
		}

		// Check if we need to update context/DB based on stepDBName
		if p != nil {
			// If the desired DB differs from the context, or if we just want to ensure we have the right DB object
			if p.CurrentDB != stepDBName {
				// Clone payload
				newPayload := *p
				newPayload.CurrentDB = stepDBName
				// Clear transaction if switching DB
				newPayload.Transaction = nil

				stepCtx = context.WithValue(groupCtx, "session_payload", &newPayload)
				// ...
			}
			// ...
		}

		// If async, run in errgroup
		if step.IsAsync {
			// Async Step Logic:
			// We capture the 'currentDBName' at the moment of dispatch (via stepCtx/stepDB).
			// The async step runs with a CLONED context containing that DB.
			// It does NOT affect the 'currentDBName' for subsequent steps in the loop,
			// nor does it affect other async steps. This ensures isolation.

			// Check if we are in a transaction.
			// If so, we generally force sync execution to maintain integrity for simple steps.
			// HOWEVER, if the step is a nested "script", we allow it to run async (Swarm pattern),
			// assuming it will manage its own transaction (e.g. on a different DB).
			p := ai.GetSessionPayload(stepCtx) // Use stepCtx
			if p != nil && p.Transaction != nil && step.Type != "script" {
				if ctx.Value(CtxKeyJSONStreamer) == nil {
					msg := fmt.Sprintf("Info: Step '%s' marked async but active transaction exists. Running synchronously.\n", step.Type)
					sb.WriteString(msg)
					if w, ok := ctx.Value(ai.CtxKeyWriter).(io.Writer); ok && w != nil {
						fmt.Fprint(w, msg)
					}
				}
				// Fall through to sync execution
			} else {
				// Detach transaction to enforce isolation (Swarm Computing model)
				asyncCtx := stepCtx
				asyncDB := stepDB

				if p != nil {
					// Clone payload and remove transaction
					newPayload := *p
					newPayload.Transaction = nil
					asyncCtx = context.WithValue(stepCtx, "session_payload", &newPayload)
				}

				// Capture step for closure
				st := step
				g.Go(func() error {
					// Handle panic in async step
					defer func() {
						if r := recover(); r != nil {
							// We can't easily return the panic as error here without named return,
							// but errgroup expects an error return.
							// We'll just log it (if we had a logger) and return a generic error.
							// For now, let's assume runStep handles its own panics or we let it crash?
							// Better to recover and return error to stop the group.
						}
					}()

					if err := s.runStep(asyncCtx, st, scope, scopeMu, sb, asyncDB); err != nil {
						if st.ContinueOnError {
							// Log error but don't stop the group
							log.Error("Async step failed (continuing)", "step_type", st.Type, "error", err)
							return nil
						}
						return err // This cancels groupCtx
					}
					return nil
				})
				continue
			}
		}

		// Sync step
		if err := s.runStep(stepCtx, step, scope, scopeMu, sb, stepDB); err != nil {
			if step.ContinueOnError {
				// Log error and continue
				log.Error("Step failed (continuing)", "step_type", step.Type, "error", err)
				continue
			}
			// Stop everything
			syncErr = err
			cancel() // Cancel context for async tasks
			break
		}
		if p != nil && p.Transaction == nil {
			fmt.Println("DEBUG: runSteps - Transaction cleared after step execution!")
		}
	}

	// Wait for all async tasks
	asyncErr := g.Wait()

	if syncErr != nil {
		return syncErr
	}
	return asyncErr
}

func (s *Service) resolveTemplate(tmplStr string, scope map[string]any, scopeMu *sync.RWMutex) string {
	if tmplStr == "" {
		return ""
	}

	if scopeMu != nil {
		scopeMu.RLock()
		defer scopeMu.RUnlock()
	}

	if tmpl, err := template.New("tmpl").Parse(tmplStr); err == nil {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, scope); err == nil {
			return buf.String()
		}
	}
	return tmplStr
}
