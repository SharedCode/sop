package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

// RunnerSession holds the state for the current agent execution session,
// including script drafting and transaction management.
type RunnerSession struct {
	Playback              bool // True if a script is currently being executed
	AutoSave              bool // If true, the draft is saved to DB after every step
	CurrentScript         *ai.Script
	CurrentScriptCategory string // Category for the script being drafted
	Transaction           sop.Transaction
	CurrentDB             string         // The database the transaction is bound to
	Variables             map[string]any // Session-scoped variables (e.g. cached stores)
	LastStep              *ai.ScriptStep
	// LastInteractionSteps tracks the number of steps added/executed in the last user interaction.
	LastInteractionSteps int
	// LastInteractionToolCalls buffers the tool calls from the last interaction for refactoring.
	LastInteractionToolCalls []ai.ScriptStep

	// PendingRefinement holds the proposed changes for a script from /script refine
	PendingRefinement *RefinementProposal
}

// RefinementProposal holds the proposed changes for a script.
type RefinementProposal struct {
	ScriptName     string
	Category       string
	OriginalScript ai.Script
	NewScript      ai.Script
	Description    string   // The new summary description
	NewParams      []string // List of new parameters
	Replacements   []string // Human readable list of replacements
}

// NewRunnerSession creates a new runner session.
func NewRunnerSession() *RunnerSession {
	return &RunnerSession{}
}

// handleSessionCommand processes session-related commands like /create, /save, /step, etc.
// Returns (response, handled, error)
func (s *Service) handleSessionCommand(ctx context.Context, query string, db *database.Database) (string, bool, error) {
	// Handle last-tool command (support both "last-tool" and "/last-tool")
	if query == "last-tool" || query == "/last-tool" {
		instructions := s.GetLastToolInstructions()
		if instructions == "" {
			return "No tool instructions found.", true, nil
		}
		return fmt.Sprintf("Last Tool Instructions\n```json\n%s\n```", instructions), true, nil
	}

	// Script Drafting Commands

	// /create <name> [category]
	// Starts a new script draft.
	if strings.HasPrefix(query, "/create ") {
		parts := strings.Fields(strings.TrimPrefix(query, "/create "))
		if len(parts) == 0 {
			return "Error: Script name required", true, nil
		}
		name := parts[0]
		category := "general"
		autoSave := false

		for i := 1; i < len(parts); i++ {
			if parts[i] == "--category" && i+1 < len(parts) {
				category = parts[i+1]
				i++
			} else if parts[i] == "--autosave" {
				autoSave = true
			}
		}

		// Check if script already exists (warning only, as we are drafting)
		scriptDB := s.getScriptDB()
		exists := false
		if scriptDB != nil {
			tx, err := scriptDB.BeginTransaction(ctx, sop.ForReading)
			if err == nil {
				store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
				if err == nil {
					var dummy ai.Script
					if err := store.Load(ctx, category, name, &dummy); err == nil {
						exists = true
					}
				}
				tx.Commit(ctx)
			}
		}

		// Capture current database from session or argument
		// We prioritize what's in the payload/session as the user is "drafting here"
		var currentDB string
		if s.session.CurrentDB != "" {
			currentDB = s.session.CurrentDB
		} else if p := ai.GetSessionPayload(ctx); p != nil {
			currentDB = p.CurrentDB
		}

		s.session.CurrentScript = &ai.Script{
			Name:     name,
			Database: currentDB,
			Steps:    []ai.ScriptStep{},
		}
		s.session.CurrentScriptCategory = category
		s.session.AutoSave = autoSave

		msg := fmt.Sprintf("Started drafting script '%s' (Category: %s).", name, category)
		if autoSave {
			msg += " [Auto-Save Enabled]"
		}
		if exists {
			msg += "\nWarning: A script with this name already exists. Saving will overwrite it."
		}
		return msg, true, nil
	}

	// /step [instruction...]
	// Adds the last executed step or a new instruction to the current draft.
	if strings.HasPrefix(query, "/step") {
		if s.session.CurrentScript == nil {
			return "Error: No active script draft. Use '/create <name>' to start.", true, nil
		}

		args := strings.Fields(strings.TrimPrefix(query, "/step"))
		msg := ""
		if len(args) > 0 {
			// Add explicit instruction
			// /step This is a comment or instruction
			instruction := strings.TrimSpace(strings.TrimPrefix(query, "/step"))
			step := ai.ScriptStep{
				Type:    "ask", // Default to 'ask' / natural language instruction
				Prompt:  instruction,
				Message: instruction,
			}
			s.session.CurrentScript.Steps = append(s.session.CurrentScript.Steps, step)
			msg = fmt.Sprintf("Added step %d: %s", len(s.session.CurrentScript.Steps), instruction)
		} else {
			// Add last executed step
			if s.session.LastStep == nil {
				return "Error: No previous step available to add.", true, nil
			}

			// Check if the last step in the script is identical to the one we're about to add.
			// This prevents duplication when the step was already auto-recorded.
			if len(s.session.CurrentScript.Steps) > 0 {
				lastRecorded := s.session.CurrentScript.Steps[len(s.session.CurrentScript.Steps)-1]
				if lastRecorded.Type == s.session.LastStep.Type &&
					lastRecorded.Command == s.session.LastStep.Command &&
					reflect.DeepEqual(lastRecorded.Args, s.session.LastStep.Args) {
					return "Step already recorded.", true, nil
				}
			}

			// Copy the step to ensure we don't modify the session record
			newStep := *s.session.LastStep

			// Defensive: Ensure Type is set correctly.
			// History shows Type might be empty or lost, causing it to run as "ask".
			if newStep.Type == "" && newStep.Command != "" {
				newStep.Type = "command"
			}

			s.session.CurrentScript.Steps = append(s.session.CurrentScript.Steps, newStep)
			msg = fmt.Sprintf("Added step %d (from last command).", len(s.session.CurrentScript.Steps))
		}

		if s.session.AutoSave {
			if err := s.saveDraft(ctx); err != nil {
				return fmt.Sprintf("%s\nWarning: Auto-save failed: %v", msg, err), true, nil
			}
			msg += " (Auto-saved)"
		}

		return msg, true, nil
	}

	// /save OR /end
	// Saves the current draft to the database and ends the drafting helper.
	if query == "/save" || query == "/end" {
		if s.session.CurrentScript == nil {
			return "Error: No active script draft.", true, nil
		}

		// Optimization: If autosave is enabled, the script is already saved.
		if s.session.AutoSave {
			msg := fmt.Sprintf("Script '%s' is up-to-date (autosave enabled). Drafting ended.", s.session.CurrentScript.Name)
			s.session.CurrentScript = nil
			s.session.CurrentScriptCategory = ""
			s.session.AutoSave = false
			return msg, true, nil
		}

		if err := s.saveDraft(ctx); err != nil {
			return fmt.Sprintf("Error saving script: %v", err), true, nil
		}

		msg := fmt.Sprintf("Script '%s' saved successfully with %d steps. Drafting ended.", s.session.CurrentScript.Name, len(s.session.CurrentScript.Steps))

		// Clear the draft after saving
		s.session.CurrentScript = nil
		s.session.CurrentScriptCategory = ""
		s.session.AutoSave = false // Reset autosave flag

		return msg, true, nil
	}

	// Handle Script Execution & Management

	// /show <name> [--category <cat>] [--json]
	// Shows the content of a saved script.
	if strings.HasPrefix(query, "/show ") {
		parts := strings.Fields(strings.TrimPrefix(query, "/show "))
		if len(parts) == 0 {
			return "Error: Script name required", true, nil
		}
		name := parts[0]
		category := "general"
		showJson := false

		for i := 1; i < len(parts); i++ {
			if parts[i] == "--category" && i+1 < len(parts) {
				category = parts[i+1]
				i++
			} else if parts[i] == "--json" {
				showJson = true
			}
		}

		scriptDB := s.getScriptDB()
		if scriptDB == nil {
			return "Error: No database configured", true, nil
		}

		tx, err := scriptDB.BeginTransaction(ctx, sop.ForReading)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), true, nil
		}

		store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), true, nil
		}

		var script ai.Script
		if err := store.Load(ctx, category, name, &script); err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error: Script '%s' (Category: %s) not found.", name, category), true, nil
		}
		tx.Commit(ctx)

		if showJson {
			b, _ := json.MarshalIndent(script, "", "  ")
			return fmt.Sprintf("```json\n%s\n```", string(b)), true, nil
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Script: %s (Category: %s)\n", script.Name, category))
		if script.Description != "" {
			sb.WriteString(fmt.Sprintf("Description: %s\n", script.Description))
		}
		if len(script.Parameters) > 0 {
			sb.WriteString(fmt.Sprintf("Parameters: %v\n", script.Parameters))
		}
		sb.WriteString("Steps:\n")
		for i, step := range script.Steps {
			prompt := step.Prompt
			if prompt == "" {
				prompt = step.Message // Fallback
			}
			if step.Type == "command" {
				prompt = step.Command
				if len(step.Args) > 0 {
					argBytes, _ := json.Marshal(step.Args)
					prompt += " " + string(argBytes)
				}
			}
			if step.Type == "call_script" {
				prompt = fmt.Sprintf("Run '%s'", step.ScriptName)
			}
			sb.WriteString(fmt.Sprintf("%d. [%s] %s\n", i+1, step.Type, prompt))
		}
		return sb.String(), true, nil
	}

	// /refine <name> [feedback] | /refine apply | /refine cancel
	// Uses AI to improve a script or applies pending improvements.
	if strings.HasPrefix(query, "/refine ") {
		args := strings.Fields(strings.TrimPrefix(query, "/refine "))
		scriptDB := s.getScriptDB()
		if scriptDB == nil {
			return "Error: No database configured", true, nil
		}
		msg, err := s.scriptRefine(ctx, scriptDB, append([]string{"refine"}, args...))
		if err != nil {
			return fmt.Sprintf("Error: %v", err), true, nil
		}
		return msg, true, nil
	}

	// /parameterize <name> <param_name> <value_to_replace> [--category <cat>]
	// Replaces hardcoded values in a script with parameters.
	if strings.HasPrefix(query, "/parameterize ") {
		args := strings.Fields(strings.TrimPrefix(query, "/parameterize "))
		scriptDB := s.getScriptDB()
		if scriptDB == nil {
			return "Error: No database configured", true, nil
		}
		msg, err := s.scriptParameterize(ctx, scriptDB, append([]string{"parameterize"}, args...))
		if err != nil {
			return fmt.Sprintf("Error: %v", err), true, nil
		}
		return msg, true, nil
	}

	// /save_as <new_name> [--category <cat>]
	// Saves the last executed tool call as a new script (Shortcut).
	if strings.HasPrefix(query, "/save_as ") {
		args := strings.Fields(strings.TrimPrefix(query, "/save_as "))
		scriptDB := s.getScriptDB()
		if scriptDB == nil {
			return "Error: No database configured", true, nil
		}
		msg, err := s.scriptSaveAs(ctx, scriptDB, append([]string{"save_as"}, args...))
		if err != nil {
			return fmt.Sprintf("Error: %v", err), true, nil
		}
		return msg, true, nil
	}

	// /insert_step <script_name> <index> [before|after]
	// Inserts the last executed step into an existing script.
	if strings.HasPrefix(query, "/insert_step ") {
		if s.session.LastStep == nil {
			return "Error: No previous step available to insert.", true, nil
		}

		parts := strings.Fields(strings.TrimPrefix(query, "/insert_step "))
		if len(parts) < 2 {
			return "Usage: /insert_step <script_name> <index> [before|after]", true, nil
		}
		scriptName := parts[0]

		idx, err := strconv.Atoi(parts[1])
		if err != nil {
			return "Error: Index must be a number.", true, nil
		}
		// Adjust 1-based index from user to 0-based
		idx = idx - 1

		position := "after"
		if len(parts) > 2 {
			position = parts[2]
		}

		scriptDB := s.getScriptDB()
		if scriptDB == nil {
			return "Error: No database configured", true, nil
		}

		tx, err := scriptDB.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), true, nil
		}
		defer tx.Rollback(ctx)

		store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
		if err != nil {
			return fmt.Sprintf("Error opening store: %v", err), true, nil
		}

		var script ai.Script
		if err := store.Load(ctx, "general", scriptName, &script); err != nil {
			return fmt.Sprintf("Error loading script '%s': %v", scriptName, err), true, nil
		}

		newStep := *s.session.LastStep
		if newStep.Type == "" && newStep.Command != "" {
			newStep.Type = "command"
		}

		// Insert logic
		if idx < 0 {
			idx = 0
		}

		if position == "before" {
			if idx > len(script.Steps) {
				idx = len(script.Steps)
			}
			script.Steps = append(script.Steps[:idx], append([]ai.ScriptStep{newStep}, script.Steps[idx:]...)...)
		} else {
			// after (default)
			if idx >= len(script.Steps) {
				script.Steps = append(script.Steps, newStep)
			} else {
				script.Steps = append(script.Steps[:idx+1], append([]ai.ScriptStep{newStep}, script.Steps[idx+1:]...)...)
			}
		}

		if err := store.Save(ctx, "general", scriptName, &script); err != nil {
			return fmt.Sprintf("Error saving script: %v", err), true, nil
		}
		tx.Commit(ctx)
		return fmt.Sprintf("Step added to script '%s'.", scriptName), true, nil
	}

	if strings.HasPrefix(query, "/run ") {
		parts := strings.Fields(strings.TrimPrefix(query, "/run "))
		if len(parts) == 0 {
			return "Error: Script name required", true, nil
		}
		name := parts[0]
		category := "general"
		var rawArgs []string

		for i := 1; i < len(parts); i++ {
			arg := parts[i]
			if arg == "--category" && i+1 < len(parts) {
				category = parts[i+1]
				i++
				continue
			}
			rawArgs = append(rawArgs, arg)
		}

		scriptDB := s.getScriptDB()
		if scriptDB == nil {
			return "Error: No database configured", true, nil
		}

		tx, err := scriptDB.BeginTransaction(ctx, sop.ForReading)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), true, nil
		}

		store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), true, nil
		}

		var script ai.Script
		if err := store.Load(ctx, category, name, &script); err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error loading script: %v", err), true, nil
		}
		tx.Commit(ctx)

		// Process arguments (Named and Positional)
		args := make(map[string]string)

		// First pass: Extract named arguments
		var positionalArgs []string
		for _, arg := range rawArgs {
			kv := strings.SplitN(arg, "=", 2)
			if len(kv) == 2 {
				args[kv[0]] = kv[1]
			} else {
				positionalArgs = append(positionalArgs, arg)
			}
		}

		// Second pass: Map positional arguments to script parameters
		for i, val := range positionalArgs {
			if i < len(script.Parameters) {
				paramName := script.Parameters[i]
				// Only set if not already set by named arg (Named takes precedence? Or Positional? Usually Named overrides)
				// But here, let's say if you provide both, Named wins.
				if _, exists := args[paramName]; !exists {
					args[paramName] = val
				}
			}
		}

		// Validation: Check if all parameters are satisfied
		var missingParams []string
		for _, param := range script.Parameters {
			if _, ok := args[param]; !ok {
				missingParams = append(missingParams, param)
			}
		}
		if len(missingParams) > 0 {
			return fmt.Sprintf("Error: Missing required parameters: %v", missingParams), true, nil
		}

		var sb strings.Builder
		// Convert args to map[string]any
		scope := make(map[string]any)
		for k, v := range args {
			scope[k] = v
		}

		// Check for streaming writer in context
		var w io.Writer = &sb
		if ctxW, ok := ctx.Value(ai.CtxKeyWriter).(io.Writer); ok {
			w = ctxW
			ctx = context.WithValue(ctx, CtxKeyUseNDJSON, true)
		}

		// Use the shared PlayScript function
		if err := s.PlayScript(ctx, name, category, scope, w); err != nil {
			// The error is already logged to sb/streamer if possible, but PlayScript returns error too.
			// We append the error message if not already there?
			// PlayScript writes error to writer.
			// But handleSessionCommand expects (string, bool, error).
			// If PlayScript fails, the output is in sb.
			if w == &sb {
				return sb.String(), true, nil
			}
			return fmt.Sprintf("Error: %v", err), true, nil
		}

		if w != &sb {
			return "", true, nil
		}

		// Parse the JSON output from PlayScript and format it nicely
		output := sb.String()
		if strings.HasPrefix(strings.TrimSpace(output), "[") {
			var results []map[string]any
			if err := json.Unmarshal([]byte(output), &results); err == nil {
				// We want to return the actual execution results, not the wrapper.
				// If there are multiple results (multiple records), we should return a list.
				// If there is just one step outputting a list, return that list.

				var allRecords []any

				for _, res := range results {
					if val, ok := res["result"]; ok && val != nil {
						// Flatten list results
						if list, ok := val.([]any); ok {
							allRecords = append(allRecords, list...)
						} else {
							allRecords = append(allRecords, val)
						}
					} else if errMsg, ok := res["error"]; ok {
						// Return error string directly if it's an error result
						return fmt.Sprintf("Error: %v", errMsg), true, nil
					}
				}

				if len(allRecords) > 0 {
					// Encode as JSON string so main.ai.go detects it as data
					b, _ := json.MarshalIndent(allRecords, "", "  ")
					return string(b), true, nil
				}
			}
		}

		return sb.String(), true, nil
	}

	if strings.HasPrefix(query, "/delete ") {
		parts := strings.Fields(strings.TrimPrefix(query, "/delete "))
		if len(parts) == 0 {
			return "Error: Script name required", true, nil
		}
		name := parts[0]
		category := "general"

		for i := 1; i < len(parts); i++ {
			if parts[i] == "--category" && i+1 < len(parts) {
				category = parts[i+1]
				i++
			}
		}

		scriptDB := s.getScriptDB()
		if scriptDB == nil {
			return "Error: No database configured", true, nil
		}

		tx, err := scriptDB.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), true, nil
		}

		store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), true, nil
		}

		var dummy ai.Script
		if err := store.Load(ctx, category, name, &dummy); err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error: Script '%s' (Category: %s) not found.", name, category), true, nil
		}

		if err := store.Delete(ctx, category, name); err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error deleting script: %v", err), true, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Sprintf("Error committing transaction: %v", err), true, nil
		}
		return fmt.Sprintf("Script '%s' (Category: %s) deleted.", name, category), true, nil
	}

	if strings.HasPrefix(query, "/list") {
		args := strings.Fields(query)
		category := "general"
		for i := 1; i < len(args); i++ {
			if args[i] == "--category" && i+1 < len(args) {
				category = args[i+1]
				i++
			}
		}

		scriptDB := s.getScriptDB()
		if scriptDB == nil {
			return "Error: No database configured", true, nil
		}

		tx, err := scriptDB.BeginTransaction(ctx, sop.ForReading)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), true, nil
		}

		store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), true, nil
		}

		names, err := store.List(ctx, category)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error listing scripts: %v", err), true, nil
		}
		tx.Commit(ctx)

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Available Scripts (Category: %s):\n", category))
		for _, n := range names {
			sb.WriteString(fmt.Sprintf("- %s\n", n))
		}
		return sb.String(), true, nil
	}

	return "", false, nil
}

func (s *Service) saveDraft(ctx context.Context) error {
	if s.session.CurrentScript == nil {
		return fmt.Errorf("no active script draft")
	}
	scriptDB := s.getScriptDB()
	if scriptDB == nil {
		return fmt.Errorf("no database configured")
	}

	tx, err := scriptDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("transaction start failed: %w", err)
	}

	store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("store open failed: %w", err)
	}

	if err := store.Save(ctx, s.session.CurrentScriptCategory, s.session.CurrentScript.Name, *s.session.CurrentScript); err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("save failed: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	return nil
}
