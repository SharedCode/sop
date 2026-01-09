package agent

import (
	"context"
	"fmt"
	"strings"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

// RunnerSession holds the state for the current agent execution session,
// including script recording and transaction management.
type RunnerSession struct {
	Recording             bool
	Playback              bool   // True if a script is currently being executed
	RecordingMode         string // "standard" or "compiled"
	StopOnError           bool
	CurrentScript         *ai.Script
	CurrentScriptCategory string // Category for the script being recorded
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
	return &RunnerSession{
		RecordingMode: "compiled",
	}
}

// handleSessionCommand processes session-related commands like /record, /play, etc.
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

	// Handle Script Management Commands
	if strings.HasPrefix(query, "/script ") {
		resp, err := s.handleScriptCommand(ctx, query)
		return resp, true, err
	}

	// Handle Script Commands
	if strings.HasPrefix(query, "/record ") {
		args := strings.Fields(strings.TrimPrefix(query, "/record "))
		if len(args) == 0 {
			return "Error: Script name required", true, nil
		}

		mode := "compiled"
		stopOnError := false
		force := false
		category := "general"

		// Parse arguments
		// /record <name> [--ask] [--stop-on-error] [--force] [--category <cat>]
		var cleanArgs []string
		for i := 0; i < len(args); i++ {
			arg := args[i]
			if arg == "--ask" {
				mode = "standard"
			} else if arg == "--stop-on-error" {
				stopOnError = true
			} else if arg == "--force" {
				force = true
			} else if arg == "--category" {
				if i+1 < len(args) {
					category = args[i+1]
					i++
				}
			} else {
				cleanArgs = append(cleanArgs, arg)
			}
		}
		args = cleanArgs

		if len(args) == 0 {
			return "Error: Script name required", true, nil
		}

		name := args[0]
		if len(args) > 1 {
			return fmt.Sprintf("Error: Too many arguments. Usage: /record <name> [flags]. Found extra: %v", args[1:]), true, nil
		}

		// Check if script exists
		scriptDB := s.getScriptDB()
		if scriptDB != nil {
			tx, err := scriptDB.BeginTransaction(ctx, sop.ForReading)
			if err == nil {
				store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
				if err == nil {
					var dummy ai.Script
					if err := store.Load(ctx, category, name, &dummy); err == nil {
						// Found!
						if !force {
							tx.Rollback(ctx)
							return fmt.Sprintf("Error: Script '%s' (Category: %s) already exists. Use '/record %s --force' to overwrite.", name, category, name), true, nil
						}
					}
				}
				tx.Commit(ctx)
			}
		}

		s.session.Recording = true
		s.session.RecordingMode = mode
		s.session.StopOnError = stopOnError
		// Set script.Database to current DB if available, else leave empty for composability
		var dbName string
		if p := ai.GetSessionPayload(ctx); p != nil {
			dbName = p.CurrentDB
		}

		log.Debug(fmt.Sprintf("database: %s", dbName))

		s.session.CurrentScript = &ai.Script{
			Name:     name,
			Database: dbName,
			Steps:    []ai.ScriptStep{},
		}
		s.session.CurrentScriptCategory = category

		// We do NOT start a transaction here.
		// Recording mode uses "Auto-Commit per Step".
		// Each step will start and commit its own transaction.

		msg := fmt.Sprintf("Recording script '%s' (Mode: %s)", name, mode)
		if stopOnError {
			msg += " [Stop on Error]"
		}
		if dbName == "System DB" {
			msg += "\nWarning: You are recording in 'System DB'. This script will switch to 'System DB' when played."
		}
		return msg + "...", true, nil
	}

	if query == "/pause" {
		if s.session.CurrentScript == nil {
			return "Error: No active script recording", true, nil
		}
		s.session.Recording = false
		return "Recording paused.", true, nil
	}

	if query == "/resume" {
		if s.session.CurrentScript == nil {
			return "Error: No active script recording", true, nil
		}
		s.session.Recording = true
		return "Recording resumed.", true, nil
	}

	if query == "/stop" {
		if s.session.CurrentScript == nil {
			return "Error: Not recording", true, nil
		}
		s.session.Recording = false
		scriptDB := s.getScriptDB()
		if scriptDB != nil {
			tx, err := scriptDB.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return fmt.Sprintf("Error starting transaction: %v", err), true, nil
			}
			store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error opening store: %v", err), true, nil
			}

			log.Debug(fmt.Sprintf("saving script w/ db: %s", s.session.CurrentScript.Database))

			if err := store.Save(ctx, s.session.CurrentScriptCategory, s.session.CurrentScript.Name, s.session.CurrentScript); err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error saving script: %v", err), true, nil
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Sprintf("Error committing transaction: %v", err), true, nil
			}
			msg := fmt.Sprintf("Script '%s' (Category: %s) saved with %d steps.", s.session.CurrentScript.Name, s.session.CurrentScriptCategory, len(s.session.CurrentScript.Steps))

			// We do NOT commit any recording transaction here because we are in "Auto-Commit per Step" mode.
			// Any data changes were already committed during the step execution.
			s.session.Transaction = nil
			s.session.Variables = nil

			s.session.CurrentScript = nil
			return msg, true, nil
		}
		s.session.CurrentScript = nil
		return "Warning: No database configured, script lost.", true, nil
	}

	if strings.HasPrefix(query, "/play ") {
		parts := strings.Fields(strings.TrimPrefix(query, "/play "))
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

		// Use the shared PlayScript function
		if err := s.PlayScript(ctx, name, category, scope, &sb); err != nil {
			// The error is already logged to sb/streamer if possible, but PlayScript returns error too.
			// We append the error message if not already there?
			// PlayScript writes error to writer.
			// But handleSessionCommand expects (string, bool, error).
			// If PlayScript fails, the output is in sb.
			return sb.String(), true, nil
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

func (s *Service) handleScriptCommand(ctx context.Context, query string) (string, error) {
	args := strings.Fields(strings.TrimPrefix(query, "/script "))
	if len(args) == 0 {
		return "Usage: /script <list|show|delete|step> ...", nil
	}

	cmd := args[0]
	scriptDB := s.getScriptDB()
	if scriptDB == nil {
		return "Error: No database configured", nil
	}

	switch cmd {
	case "list":
		return s.scriptList(ctx, scriptDB, args)

	case "create":
		return s.scriptCreate(ctx, scriptDB, args)

	case "show":
		return s.scriptShow(ctx, scriptDB, args)

	case "delete":
		return s.scriptDelete(ctx, scriptDB, args)

	case "save_as":
		return s.scriptSaveAs(ctx, scriptDB, args)

	case "step":
		return s.scriptStep(ctx, scriptDB, args)

	case "parameters":
		return s.scriptParameters(ctx, scriptDB, args)

	case "parameterize":
		return s.scriptParameterize(ctx, scriptDB, args)

	case "refine":
		return s.scriptRefine(ctx, scriptDB, args)

	default:
		return "Unknown script command. Usage: /script <list|create|show|delete|step|parameters|parameterize|refine> ...", nil
	}
}
