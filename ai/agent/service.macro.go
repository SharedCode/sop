package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"io"
	"sync"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

// PlayMacro executes a macro by name with provided arguments and streams the output to the writer.
func (s *Service) PlayMacro(ctx context.Context, name string, category string, args map[string]any, w io.Writer) error {
	macroDB := s.getMacroDB()
	if macroDB == nil {
		return fmt.Errorf("no database configured")
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}

	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("error opening store: %v", err)
	}

	var macro ai.Macro
	if err := store.Load(ctx, category, name, &macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("error loading macro: %v", err)
	}
	tx.Commit(ctx)

	// Validation: Check if all parameters are satisfied
	var missingParams []string
	for _, param := range macro.Parameters {
		if _, ok := args[param]; !ok {
			missingParams = append(missingParams, param)
		}
	}
	if len(missingParams) > 0 {
		return fmt.Errorf("missing required parameters: %v", missingParams)
	}

	var scopeMu sync.RWMutex

	// Handle Database Switching for Macro
	var macroCtx context.Context = ctx
	var db *database.Database = s.systemDB

	// Initialize streamer for structured output
	streamer := NewJSONStreamer(w)
	fmt.Fprint(w, "[\n") // Start JSON array
	macroCtx = context.WithValue(macroCtx, CtxKeyJSONStreamer, streamer)

	if macro.Database != "" && !macro.Portable {
		// Resolve Database from Service Options
		if opts, ok := s.databases[macro.Database]; ok {
			targetDB := database.NewDatabase(opts)
			db = targetDB

			// Update Payload in Context
			if p := ai.GetSessionPayload(ctx); p != nil {
				newPayload := *p
				newPayload.CurrentDB = macro.Database
				newPayload.Transaction = nil // Ensure Open starts a new one
				macroCtx = context.WithValue(macroCtx, "session_payload", &newPayload)
			}
		} else {
			return fmt.Errorf("macro '%s' requires database '%s' which is not configured", name, macro.Database)
		}
	} else {
		// If portable or no DB specified, use the DB from context payload if available
		if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" {
			if opts, ok := s.databases[p.CurrentDB]; ok {
				db = database.NewDatabase(opts)
			}
		}
	}

	// Lifecycle Management for Macro Execution
	if err := s.Open(macroCtx); err != nil {
		return fmt.Errorf("error initializing session: %v", err)
	}
	// Ensure we close the session (commit transaction)
	defer func() {
		if err := s.Close(macroCtx); err != nil {
			fmt.Fprintf(w, "\nError closing session: %v", err)
		}
		// Safety: If the macro left a transaction open (Explicitly), rollback it now.
		if s.session.Transaction != nil {
			if tx, ok := s.session.Transaction.(sop.Transaction); ok {
				_ = tx.Rollback(macroCtx)
			}
			s.session.Transaction = nil
			s.session.Variables = nil
			fmt.Fprint(w, "\nWarning: Uncommitted transaction was automatically rolled back for safety.")
		}
		fmt.Fprint(w, "\n]") // End JSON array
	}()

	// Capture transaction to prevent loss during execution
	var preservedTx any
	if p := ai.GetSessionPayload(macroCtx); p != nil {
		preservedTx = p.Transaction
	}

	// We need a strings.Builder for the executeMacro signature, but we want to stream.
	// The executeMacro function writes to sb, but also uses the streamer in context.
	// We can pass a dummy builder or one that we discard, since we rely on the streamer.
	// However, executeMacro might write non-JSON text to sb.
	// Let's pass a builder that writes to our writer? No, executeMacro takes *strings.Builder.
	// We will just capture the text output in a builder, but the structured output goes to streamer.
	var sb strings.Builder

	if err := s.executeMacro(macroCtx, &macro, args, &scopeMu, &sb, db); err != nil {
		errMsg := fmt.Sprintf("Error executing macro: %v", err)

		// Also add to streamer
		streamer.Write(StepExecutionResult{
			Type:  "error",
			Error: errMsg,
		})

		// If error, we might want to rollback.
		if p := ai.GetSessionPayload(macroCtx); p != nil && p.Transaction != nil {
			if tx, ok := p.Transaction.(sop.Transaction); ok {
				tx.Rollback(macroCtx)
				p.Transaction = nil // Prevent Close from committing
			}
		}
		return fmt.Errorf("execution error: %v", err)
	}

	if p := ai.GetSessionPayload(macroCtx); p != nil {
		// Restore transaction if it was lost
		if p.Transaction == nil && preservedTx != nil {
			p.Transaction = preservedTx
		}
	}

	return nil
}

// RunMacro executes a macro by name with provided arguments and returns the output as a string.
// This is a convenience wrapper around PlayMacro for non-streaming use cases.
func (s *Service) RunMacro(ctx context.Context, name string, category string, args map[string]any) (string, error) {
	var sb strings.Builder
	err := s.PlayMacro(ctx, name, category, args, &sb)
	return sb.String(), err
}

func (s *Service) macroList(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	category := "general"
	for i := 1; i < len(args); i++ {
		if args[i] == "--category" && i+1 < len(args) {
			category = args[i+1]
			i++
		}
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}
	names, err := store.List(ctx, category)
	tx.Commit(ctx)
	if err != nil {
		return fmt.Sprintf("Error listing macros: %v", err), nil
	}
	if len(names) == 0 {
		return fmt.Sprintf("No macros found in category '%s'.", category), nil
	}
	return fmt.Sprintf("Macros (Category: %s):\n- %s", category, strings.Join(names, "\n- ")), nil
}

func (s *Service) macroCreate(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	// /macro create <name> [description] [--category <cat>] [--force]
	if len(args) < 2 {
		return "Usage: /macro create <name> [description] [--category <cat>] [--force]", nil
	}
	name := args[1]
	description := ""
	category := "general"
	force := false

	for i := 2; i < len(args); i++ {
		if args[i] == "--category" && i+1 < len(args) {
			category = args[i+1]
			i++
		} else if args[i] == "--force" {
			force = true
		} else {
			if description == "" {
				description = args[i]
			} else {
				description += " " + args[i]
			}
		}
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}

	// Check if exists
	var dummy ai.Macro
	if err := store.Load(ctx, category, name, &dummy); err == nil {
		if !force {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error: Macro '%s' (Category: %s) already exists. Use --force to overwrite.", name, category), nil
		}
	}

	// Create new macro
	// We default to current DB if available in payload, else empty
	var dbName string
	if p := ai.GetSessionPayload(ctx); p != nil {
		dbName = p.CurrentDB
	}

	newMacro := ai.Macro{
		Name:        name,
		Description: description,
		Database:    dbName,
		Steps:       []ai.MacroStep{},
	}

	if err := store.Save(ctx, category, name, newMacro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Sprintf("Error committing transaction: %v", err), nil
	}

	return fmt.Sprintf("Macro '%s' created successfully.", name), nil
}

func (s *Service) macroShow(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	if len(args) < 2 {
		return "Usage: /macro show <name> [--json] [--category <cat>]", nil
	}
	name := args[1]
	category := "general"
	showJSON := false

	for i := 2; i < len(args); i++ {
		if args[i] == "--json" {
			showJSON = true
		} else if args[i] == "--category" && i+1 < len(args) {
			category = args[i+1]
			i++
		}
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}
	var macro ai.Macro
	err = store.Load(ctx, category, name, &macro)
	tx.Commit(ctx)
	if err != nil {
		return fmt.Sprintf("Error loading macro: %v", err), nil
	}

	if showJSON {
		b, err := json.MarshalIndent(macro, "", "  ")
		if err != nil {
			return fmt.Sprintf("Error marshaling macro: %v", err), nil
		}
		return fmt.Sprintf("```json\n%s\n```", string(b)), nil
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Macro: %s\n", macro.Name))
	if len(macro.Parameters) > 0 {
		sb.WriteString(fmt.Sprintf("Parameters: %v\n", macro.Parameters))
	}
	if macro.Description != "" {
		sb.WriteString(fmt.Sprintf("Description: %s\n", macro.Description))
	}
	for i, step := range macro.Steps {
		desc := step.Message
		if step.Type == "ask" {
			desc = step.Prompt
		} else if step.Type == "macro" {
			desc = fmt.Sprintf("Run '%s'", step.MacroName)
		} else if step.Type == "command" {
			argsJSON, _ := json.Marshal(step.Args)
			desc = fmt.Sprintf("Execute '%s' %s", step.Command, string(argsJSON))
		}
		sb.WriteString(fmt.Sprintf("%d. [%s] %s", i+1, step.Type, desc))
		sb.WriteString("\n")
	}
	return sb.String(), nil
}

func (s *Service) macroDelete(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	if len(args) < 2 {
		return "Usage: /macro delete <name> [--category <cat>]", nil
	}
	name := args[1]
	category := "general"
	if len(args) > 3 && args[2] == "--category" {
		category = args[3]
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}

	var dummy ai.Macro
	if err := store.Load(ctx, category, name, &dummy); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error: Macro '%s' (Category: %s) not found.", name, category), nil
	}

	err = store.Delete(ctx, category, name)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error deleting macro: %v", err), nil
	}
	tx.Commit(ctx)
	return fmt.Sprintf("Macro '%s' (Category: %s) deleted.", name, category), nil
}

func (s *Service) macroSaveAs(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	if len(args) < 2 {
		return "Usage: /macro save_as <name> [--category <cat>]", nil
	}
	name := args[1]
	category := "general"
	if len(args) > 3 && args[2] == "--category" {
		category = args[3]
	}

	if s.session.LastStep == nil {
		return "Error: No previous step available to save. Run a command first.", nil
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}

	// Check if macro exists
	var dummy ai.Macro
	if err := store.Load(ctx, category, name, &dummy); err == nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error: Macro '%s' (Category: %s) already exists. Use '/macro delete %s' first.", name, category, name), nil
	}

	newMacro := ai.Macro{
		Name:  name,
		Steps: []ai.MacroStep{*s.session.LastStep},
	}

	if err := store.Save(ctx, category, name, newMacro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}
	tx.Commit(ctx)
	return fmt.Sprintf("Macro '%s' (Category: %s) created from last step.", name, category), nil
}

func (s *Service) macroStep(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	if len(args) < 3 {
		return "Usage: /macro step <add|delete|update> <macro_name> ... [--category <cat>]", nil
	}
	subCmd := args[1]
	name := args[2]

	category := "general"
	var cleanArgs []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--category" && i+1 < len(args) {
			category = args[i+1]
			i++
		} else {
			cleanArgs = append(cleanArgs, args[i])
		}
	}
	args = cleanArgs

	if subCmd == "add" {
		return s.macroStepAdd(ctx, macroDB, name, category, args)
	}

	if subCmd == "delete" {
		return s.macroStepDelete(ctx, macroDB, name, category, args)
	}

	if subCmd == "update" {
		return s.macroStepUpdate(ctx, macroDB, name, category, args)
	}

	return "Unknown step command. Usage: /macro step <delete|add|update> ...", nil
}

func (s *Service) macroStepAdd(ctx context.Context, macroDB *database.Database, name string, category string, args []string) (string, error) {
	// /macro step add <macro_name> <position> [target_index]
	if len(args) < 4 {
		return "Usage: /macro step add <macro_name> <position> [target_index]", nil
	}
	if s.session.LastStep == nil {
		return "Error: No previous step available to add. Run a command first.", nil
	}

	position := args[3]
	targetIdx := -1
	if position == "before" || position == "after" {
		if len(args) < 5 {
			return "Usage: /macro step add <macro_name> <before|after> <target_index>", nil
		}
		var err error
		targetIdx, err = strconv.Atoi(args[4])
		if err != nil || targetIdx < 1 {
			return "Error: Invalid target index", nil
		}
		targetIdx-- // 0-based
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}
	var macro ai.Macro
	if err := store.Load(ctx, category, name, &macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error loading macro: %v", err), nil
	}

	newStep := *s.session.LastStep

	switch position {
	case "top":
		macro.Steps = append([]ai.MacroStep{newStep}, macro.Steps...)
	case "bottom":
		macro.Steps = append(macro.Steps, newStep)
	case "before":
		if targetIdx >= len(macro.Steps) {
			tx.Rollback(ctx)
			return "Error: Target index out of range", nil
		}
		macro.Steps = append(macro.Steps[:targetIdx], append([]ai.MacroStep{newStep}, macro.Steps[targetIdx:]...)...)
	case "after":
		if targetIdx >= len(macro.Steps) {
			tx.Rollback(ctx)
			return "Error: Target index out of range", nil
		}
		// Insert after targetIdx (so at targetIdx + 1)
		targetIdx++
		macro.Steps = append(macro.Steps[:targetIdx], append([]ai.MacroStep{newStep}, macro.Steps[targetIdx:]...)...)
	default:
		tx.Rollback(ctx)
		return "Error: Invalid position. Use top, bottom, before, or after.", nil
	}

	if err := store.Save(ctx, category, name, macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}
	tx.Commit(ctx)
	return fmt.Sprintf("Step added to macro '%s' (Category: %s) at %s.", name, category, position), nil
}

func (s *Service) macroStepDelete(ctx context.Context, macroDB *database.Database, name string, category string, args []string) (string, error) {
	if len(args) < 4 {
		return "Usage: /macro step delete <macro_name> <step_index>", nil
	}
	idxStr := args[3]
	idx, err := strconv.Atoi(idxStr)
	if err != nil || idx < 1 {
		return "Error: Invalid step index", nil
	}
	// Adjust to 0-based
	idx--

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}
	var macro ai.Macro
	if err := store.Load(ctx, category, name, &macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error loading macro: %v", err), nil
	}

	if idx >= len(macro.Steps) {
		tx.Rollback(ctx)
		return "Error: Step index out of range", nil
	}

	// Remove step
	macro.Steps = append(macro.Steps[:idx], macro.Steps[idx+1:]...)

	if err := store.Save(ctx, category, name, macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}
	tx.Commit(ctx)
	return fmt.Sprintf("Step %d deleted from macro '%s' (Category: %s).", idx+1, name, category), nil
}

func (s *Service) macroStepUpdate(ctx context.Context, macroDB *database.Database, name string, category string, args []string) (string, error) {
	// /macro step update <macro_name> <step_index>
	if len(args) < 4 {
		return "Usage: /macro step update <macro_name> <step_index>", nil
	}
	if s.session.LastStep == nil {
		return "Error: No previous step available to update with. Run a command first.", nil
	}

	idxStr := args[3]
	idx, err := strconv.Atoi(idxStr)
	if err != nil || idx < 1 {
		return "Error: Invalid step index", nil
	}
	idx-- // 0-based

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}
	var macro ai.Macro
	if err := store.Load(ctx, category, name, &macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error loading macro: %v", err), nil
	}

	if idx >= len(macro.Steps) {
		tx.Rollback(ctx)
		return "Error: Step index out of range", nil
	}

	// Update step
	macro.Steps[idx] = *s.session.LastStep

	if err := store.Save(ctx, category, name, macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}
	tx.Commit(ctx)
	return fmt.Sprintf("Step %d updated in macro '%s' (Category: %s).", idx+1, name, category), nil
}

func (s *Service) macroParameters(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	// /macro parameters <name> <p1> <p2> ... [--category <cat>]
	if len(args) < 2 {
		return "Usage: /macro parameters <name> <p1> <p2> ... [--category <cat>]", nil
	}
	name := args[1]
	category := "general"
	var params []string

	for i := 2; i < len(args); i++ {
		if args[i] == "--category" && i+1 < len(args) {
			category = args[i+1]
			i++
		} else {
			params = append(params, args[i])
		}
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}

	var macro ai.Macro
	if err := store.Load(ctx, category, name, &macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error loading macro: %v", err), nil
	}

	macro.Parameters = params

	if err := store.Save(ctx, category, name, macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}
	tx.Commit(ctx)

	if len(params) == 0 {
		return fmt.Sprintf("Parameters cleared for macro '%s' (Category: %s).", name, category), nil
	}
	return fmt.Sprintf("Parameters updated for macro '%s' (Category: %s): %v", name, category, params), nil
}

func (s *Service) macroParameterize(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	// /macro parameterize <name> <param_name> <value_to_replace> [--category <cat>]
	if len(args) < 4 {
		return "Usage: /macro parameterize <name> <param_name> <value_to_replace> [--category <cat>]", nil
	}
	name := args[1]
	paramName := args[2]
	valueToReplace := args[3]
	category := "general"

	for i := 4; i < len(args); i++ {
		if args[i] == "--category" && i+1 < len(args) {
			category = args[i+1]
			i++
		}
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}

	var macro ai.Macro
	if err := store.Load(ctx, category, name, &macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error loading macro: %v", err), nil
	}

	// 1. Add parameter to list if not exists
	exists := false
	for _, p := range macro.Parameters {
		if p == paramName {
			exists = true
			break
		}
	}
	if !exists {
		macro.Parameters = append(macro.Parameters, paramName)
	}

	// 2. Scan all steps and replace valueToReplace with {{.paramName}}
	count := 0
	templateVar := fmt.Sprintf("{{.%s}}", paramName)

	for i := range macro.Steps {
		step := &macro.Steps[i]

		// Helper to replace in string
		replace := func(s string) string {
			if strings.Contains(s, valueToReplace) {
				count++
				return strings.ReplaceAll(s, valueToReplace, templateVar)
			}
			return s
		}

		step.Prompt = replace(step.Prompt)
		step.Message = replace(step.Message)
		step.Value = replace(step.Value)
		step.Condition = replace(step.Condition)
		step.List = replace(step.List)

		// Replace in Args (recursive for maps?)
		// For now, just top-level string args
		for k, v := range step.Args {
			if strVal, ok := v.(string); ok {
				if strings.Contains(strVal, valueToReplace) {
					step.Args[k] = strings.ReplaceAll(strVal, valueToReplace, templateVar)
					count++
				}
			}
		}

		// Replace in MacroArgs (for nested macros)
		for k, v := range step.MacroArgs {
			if strings.Contains(v, valueToReplace) {
				step.MacroArgs[k] = strings.ReplaceAll(v, valueToReplace, templateVar)
				count++
			}
		}
	}

	if err := store.Save(ctx, category, name, macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}
	tx.Commit(ctx)

	return fmt.Sprintf("Macro '%s' parameterized. Replaced %d occurrences of '%s' with '{{.%s}}'. Parameter '%s' added.", name, count, valueToReplace, paramName, paramName), nil
}

func (s *Service) macroRefine(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	// /macro refine <name> [instructions...] [--category <cat>]
	// Subcommands: apply, cancel
	if len(args) >= 2 && args[1] == "apply" {
		return s.macroRefineApply(ctx, macroDB)
	}
	if len(args) >= 2 && args[1] == "cancel" {
		s.session.PendingRefinement = nil
		return "Refinement cancelled.", nil
	}

	if len(args) < 2 {
		return "Usage: /macro refine <name> [instructions...] [--category <cat>]", nil
	}

	name := args[1]
	category := "general"
	var instructionsParts []string

	for i := 2; i < len(args); i++ {
		if args[i] == "--category" && i+1 < len(args) {
			category = args[i+1]
			i++
		} else {
			instructionsParts = append(instructionsParts, args[i])
		}
	}
	instructions := strings.Join(instructionsParts, " ")

	// 1. Load Macro
	tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}
	var macro ai.Macro
	if err := store.Load(ctx, category, name, &macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error: Macro '%s' (Category: %s) not found.", name, category), nil
	}
	tx.Commit(ctx)

	// 2. Prepare LLM Prompt
	macroJSON, _ := json.MarshalIndent(macro, "", "  ")
	prompt := fmt.Sprintf(`You are an expert macro optimizer. Your task is to refine the following macro.

Macro JSON:
%s

User Instructions: "%s"

Tasks:
1. Identify hardcoded values in the steps that should be parameters (e.g., specific regions, dates, IDs).
   - If the user provided instructions, follow them strictly for parameterization.
   - If no instructions, use your best judgment to identify likely variables.
2. Generate a concise "Summary Description" (1-2 sentences) for the macro metadata.
3. Return a JSON object with the following structure:
{
  "summary": "Short description...",
  "new_parameters": ["param1", "param2"],
  "replacements": [
    {"value": "hardcoded_value", "parameter": "param_name", "description": "Replaced 'hardcoded_value' with '{{.param_name}}'"}
  ]
}

IMPORTANT:
- Only return the JSON object. No markdown formatting around it.
- Ensure the "replacements" list is accurate.
- Do not change the logic of the macro, only replace values with template variables.
`, string(macroJSON), instructions)

	// 3. Call LLM
	genOut, err := s.generator.Generate(ctx, prompt, ai.GenOptions{
		Temperature: 0.2, // Low temperature for precision
	})
	if err != nil {
		return fmt.Sprintf("Error generating refinement: %v", err), nil
	}

	// 4. Parse Response
	var result struct {
		Summary       string   `json:"summary"`
		NewParameters []string `json:"new_parameters"`
		Replacements  []struct {
			Value       string `json:"value"`
			Parameter   string `json:"parameter"`
			Description string `json:"description"`
		} `json:"replacements"`
	}

	// Clean up potential markdown code blocks from LLM
	cleanJSON := strings.TrimSpace(genOut.Text)
	cleanJSON = strings.TrimPrefix(cleanJSON, "```json")
	cleanJSON = strings.TrimPrefix(cleanJSON, "```")
	cleanJSON = strings.TrimSuffix(cleanJSON, "```")

	if err := json.Unmarshal([]byte(cleanJSON), &result); err != nil {
		return fmt.Sprintf("Error parsing AI response: %v\nResponse: %s", err, cleanJSON), nil
	}

	// 5. Apply Changes to a Copy
	newMacro := macro
	newMacro.Description = result.Summary
	// Merge parameters
	existingParams := make(map[string]bool)
	for _, p := range newMacro.Parameters {
		existingParams[p] = true
	}
	for _, p := range result.NewParameters {
		if !existingParams[p] {
			newMacro.Parameters = append(newMacro.Parameters, p)
		}
	}

	// Apply replacements
	count := 0
	for _, rep := range result.Replacements {
		templateVar := fmt.Sprintf("{{.%s}}", rep.Parameter)
		for i := range newMacro.Steps {
			step := &newMacro.Steps[i]
			// Helper to replace
			replace := func(s string) string {
				if strings.Contains(s, rep.Value) {
					return strings.ReplaceAll(s, rep.Value, templateVar)
				}
				return s
			}
			step.Prompt = replace(step.Prompt)
			step.Message = replace(step.Message)
			step.Value = replace(step.Value)
			step.Condition = replace(step.Condition)
			step.List = replace(step.List)
			for k, v := range step.Args {
				if strVal, ok := v.(string); ok {
					if strings.Contains(strVal, rep.Value) {
						step.Args[k] = strings.ReplaceAll(strVal, rep.Value, templateVar)
						count++
					}
				}
			}
			for k, v := range step.MacroArgs {
				if strings.Contains(v, rep.Value) {
					step.MacroArgs[k] = strings.ReplaceAll(v, rep.Value, templateVar)
					count++
				}
			}
		}
	}

	// 6. Store Proposal
	var replacementDescs []string
	for _, r := range result.Replacements {
		replacementDescs = append(replacementDescs, fmt.Sprintf("- %s -> {{.%s}}", r.Value, r.Parameter))
	}

	s.session.PendingRefinement = &RefinementProposal{
		MacroName:     name,
		Category:      category,
		OriginalMacro: macro,
		NewMacro:      newMacro,
		Description:   result.Summary,
		NewParams:     result.NewParameters,
		Replacements:  replacementDescs,
	}

	// 7. Generate Preview Output
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Refinement Proposal for '%s':\n\n", name))
	sb.WriteString(fmt.Sprintf("Summary:\n  Old: %s\n  New: %s\n\n", macro.Description, result.Summary))
	sb.WriteString("New Parameters:\n")
	for _, p := range result.NewParameters {
		sb.WriteString(fmt.Sprintf("  - %s\n", p))
	}
	sb.WriteString("\nReplacements:\n")
	for _, r := range replacementDescs {
		sb.WriteString(fmt.Sprintf("  %s\n", r))
	}
	sb.WriteString("\n\nRun '/macro refine apply' to save these changes, or '/macro refine cancel' to discard.")

	return sb.String(), nil
}

func (s *Service) macroRefineApply(ctx context.Context, macroDB *database.Database) (string, error) {
	proposal := s.session.PendingRefinement
	if proposal == nil {
		return "Error: No pending refinement. Run '/macro refine <name>' first.", nil
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}

	// 1. Save Macro
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening macros store: %v", err), nil
	}
	if err := store.Save(ctx, proposal.Category, proposal.MacroName, proposal.NewMacro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}

	tx.Commit(ctx)
	s.session.PendingRefinement = nil

	return fmt.Sprintf("Macro '%s' updated successfully with new parameters and documentation.", proposal.MacroName), nil
}
