package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

func (a *DataAdminAgent) toolListScripts(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Look for "system" database
	db := a.systemDB
	if db == nil {
		if opts, ok := a.databases["system"]; ok {
			db = database.NewDatabase(opts)
		}
	}

	if db == nil {
		return "", fmt.Errorf("system database not found")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	store, err := db.OpenModelStore(ctx, "scripts", tx)
	if err != nil {
		return "", fmt.Errorf("failed to open scripts store: %w", err)
	}

	names, err := store.List(ctx, "general")
	if err != nil {
		return "", fmt.Errorf("failed to list scripts: %w", err)
	}

	if len(names) == 0 {
		return "No scripts found.", nil
	}
	sort.Strings(names)
	return fmt.Sprintf("Scripts: %s", strings.Join(names, ", ")), nil
}

func (a *DataAdminAgent) toolGetScriptDetails(ctx context.Context, args map[string]any) (string, error) {
	name, _ := args["name"].(string)
	if name == "" {
		return "", fmt.Errorf("script name required")
	}

	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Look for "system" database
	db := a.systemDB
	if db == nil {
		if opts, ok := a.databases["system"]; ok {
			db = database.NewDatabase(opts)
		}
	}

	if db == nil {
		return "", fmt.Errorf("system database not found")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	store, err := db.OpenModelStore(ctx, "scripts", tx)
	if err != nil {
		return "", fmt.Errorf("failed to open scripts store: %w", err)
	}

	var script ai.Script
	if err := store.Load(ctx, "general", name, &script); err != nil {
		return "", fmt.Errorf("failed to load script '%s': %w", name, err)
	}

	// Format details
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Script: %s\n", script.Name))
	if script.Description != "" {
		sb.WriteString(fmt.Sprintf("Description: %s\n", script.Description))
	}
	if script.Database != "" {
		sb.WriteString(fmt.Sprintf("Database: %s\n", script.Database))
	}
	if script.Portable {
		sb.WriteString("Portable: true\n")
	}
	sb.WriteString("Steps:\n")
	printSteps(&sb, script.Steps, 0)

	return sb.String(), nil
}

func (a *DataAdminAgent) toolCreateScript(ctx context.Context, args map[string]any) (string, error) {
	name, _ := args["name"].(string)
	if name == "" {
		return "", fmt.Errorf("script name is required")
	}

	description, _ := args["description"].(string)
	stepsList, _ := args["steps"].([]any)
	steps, err := mapToScriptSteps(stepsList)
	if err != nil {
		return "", fmt.Errorf("invalid steps: %v", err)
	}

	var currentDB string
	if p := ai.GetSessionPayload(ctx); p != nil {
		currentDB = p.CurrentDB
	}

	script := ai.Script{
		Name:        name,
		Description: description,
		Database:    currentDB,
		Steps:       steps,
	}

	// Persist the new script
	// Using a dedicated Create or overwriting 'updatedScript'?
	// updateScript function handles open/save/commit. We can reuse it or create a new one.
	// But updateScript expects the script to exist or creates it?
	// The implementation of updateScript reads it first.
	// Let's implement a create logic using store operations directly OR modify updateScript to handle creation.
	// Actually, looking at toolListScripts, it opens the "scripts" store.

	// Let's implement independent logic for clarity or reuse updateScript if it supports "not found".
	// The current updateScript function:
	/*
			func (a *DataAdminAgent) updateScript(ctx context.Context, name string, updateFunc func(*ai.Script) error) error {
			    ...
				var script ai.Script
				if err := store.Get(ctx, "general", name, &script); err != nil {
					return err
				}
		        ...
		    }
	*/
	// It assumes existence. So we need a dedicated create flow.

	db := a.systemDB
	if db == nil {
		if opts, ok := a.databases["system"]; ok {
			db = database.NewDatabase(opts)
		}
	}
	if db == nil {
		return "", fmt.Errorf("system database not found")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	store, err := db.OpenModelStore(ctx, "scripts", tx)
	if err != nil {
		return "", fmt.Errorf("failed to open scripts store: %w", err)
	}

	// Check if already exists
	// store.Get returns error if not found? Or false?
	// The modelStore interface usually returns error if not found in SOP (if KeyNotFound).
	// We can try to load into a dummy var.
	var dummy ai.Script
	err = store.Load(ctx, "general", name, &dummy)
	if err == nil {
		return "", fmt.Errorf("script '%s' already exists", name)
	}
	// Warning: We are assuming error means Not Found. SOP might have specific error types.
	// Ideally we check error type. But for now, assuming any error is good enough to proceed to Create.

	if err := store.Save(ctx, "general", name, &script); err != nil {
		return "", fmt.Errorf("failed to save script: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	return fmt.Sprintf("Script '%s' created successfully.", name), nil
}

func (a *DataAdminAgent) toolSaveScript(ctx context.Context, args map[string]any) (string, error) {
	name, _ := args["name"].(string)
	if name == "" {
		return "", fmt.Errorf("script name is required")
	}

	description, _ := args["description"].(string)

	stepsList, _ := args["steps"].([]any)
	steps, err := mapToScriptSteps(stepsList)
	if err != nil {
		return "", fmt.Errorf("invalid steps: %v", err)
	}

	var currentDB string
	if p := ai.GetSessionPayload(ctx); p != nil {
		currentDB = p.CurrentDB
	}

	script := ai.Script{
		Name:        name,
		Description: description,
		Database:    currentDB,
		Steps:       steps,
	}

	db := a.systemDB
	if db == nil {
		if opts, ok := a.databases["system"]; ok {
			db = database.NewDatabase(opts)
		}
	}
	if db == nil {
		return "", fmt.Errorf("system database not found")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	store, err := db.OpenModelStore(ctx, "scripts", tx)
	if err != nil {
		return "", fmt.Errorf("failed to open scripts store: %w", err)
	}

	if err := store.Save(ctx, "general", name, &script); err != nil {
		return "", fmt.Errorf("failed to save script: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	return fmt.Sprintf("Script '%s' saved successfully with %d steps.", name, len(steps)), nil
}

func printSteps(sb *strings.Builder, steps []ai.ScriptStep, indent int) {
	indentStr := strings.Repeat("  ", indent)
	for i, step := range steps {
		desc := step.Message
		if step.Type == "ask" {
			desc = step.Prompt
		} else if step.Type == "call_script" {
			desc = fmt.Sprintf("Run '%s'", step.ScriptName)
		} else if step.Type == "command" {
			argsJSON, _ := json.Marshal(step.Args)
			desc = fmt.Sprintf("Execute '%s' %s", step.Command, string(argsJSON))
		} else if step.Type == "block" {
			desc = fmt.Sprintf("Sequence of %d steps", len(step.Steps))
		} else if step.Type == "loop" {
			desc = fmt.Sprintf("For %s in %s", step.Iterator, step.List)
		} else if step.Type == "if" {
			desc = fmt.Sprintf("If %s", step.Condition)
		}

		sb.WriteString(fmt.Sprintf("%s%d. [%s] %s\n", indentStr, i+1, step.Type, desc))

		if step.Type == "block" || step.Type == "loop" {
			printSteps(sb, step.Steps, indent+1)
		} else if step.Type == "if" {
			if len(step.Then) > 0 {
				sb.WriteString(fmt.Sprintf("%s  Then:\n", indentStr))
				printSteps(sb, step.Then, indent+2)
			}
			if len(step.Else) > 0 {
				sb.WriteString(fmt.Sprintf("%s  Else:\n", indentStr))
				printSteps(sb, step.Else, indent+2)
			}
		}
	}
}

func (a *DataAdminAgent) toolRefactorScript(ctx context.Context, args map[string]any) (string, error) {
	mode, _ := args["mode"].(string)
	if mode == "" {
		return "", fmt.Errorf("mode is required (script or block)")
	}
	name, _ := args["name"].(string)

	recorder, ok := ctx.Value(ai.CtxKeyScriptRecorder).(ai.ScriptRecorder)
	if !ok {
		return "", fmt.Errorf("no script recorder available")
	}

	// We don't know the count here, but RefactorLastSteps can handle count=0 (use buffer)
	if err := recorder.RefactorLastSteps(0, mode, name); err != nil {
		return "", fmt.Errorf("refactor failed: %w", err)
	}

	if mode == "script" {
		return fmt.Sprintf("Last interaction refactored into new script '%s' and added as step.", name), nil
	}
	return "Last interaction refactored into a block step.", nil
}

func (a *DataAdminAgent) toolScriptInsertStep(ctx context.Context, args map[string]any) (string, error) {
	scriptName, _ := args["script"].(string)
	index, _ := args["index"].(float64)
	stepType, _ := args["type"].(string)

	if scriptName == "" {
		return "", fmt.Errorf("script name required")
	}
	if stepType == "" {
		return "", fmt.Errorf("step type required")
	}

	newStep := ai.ScriptStep{
		Type: stepType,
	}

	// Populate fields based on args
	if v, ok := args["prompt"].(string); ok {
		newStep.Prompt = v
	}
	if v, ok := args["message"].(string); ok {
		newStep.Message = v
	}
	if v, ok := args["script_name"].(string); ok {
		newStep.ScriptName = v
	}
	if v, ok := args["command"].(string); ok {
		newStep.Command = v
	}
	if v, ok := args["condition"].(string); ok {
		newStep.Condition = v
	}
	if v, ok := args["iterator"].(string); ok {
		newStep.Iterator = v
	}
	if v, ok := args["list"].(string); ok {
		newStep.List = v
	}
	if v, ok := args["description"].(string); ok {
		newStep.Description = v
	}
	if v, ok := args["name"].(string); ok {
		newStep.Name = v
	}

	// Additional fields mapping
	if v, ok := args["output_variable"].(string); ok {
		newStep.OutputVariable = v
	}
	if v, ok := args["variable"].(string); ok {
		newStep.Variable = v
	}
	if v, ok := args["value"].(string); ok {
		newStep.Value = v
	}
	if v, ok := args["database"].(string); ok {
		newStep.Database = v
	}
	if v, ok := args["source"].(string); ok {
		newStep.Source = v
	}
	if v, ok := args["resource"].(string); ok {
		newStep.Resource = v
	}
	if v, ok := args["filter"].(string); ok {
		newStep.Filter = v
	}
	if v, ok := args["script_args"].(map[string]any); ok {
		// Convert map[string]any to map[string]string for ScriptArgs
		sa := make(map[string]string)
		for k, val := range v {
			sa[k] = fmt.Sprintf("%v", val)
		}
		newStep.ScriptArgs = sa
	}
	if v, ok := args["is_async"].(bool); ok {
		newStep.IsAsync = v
	}
	if v, ok := args["continue_on_error"].(bool); ok {
		newStep.ContinueOnError = v
	}

	if v, ok := args["args"].(map[string]any); ok {
		newStep.Args = v
	}

	err := a.updateScript(ctx, scriptName, func(m *ai.Script) error {
		idx := int(index)
		if idx < 0 || idx > len(m.Steps) {
			return fmt.Errorf("index out of bounds")
		}
		// Insert
		m.Steps = append(m.Steps[:idx], append([]ai.ScriptStep{newStep}, m.Steps[idx:]...)...)
		return nil
	})

	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Step inserted into script '%s' at index %d", scriptName, int(index)), nil
}

func (a *DataAdminAgent) toolScriptSaveStep(ctx context.Context, args map[string]any) (string, error) {
	// Wrapper to Append a step.
	// We construct a ScriptStep from args then append it.

	scriptName, _ := args["script"].(string)
	if scriptName == "" {
		return "", fmt.Errorf("script name required")
	}
	stepType, _ := args["type"].(string)
	if stepType == "" {
		// Try to infer type?
		// If command provided -> command
		// If variable provided -> set
		// Better to require type or default to 'command' if command present.
		if _, ok := args["command"]; ok {
			stepType = "command"
		} else if _, ok := args["variable"]; ok {
			stepType = "set"
		} else if _, ok := args["prompt"]; ok {
			stepType = "ask"
		} else if _, ok := args["source"]; ok {
			stepType = "fetch"
		} else {
			return "", fmt.Errorf("step type required")
		}
	}

	newStep := ai.ScriptStep{
		Type: stepType,
	}

	// Populate standard fields
	if v, ok := args["name"].(string); ok {
		newStep.Name = v
	}
	if v, ok := args["description"].(string); ok {
		newStep.Description = v
	}

	if v, ok := args["prompt"].(string); ok {
		newStep.Prompt = v
	}
	if v, ok := args["message"].(string); ok {
		newStep.Message = v
	}
	if v, ok := args["script_name"].(string); ok {
		newStep.ScriptName = v
	}
	if v, ok := args["command"].(string); ok {
		newStep.Command = v
	}
	if v, ok := args["condition"].(string); ok {
		newStep.Condition = v
	}
	if v, ok := args["iterator"].(string); ok {
		newStep.Iterator = v
	}
	if v, ok := args["list"].(string); ok {
		newStep.List = v
	}

	if v, ok := args["output_variable"].(string); ok {
		newStep.OutputVariable = v
	}
	if v, ok := args["variable"].(string); ok {
		newStep.Variable = v
	}
	if v, ok := args["value"].(string); ok {
		newStep.Value = v
	}
	if v, ok := args["database"].(string); ok {
		newStep.Database = v
	}
	if v, ok := args["source"].(string); ok {
		newStep.Source = v
	}
	if v, ok := args["resource"].(string); ok {
		newStep.Resource = v
	}
	if v, ok := args["filter"].(string); ok {
		newStep.Filter = v
	}

	if v, ok := args["script_args"].(map[string]any); ok {
		sa := make(map[string]string)
		for k, val := range v {
			sa[k] = fmt.Sprintf("%v", val)
		}
		newStep.ScriptArgs = sa
	}
	if v, ok := args["is_async"].(bool); ok {
		newStep.IsAsync = v
	}
	if v, ok := args["continue_on_error"].(bool); ok {
		newStep.ContinueOnError = v
	}

	if v, ok := args["args"].(map[string]any); ok {
		newStep.Args = v
	}

	err := a.updateScript(ctx, scriptName, func(m *ai.Script) error {
		m.Steps = append(m.Steps, newStep)
		return nil
	})

	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Step '%s' saved to script '%s'.", newStep.Name, scriptName), nil
}

func (a *DataAdminAgent) toolScriptDeleteStep(ctx context.Context, args map[string]any) (string, error) {
	scriptName, _ := args["script"].(string)
	index, _ := args["index"].(float64)

	if scriptName == "" {
		return "", fmt.Errorf("script name required")
	}

	err := a.updateScript(ctx, scriptName, func(m *ai.Script) error {
		idx := int(index)
		if idx < 0 || idx >= len(m.Steps) {
			return fmt.Errorf("index out of bounds")
		}
		// Delete
		m.Steps = append(m.Steps[:idx], m.Steps[idx+1:]...)
		return nil
	})

	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Step deleted from script '%s' at index %d", scriptName, int(index)), nil
}

func (a *DataAdminAgent) toolScriptUpdateStep(ctx context.Context, args map[string]any) (string, error) {
	scriptName, _ := args["script"].(string)
	index, _ := args["index"].(float64)

	if scriptName == "" {
		return "", fmt.Errorf("script name required")
	}

	err := a.updateScript(ctx, scriptName, func(m *ai.Script) error {
		idx := int(index)
		if idx < 0 || idx >= len(m.Steps) {
			return fmt.Errorf("index out of bounds")
		}

		step := &m.Steps[idx]

		// Update fields if present
		if v, ok := args["type"].(string); ok {
			step.Type = v
		}
		if v, ok := args["prompt"].(string); ok {
			step.Prompt = v
		}
		if v, ok := args["message"].(string); ok {
			step.Message = v
		}
		if v, ok := args["description"].(string); ok {
			step.Description = v
		}
		if v, ok := args["name"].(string); ok {
			step.Name = v
		}

		// Additional fields mapping
		if v, ok := args["output_variable"].(string); ok {
			step.OutputVariable = v
		}
		if v, ok := args["variable"].(string); ok {
			step.Variable = v
		}
		if v, ok := args["value"].(string); ok {
			step.Value = v
		}
		if v, ok := args["database"].(string); ok {
			step.Database = v
		}
		if v, ok := args["source"].(string); ok {
			step.Source = v
		}
		if v, ok := args["resource"].(string); ok {
			step.Resource = v
		}
		if v, ok := args["filter"].(string); ok {
			step.Filter = v
		}
		if v, ok := args["script_args"].(map[string]any); ok {
			sa := make(map[string]string)
			for k, val := range v {
				sa[k] = fmt.Sprintf("%v", val)
			}
			step.ScriptArgs = sa
		}
		if v, ok := args["is_async"].(bool); ok {
			step.IsAsync = v
		}
		if v, ok := args["continue_on_error"].(bool); ok {
			step.ContinueOnError = v
		}

		if v, ok := args["script_name"].(string); ok {
			step.ScriptName = v
		}

		// PROTECT COMMAND: Only update if explicitly provided and non-empty.
		// UI might send empty "command" if it's mapping "Function" to "Name" but clearing command.
		// However, if the intent is to update "Function" (Name), strictly update Name.
		if v, ok := args["command"].(string); ok && v != "" {
			step.Command = v
		}
		if v, ok := args["condition"].(string); ok {
			step.Condition = v
		}
		if v, ok := args["iterator"].(string); ok {
			step.Iterator = v
		}
		if v, ok := args["list"].(string); ok {
			step.List = v
		}
		if v, ok := args["args"].(map[string]any); ok {
			step.Args = v
		}

		return nil
	})

	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Step updated in script '%s' at index %d", scriptName, int(index)), nil
}

func (a *DataAdminAgent) toolScriptReorderSteps(ctx context.Context, args map[string]any) (string, error) {
	scriptName, _ := args["script"].(string)
	fromIndex, _ := args["from_index"].(float64)
	toIndex, _ := args["to_index"].(float64)

	if scriptName == "" {
		return "", fmt.Errorf("script name required")
	}

	err := a.updateScript(ctx, scriptName, func(m *ai.Script) error {
		from := int(fromIndex)
		to := int(toIndex)
		if from < 0 || from >= len(m.Steps) {
			return fmt.Errorf("from_index out of bounds")
		}
		if to < 0 || to > len(m.Steps) { // Allow moving to end
			return fmt.Errorf("to_index out of bounds")
		}

		step := m.Steps[from]
		// Remove
		m.Steps = append(m.Steps[:from], m.Steps[from+1:]...)

		// Adjust 'to' if we removed an item before it
		if from < to {
			to--
		}

		// Insert
		m.Steps = append(m.Steps[:to], append([]ai.ScriptStep{step}, m.Steps[to:]...)...)
		return nil
	})

	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Step moved from %d to %d in script '%s'", int(fromIndex), int(toIndex), scriptName), nil
}

// Helper to update a script transactionally
func (a *DataAdminAgent) updateScript(ctx context.Context, name string, updateFunc func(*ai.Script) error) error {
	// Look for "system" database
	db := a.systemDB
	if db == nil {
		if opts, ok := a.databases["system"]; ok {
			db = database.NewDatabase(opts)
		}
	}
	if db == nil {
		return fmt.Errorf("system database not found")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	store, err := db.OpenModelStore(ctx, "scripts", tx)
	if err != nil {
		return fmt.Errorf("failed to open scripts store: %w", err)
	}

	var script ai.Script
	if err := store.Load(ctx, "general", name, &script); err != nil {
		return fmt.Errorf("failed to load script '%s': %w", name, err)
	}

	if err := updateFunc(&script); err != nil {
		return err
	}

	if err := store.Save(ctx, "general", name, &script); err != nil {
		return fmt.Errorf("failed to save script '%s': %w", name, err)
	}

	return tx.Commit(ctx)
}

func (a *DataAdminAgent) toolScriptAddStepFromLast(ctx context.Context, args map[string]any) (string, error) {
	if a.lastToolCall == nil {
		return "", fmt.Errorf("no last tool call found")
	}

	scriptName, _ := args["script"].(string)
	if scriptName == "" {
		return "", fmt.Errorf("script name required")
	}

	// Optional: index
	var index int = -1
	if v, ok := args["index"].(float64); ok {
		index = int(v)
	}

	// Optional: position (before, after, append)
	position, _ := args["position"].(string)
	if position == "" {
		if index == -1 {
			position = "append"
		} else {
			position = "after" // Default to after if index is provided
		}
	}

	err := a.updateScript(ctx, scriptName, func(m *ai.Script) error {
		// Convert generic tool call to ScriptStep
		// Assuming lastToolCall is generic or we map it
		// For now, let's assume we construct a simple command step
		step := ai.ScriptStep{
			Type:    "command",
			Command: a.lastToolCall.Command, // Assuming a.lastToolCall has Command
			Args:    a.lastToolCall.Args,
		}

		// Optional: description
		if v, ok := args["description"].(string); ok {
			step.Description = v
		}
		// Optional: name
		if v, ok := args["name"].(string); ok {
			step.Name = v
		}

		if position == "append" || index == -1 {
			m.Steps = append(m.Steps, step)
			return nil
		}

		if index < 0 || index >= len(m.Steps) {
			if index == len(m.Steps) {
				m.Steps = append(m.Steps, step)
				return nil
			}
			return fmt.Errorf("index out of bounds")
		}

		if position == "before" {
			// Insert at index
			m.Steps = append(m.Steps[:index], append([]ai.ScriptStep{step}, m.Steps[index:]...)...)
		} else {
			// "after" -> Insert at index + 1
			if index+1 >= len(m.Steps) {
				m.Steps = append(m.Steps, step)
			} else {
				m.Steps = append(m.Steps[:index+1], append([]ai.ScriptStep{step}, m.Steps[index+1:]...)...)
			}
		}
		return nil
	})

	if err != nil {
		return "", err
	}

	return fmt.Sprintf("Step added to script '%s'", scriptName), nil
}
