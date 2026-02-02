package agent

import (
	"fmt"

	"github.com/sharedcode/sop/ai"
)

// RefineScriptSteps applies automatic refinements to script steps to improve readability and explicitness.
// This ensures that scripts stored in the system follow conventions (like explicit variable names)
// even if the creator (LLM or user) relied on implicit behaviors.
func RefineScriptSteps(steps []ai.ScriptStep) []ai.ScriptStep {
	refined := make([]ai.ScriptStep, len(steps))
	copy(refined, steps)

	lastResultVar := ""
	knownVars := make(map[string]bool)

	for i := range refined {
		step := &refined[i]

		// Only refine "command" steps (and potentially call_script if strictly used as data pipe)
		if step.Type == "command" {
			if step.Args == nil {
				step.Args = make(map[string]any)
			}

			// 1. Auto-inject result_var based on conventions
			if _, hasRes := step.Args["result_var"]; !hasRes {
				if step.Command == "begin_tx" {
					step.Args["result_var"] = "tx"
					// standard convention, usually doesn't flow into 'filter'
				} else if step.Command == "open_store" {
					if name, ok := step.Args["name"].(string); ok && name != "" {
						step.Args["result_var"] = name
					}
				} else if step.Command == "open_db" {
					if name, ok := step.Args["name"].(string); ok && name != "" {
						step.Args["result_var"] = name
					}
				}
			}

			// 2. Auto-inject explicit dependencies (e.g. transaction name)
			if step.Command == "open_store" {
				if _, hasTx := step.Args["transaction"]; !hasTx {
					step.Args["transaction"] = "tx"
				}
			}

			// 3. Chain Wiring: Connect Input to Previous Output
			consumesInput := false
			switch step.Command {
			case "filter", "sort", "project", "limit", "join", "join_right", "update", "delete", "list_append":
				consumesInput = true
			}

			if consumesInput {
				if _, hasInput := step.Args["input_var"]; !hasInput && lastResultVar != "" {
					step.Args["input_var"] = lastResultVar
				}
			}

			// 4. Output Generation: Create Readable Variable Names
			producesOutput := false
			switch step.Command {
			case "scan", "filter", "sort", "project", "limit", "join", "join_right", "list_new", "call_script", "script":
				producesOutput = true
			}

			if producesOutput {
				if _, hasRes := step.Args["result_var"]; !hasRes {
					// Generate Readable Name
					var newName string

					if step.Command == "scan" {
						// Try to use store name
						if storeArgs, ok := step.Args["store"]; ok {
							if storeName, ok := storeArgs.(string); ok && storeName != "" {
								newName = fmt.Sprintf("%s_list", storeName)
							}
						}
						if newName == "" {
							newName = fmt.Sprintf("scan_result_%d", i)
						}
					} else {
						// Generic readable name: "filtered_result_2"
						suffix := "result"
						if step.Command == "join" || step.Command == "join_right" {
							suffix = "joined"
						} else if step.Command == "project" {
							suffix = "view"
						}
						newName = fmt.Sprintf("%s_%s_%d", step.Command, suffix, i)
					}
					step.Args["result_var"] = newName
				}
				// Update tracker
				lastResultVar = step.Args["result_var"].(string)

			} else if res, ok := step.Args["result_var"].(string); ok && res != "" {
				// Step has explicit result, track it
				lastResultVar = res
			} else if step.Command == "open_store" {
				// open_store produces a store object, usually implicitly result_var=name
				if name, ok := step.Args["name"].(string); ok {
					lastResultVar = name // Store name is the variable name usually
				}
			}

			// Track all defined variables
			if lastResultVar != "" {
				knownVars[lastResultVar] = true
			}
		}
	}

	// 5. Auto-inject return for the last step if missing (Legacy support)
	if len(refined) > 0 {
		lastStep := &refined[len(refined)-1]
		hasExplicitReturn := checkHasExplicitReturn(lastStep)

		// Fix: If the last step IS a return, ensure it points to a valid variable
		if lastStep.Type == "command" && lastStep.Command == "return" && lastResultVar != "" {
			value, _ := lastStep.Args["value"].(string)
			// If variable is unknown (phantom) OR it's a known placeholder like "final_result"
			// Check if 'value' is actually a known variable.
			isKnown := knownVars[value]

			// We treat "final_result", "result", "final_output" as placeholders that should be overwritten
			// UNLESS they were actually defined in the script.
			isPlaceholder := (value == "final_result" || value == "result" || value == "final_output")

			if (!isKnown || isPlaceholder) && value != lastResultVar {
				// We overwrite it to point to the actual last variable we generated/tracked
				lastStep.Args["value"] = lastResultVar
			}
		}

		if !hasExplicitReturn {
			injectImplicitReturn(lastStep)
		}
	}

	// 6. Atomic Script Collapsing
	// If the script contains low-level atomic operations (like open_db, scan),
	// we collapse the entire sequence into a single 'execute_script' step.
	// This ensures they run in the Atomic Engine, which supports these low-level ops.
	if shouldCollapseToAtomic(refined) {
		refined = collapseToExecuteScript(refined)
	}

	return refined
}

func shouldCollapseToAtomic(steps []ai.ScriptStep) bool {
	for _, step := range steps {
		// If we see explicit atomic resource management or low-level scan, it's atomic.
		switch step.Command {
		case "open_db", "begin_tx", "open_store", "scan":
			return true
		}
	}
	return false
}

func collapseToExecuteScript(steps []ai.ScriptStep) []ai.ScriptStep {
	// Convert High-Level Steps -> Atomic Instructions
	var instructions []ScriptInstruction

	for _, step := range steps {
		instr := ScriptInstruction{
			Op:   step.Command,
			Args: make(map[string]any),
		}

		// Flatten Args (Handle Input/Result Vars)
		for k, v := range step.Args {
			if k == "input_var" {
				if str, ok := v.(string); ok {
					instr.InputVar = str
				}
			} else if k == "result_var" {
				if str, ok := v.(string); ok {
					instr.ResultVar = str
				}
			} else {
				instr.Args[k] = v
			}
		}

		// Map 'return' command arguments if needed
		if step.Command == "return" {
			// High level return might be: [return] {"value": "x"}
			// Atomic return expects args["value"]
			if val, ok := step.Args["value"]; ok {
				instr.Args["value"] = val
			}
		}

		instructions = append(instructions, instr)
	}

	// Apply Atomic Sanitization (This handles commit_tx -> defer conversion!)
	instructions = sanitizeScript(instructions)

	// Create Wrapper Step
	wrapper := ai.ScriptStep{
		Type:    "command",
		Command: "execute_script",
		Args: map[string]any{
			"script": instructions,
		},
	}

	return []ai.ScriptStep{wrapper}
}

func checkHasExplicitReturn(lastStep *ai.ScriptStep) bool {
	switch lastStep.Type {
	case "command":
		if lastStep.Args != nil {
			if _, ok := lastStep.Args["result_var"]; ok {
				return true
			}
			if lastStep.Command == "return" {
				return true
			}
		}
	case "call_script":
		if lastStep.Args != nil { // Note: call_script step usually defines output in specific ways
			if _, ok := lastStep.Args["result_var"]; ok {
				return true
			}
		}
	case "fetch", "ask":
		if lastStep.OutputVariable != "" {
			return true
		}
	}
	return false
}

func injectImplicitReturn(lastStep *ai.ScriptStep) {
	// Apply "result" convention
	switch lastStep.Type {
	case "command":
		if lastStep.Args == nil {
			lastStep.Args = make(map[string]any)
		}
		if _, ok := lastStep.Args["result_var"]; !ok {
			lastStep.Args["result_var"] = "result"
		}
	case "call_script":
		if lastStep.Args == nil {
			lastStep.Args = make(map[string]any)
		}
		if _, ok := lastStep.Args["result_var"]; !ok {
			lastStep.Args["result_var"] = "result"
		}
	case "fetch", "ask":
		if lastStep.OutputVariable == "" {
			lastStep.OutputVariable = "result"
		}
	}
}
