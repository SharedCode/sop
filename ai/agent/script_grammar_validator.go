package agent

import (
	"fmt"
	"strings"
)

// grammarValidationError represents a structural/logical error in the script.
type grammarValidationError struct {
	StepIndex int
	Category  string
	Message   string
	Hint      string
}

func (e *grammarValidationError) Error() string {
	return fmt.Sprintf("Step %d [%s]: %s", e.StepIndex, e.Category, e.Message)
}

// grammarValidationErrors aggregates multiple grammar validation errors.
type grammarValidationErrors struct {
	Errors []*grammarValidationError
}

func (e *grammarValidationErrors) Error() string {
	if len(e.Errors) == 0 {
		return "script grammar validation failed"
	}
	var sb strings.Builder
	sb.WriteString("Script grammar validation failed:\n")
	for i, err := range e.Errors {
		sb.WriteString(fmt.Sprintf("  %d. %s", i+1, err.Message))
		if err.Hint != "" {
			sb.WriteString(fmt.Sprintf("\n     Hint: %s", err.Hint))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// validateScriptGrammar performs structural and logical validation of the script.
// This runs AFTER normalization/sanitization but BEFORE execution.
// It catches issues like:
// - Consecutive duplicate operations on the same store
// - Variable use before definition
// - Invalid operation dependencies
// - Unreachable code after return
func validateScriptGrammar(script []ScriptInstruction) error {
	var errors []*grammarValidationError

	// Track defined variables
	definedVars := make(map[string]int)    // var -> step index where defined
	requiredVars := make(map[string][]int) // var -> step indices where used

	// Track store operations
	type storeOp struct {
		stepIndex int
		op        string
		store     string
	}
	recentStoreOps := make(map[string]*storeOp) // result_var -> last operation

	// Track if we've seen a return (code after return is unreachable)
	returnIndex := -1

	for i, instr := range script {
		op := strings.ToLower(strings.TrimSpace(instr.Op))

		// Check for unreachable code after return
		if returnIndex >= 0 {
			errors = append(errors, &grammarValidationError{
				StepIndex: i,
				Category:  "unreachable_code",
				Message:   fmt.Sprintf("Step %d (%s) is unreachable: return was called at step %d", i, instr.Op, returnIndex),
				Hint:      "Remove steps after return or restructure the script",
			})
			continue
		}

		// Track return operations
		if op == "return" {
			returnIndex = i
		}

		// Check input_var is defined
		if instr.InputVar != "" {
			inputVar := strings.TrimSpace(instr.InputVar)
			if _, defined := definedVars[inputVar]; !defined {
				requiredVars[inputVar] = append(requiredVars[inputVar], i)
			}
		}

		// Get store reference for this operation
		storeRef := ""
		if instr.Args != nil {
			// Try various keys that reference stores
			for _, key := range []string{"store", "target", "with", "name"} {
				if val, ok := instr.Args[key].(string); ok && strings.TrimSpace(val) != "" {
					storeRef = strings.TrimSpace(val)
					break
				}
			}
		}

		// Detect consecutive duplicate joins to the same store
		if (op == "join" || op == "join_right") && instr.InputVar != "" && storeRef != "" {
			inputVar := strings.TrimSpace(instr.InputVar)

			// Check if the input_var came from a recent join to the same store
			if prevOp, exists := recentStoreOps[inputVar]; exists {
				if (prevOp.op == "join" || prevOp.op == "join_right") && prevOp.store == storeRef {
					errors = append(errors, &grammarValidationError{
						StepIndex: i,
						Category:  "duplicate_consecutive_join",
						Message: fmt.Sprintf(
							"Step %d: Joining to store %q immediately after step %d already joined to %q (via %s). This creates unnecessary nested joins.",
							i, storeRef, prevOp.stepIndex, prevOp.store, inputVar,
						),
						Hint: fmt.Sprintf(
							"Combine the join mappings into a single join operation, or verify the store reference is correct. Bridge tables typically need only one join with proper field mappings.",
						),
					})
				}
			}

			// Track this join operation
			if instr.ResultVar != "" {
				recentStoreOps[strings.TrimSpace(instr.ResultVar)] = &storeOp{
					stepIndex: i,
					op:        op,
					store:     storeRef,
				}
			}
		}

		// Track variable definitions (result_var)
		if instr.ResultVar != "" {
			resultVar := strings.TrimSpace(instr.ResultVar)
			definedVars[resultVar] = i

			// Track for duplicate join detection
			if op != "join" && op != "join_right" {
				// Non-join operations reset the store tracking
				recentStoreOps[resultVar] = &storeOp{
					stepIndex: i,
					op:        op,
					store:     storeRef,
				}
			}
		}

		// Validate operation-specific requirements
		switch op {
		case "join", "join_right":
			if instr.InputVar == "" {
				errors = append(errors, &grammarValidationError{
					StepIndex: i,
					Category:  "missing_input_var",
					Message:   fmt.Sprintf("Step %d: %s operation requires input_var (the pipeline result to join from)", i, instr.Op),
					Hint:      "Set input_var to the result_var of a previous scan, select, or join operation",
				})
			}
			if storeRef == "" {
				errors = append(errors, &grammarValidationError{
					StepIndex: i,
					Category:  "missing_store_ref",
					Message:   fmt.Sprintf("Step %d: %s operation requires a store reference (store, target, or with)", i, instr.Op),
					Hint:      "Specify the target store to join with",
				})
			}

		case "filter", "sort", "project", "limit":
			if instr.InputVar == "" {
				errors = append(errors, &grammarValidationError{
					StepIndex: i,
					Category:  "missing_input_var",
					Message:   fmt.Sprintf("Step %d: %s operation requires input_var (the pipeline result to %s)", i, instr.Op, op),
					Hint:      "Set input_var to the result_var of a previous scan, select, join, or filter operation",
				})
			}

		case "commit_tx", "rollback_tx":
			if instr.Args == nil || instr.Args["transaction"] == nil {
				errors = append(errors, &grammarValidationError{
					StepIndex: i,
					Category:  "missing_transaction",
					Message:   fmt.Sprintf("Step %d: %s operation requires transaction argument", i, instr.Op),
					Hint:      "Specify the transaction variable from begin_tx",
				})
			}
		}
	}

	// Check for variables used before definition
	for varName, usedAtSteps := range requiredVars {
		if defStep, defined := definedVars[varName]; defined {
			// Check if any usage is before definition
			for _, useStep := range usedAtSteps {
				if useStep < defStep {
					errors = append(errors, &grammarValidationError{
						StepIndex: useStep,
						Category:  "undefined_variable",
						Message:   fmt.Sprintf("Step %d uses variable %q before it is defined (defined at step %d)", useStep, varName, defStep),
						Hint:      "Reorder steps so the variable is defined before use",
					})
				}
			}
		} else {
			// Variable is never defined
			for _, useStep := range usedAtSteps {
				errors = append(errors, &grammarValidationError{
					StepIndex: useStep,
					Category:  "undefined_variable",
					Message:   fmt.Sprintf("Step %d uses undefined variable %q", useStep, varName),
					Hint:      "Ensure the variable is defined with result_var in a previous step",
				})
			}
		}
	}

	if len(errors) > 0 {
		return &grammarValidationErrors{Errors: errors}
	}

	return nil
}
