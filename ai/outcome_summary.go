package ai

import (
	"fmt"
	"sort"
	"strings"
)

const outcomeFactsLimit = 4

const (
	outcomeRecipeKindImplicit          = "implicit"
	outcomeRecipeScopeMicro            = "micro"
	outcomeRecipeStoresDomain          = "Stores"
	outcomeRepairStrategySameTool      = "same_tool"
	outcomeRepairStrategyResearchFirst = "research_first"
)

func SummarizeSuccessfulToolResult(result ReActToolResult) []string {
	if result.Name == "list_stores" {
		if facts := extractListStoresFacts(result.Result); len(facts) > 0 {
			return facts
		}
	}
	if result.Name == "execute_script" {
		facts := extractExecuteScriptPlanFacts(result.Args)
		if summary := summarizeGenericSuccessfulToolResult(result); summary != "" {
			facts = append(facts, summary)
		}
		if len(facts) > 0 {
			return facts
		}
	}

	if summary := summarizeGenericSuccessfulToolResult(result); summary != "" {
		return []string{summary}
	}
	return nil
}

func SummarizeOutcomeFacts(toolResults []ReActToolResult) []string {
	if len(toolResults) == 0 {
		return nil
	}

	facts := make([]string, 0, outcomeFactsLimit)
	seen := make(map[string]bool, outcomeFactsLimit)
	for i := len(toolResults) - 1; i >= 0 && len(facts) < outcomeFactsLimit; i-- {
		result := toolResults[i]
		if strings.Contains(result.Result, "Retry instruction:") {
			continue
		}
		for _, fact := range SummarizeSuccessfulToolResult(result) {
			fact = strings.TrimSpace(fact)
			if fact == "" || seen[fact] {
				continue
			}
			seen[fact] = true
			facts = append(facts, fact)
			if len(facts) >= outcomeFactsLimit {
				break
			}
		}
	}
	return facts
}

func SummarizeOutcomeRecipes(toolResults []ReActToolResult) []LearnedRecipe {
	if len(toolResults) == 0 {
		return nil
	}

	recipes := make([]LearnedRecipe, 0, 2)
	seen := make(map[string]bool, 2)
	appendRecipe := func(recipe LearnedRecipe) {
		if strings.TrimSpace(recipe.ID) == "" || seen[recipe.ID] {
			return
		}
		seen[recipe.ID] = true
		recipes = append(recipes, recipe)
	}

	for i, result := range toolResults {
		if !isRecoverableRepairResult(result, "execute_script", outcomeRepairStrategyResearchFirst) {
			continue
		}
		if hasSuccessfulToolSequence(toolResults[i+1:], "list_stores", "execute_script") {
			appendRecipe(LearnedRecipe{
				ID:      "implicit.execute_script.research_then_retry",
				Kind:    outcomeRecipeKindImplicit,
				Scope:   outcomeRecipeScopeMicro,
				Domain:  outcomeRecipeStoresDomain,
				Topic:   "Research grounded schema before execute_script retry",
				Trigger: "execute_script repair requires missing schema or relation facts",
				Protocol: []string{
					"Call list_stores first to confirm the active store schema and relations.",
					"Reuse the confirmed names as the source of truth instead of guessing field or join paths.",
					"Retry execute_script with corrected grounded arguments without restarting the whole plan.",
				},
				Invariants: []string{
					"Preserve valid script slices that already conform to the plan.",
					"Do not broaden scope before the grounded retry is attempted.",
				},
				Confidence: 0.95,
				Source:     "inner_loop_success",
			})
		}
	}

	for i, result := range toolResults {
		if !isRecoverableRepairResult(result, "execute_script", outcomeRepairStrategySameTool) {
			continue
		}
		if hasSuccessfulTool(toolResults[i+1:], "execute_script") {
			appendRecipe(LearnedRecipe{
				ID:      "implicit.execute_script.repair_in_place",
				Kind:    outcomeRecipeKindImplicit,
				Scope:   outcomeRecipeScopeMicro,
				Domain:  outcomeRecipeStoresDomain,
				Topic:   "Repair execute_script in place",
				Trigger: "execute_script has a recoverable argument-shape error",
				Protocol: []string{
					"Retry the same tool instead of abandoning the plan.",
					"Preserve valid arguments and rewrite only the malformed or missing slice.",
					"Keep the repaired call grounded in the already confirmed store and field names.",
				},
				Invariants: []string{
					"Do not replace valid join or filter clauses that already work.",
					"Do not switch to unrelated tools until the repair attempt succeeds or is disproven.",
				},
				Confidence: 0.9,
				Source:     "inner_loop_success",
			})
		}
	}

	return recipes
}

func summarizeGenericSuccessfulToolResult(result ReActToolResult) string {
	trimmed := strings.TrimSpace(result.Result)
	if trimmed == "" {
		return fmt.Sprintf("%s completed successfully.", result.Name)
	}
	trimmed = strings.ReplaceAll(trimmed, "\n", " ")
	trimmed = strings.Join(strings.Fields(trimmed), " ")
	if len(trimmed) > 180 {
		trimmed = trimmed[:177] + "..."
	}
	return fmt.Sprintf("%s returned: %s", result.Name, trimmed)
}

func extractExecuteScriptPlanFacts(args map[string]any) []string {
	if len(args) == 0 {
		return nil
	}
	rawScript, ok := args["script"]
	if !ok {
		return nil
	}
	steps, ok := rawScript.([]any)
	if !ok {
		return nil
	}
	facts := make([]string, 0, len(steps))
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		op := strings.TrimSpace(fmt.Sprintf("%v", step["op"]))
		stepArgs, ok := step["args"].(map[string]any)
		if !ok {
			continue
		}
		facts = append(facts, extractExecuteScriptJoinFacts(op, stepArgs)...)
		facts = append(facts, extractExecuteScriptFilterFacts(op, stepArgs)...)
	}
	return facts
}

func extractExecuteScriptJoinFacts(op string, stepArgs map[string]any) []string {
	if op != "join" && op != "join_right" {
		return nil
	}
	store := strings.TrimSpace(fmt.Sprintf("%v", stepArgs["store"]))
	if store == "" {
		return nil
	}
	onMap, ok := stepArgs["on"].(map[string]any)
	if !ok || len(onMap) == 0 {
		return nil
	}
	leftFields := make([]string, 0, len(onMap))
	for leftField := range onMap {
		leftFields = append(leftFields, leftField)
	}
	sort.Strings(leftFields)
	facts := make([]string, 0, len(leftFields))
	for _, leftField := range leftFields {
		rightField := strings.TrimSpace(fmt.Sprintf("%v", onMap[leftField]))
		if strings.TrimSpace(leftField) == "" || rightField == "" {
			continue
		}
		facts = append(facts, fmt.Sprintf("execute_script confirmed %s store=%s on=%s->%s", op, store, leftField, rightField))
	}
	return facts
}

func extractExecuteScriptFilterFacts(op string, stepArgs map[string]any) []string {
	var rawCondition any
	switch op {
	case "filter":
		rawCondition = stepArgs["condition"]
	case "scan", "select":
		rawCondition = stepArgs["filter"]
	default:
		return nil
	}
	conditionMap, ok := rawCondition.(map[string]any)
	if !ok || len(conditionMap) == 0 {
		return nil
	}
	fields := make([]string, 0, len(conditionMap))
	for field := range conditionMap {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	facts := make([]string, 0, len(fields))
	for _, field := range fields {
		operator := extractPrimaryConditionOperator(conditionMap[field])
		if strings.TrimSpace(field) == "" || operator == "" {
			continue
		}
		facts = append(facts, fmt.Sprintf("execute_script confirmed filter field=%s op=%s", field, operator))
	}
	return facts
}

func extractPrimaryConditionOperator(rawCondition any) string {
	conditionMap, ok := rawCondition.(map[string]any)
	if !ok || len(conditionMap) == 0 {
		return ""
	}
	operators := make([]string, 0, len(conditionMap))
	for operator := range conditionMap {
		operators = append(operators, operator)
	}
	sort.Strings(operators)
	return strings.TrimSpace(operators[0])
}

func extractListStoresFacts(resultText string) []string {
	trimmed := strings.TrimSpace(resultText)
	if trimmed == "" {
		return nil
	}

	lines := strings.Split(trimmed, "\n")
	facts := make([]string, 0, len(lines)*2)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.EqualFold(line, "Stores:") {
			continue
		}

		if idx := strings.Index(line, " schema="); idx > 0 {
			storeName := strings.TrimSpace(line[:idx])
			remainder := strings.TrimSpace(line[idx+1:])
			relationIdx := strings.Index(remainder, " relations=")
			if relationIdx >= 0 {
				schemaPart := strings.TrimSpace(remainder[:relationIdx])
				relationsPart := strings.TrimSpace(remainder[relationIdx+1:])
				facts = append(facts, fmt.Sprintf("list_stores confirmed %s %s", storeName, schemaPart))
				facts = append(facts, fmt.Sprintf("list_stores confirmed %s %s", storeName, relationsPart))
				continue
			}
			facts = append(facts, fmt.Sprintf("list_stores confirmed %s %s", storeName, remainder))
			continue
		}

		facts = append(facts, fmt.Sprintf("list_stores returned: %s", line))
	}
	return facts
}

func isRecoverableRepairResult(result ReActToolResult, toolName string, strategy string) bool {
	if result.Name != toolName || !strings.Contains(result.Result, "Retry instruction:") {
		return false
	}
	if strategy == "" {
		return true
	}
	return strings.Contains(result.Result, fmt.Sprintf("Repair strategy: %s", strategy))
}

func hasSuccessfulTool(results []ReActToolResult, toolName string) bool {
	for _, result := range results {
		if result.Name == toolName && !strings.Contains(result.Result, "Retry instruction:") {
			return true
		}
	}
	return false
}

func hasSuccessfulToolSequence(results []ReActToolResult, names ...string) bool {
	if len(names) == 0 {
		return false
	}
	nameIdx := 0
	for _, result := range results {
		if strings.Contains(result.Result, "Retry instruction:") {
			continue
		}
		if result.Name != names[nameIdx] {
			continue
		}
		nameIdx++
		if nameIdx == len(names) {
			return true
		}
	}
	return false
}
