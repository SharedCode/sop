package parser

import (
	"strings"
)

// ParseSlashCommand parses a slash command line into the tool name and a map of arguments.
// It detects "name=value" pairs and also handles some positional arguments based on heuristics for known tools.
// Returns (toolName, args, error).
func ParseSlashCommand(input string) (string, map[string]any, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", nil, nil
	}

	// Simple state machine to parse input
	var parts []string
	var current strings.Builder
	inQuote := false
	escape := false

	for _, r := range input {
		if escape {
			current.WriteRune(r)
			escape = false
			continue
		}

		if r == '\\' {
			escape = true
			continue
		}

		if r == '"' {
			inQuote = !inQuote
			continue
		}

		if r == ' ' && !inQuote {
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		} else {
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	if len(parts) == 0 {
		return "", nil, nil
	}

	toolName := parts[0]
	// Remove leading slash if present (handle both "/fetch" and "fetch")
	if strings.HasPrefix(toolName, "/") {
		toolName = toolName[1:]
	}

	args := make(map[string]any)
	var positional []string

	for _, part := range parts[1:] {
		// Split on first =
		idx := strings.Index(part, "=")
		if idx > 0 {
			key := part[:idx]
			val := part[idx+1:]
			args[key] = val
		} else {
			positional = append(positional, part)
		}
	}

	if len(positional) > 0 {
		// Heuristics for common commands to map positional args to named args naturally
		switch toolName {
		case "create_script", "save_script", "get_script_details":
			if len(positional) > 0 {
				args["name"] = positional[0] // infer "name" arg is the first positional
			}
		case "run", "save_last_step":
			if len(positional) > 0 {
				args["script"] = positional[0] // infer "script" arg is the first positional
			}
			if toolName == "run" && len(positional) > 1 {
				// /run script param1=val1... (params already handled by key=value logic)
			}
		case "fetch":
			if len(positional) > 0 {
				args["store"] = positional[0]
				// Look for keywords in remaining positional args
				for i := 1; i < len(positional); i++ {
					// Handle "limit <val>"
					if positional[i] == "limit" && i+1 < len(positional) {
						args["limit"] = positional[i+1]
						i++
						continue
					}
					// Handle "prefix <val>"
					if positional[i] == "prefix" && i+1 < len(positional) {
						args["prefix"] = positional[i+1]
						i++
						continue
					}
				}
			}
		case "select":
			if len(positional) > 0 {
				args["store"] = positional[0] // infer "store" is first arg
			}
		case "add", "update", "delete", "delete_record":
			if len(positional) > 0 {
				args["store"] = positional[0]
			}
			if len(positional) > 1 && (toolName == "add" || toolName == "update" || toolName == "delete" || toolName == "delete_record") {
				args["key"] = positional[1]
			}
			if len(positional) > 2 && (toolName == "add" || toolName == "update") {
				args["value"] = positional[2]
			}
		case "script_add_step_from_last", "add_step_from_last": // Legacy mapping just in case
		}

		args["_positional"] = positional
	}

	return toolName, args, nil
}
