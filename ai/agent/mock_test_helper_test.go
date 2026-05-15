package agent

import (
	"encoding/json"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// ExtractToolCallFromMockText parses string responses into tool calls for simulated generator structs.
func ExtractToolCallFromMockText(text string) []ai.ToolCall {
	clean := strings.TrimSpace(text)
	clean = strings.TrimPrefix(clean, "```json")
	clean = strings.TrimSuffix(clean, "```")
	clean = strings.TrimSpace(clean)

	var call struct {
		Tool string         `json:"tool"`
		Args map[string]any `json:"args"`
	}
	if err := json.Unmarshal([]byte(clean), &call); err == nil && call.Tool != "" {
		return []ai.ToolCall{{Name: call.Tool, Args: call.Args}}
	}
	return nil
}
