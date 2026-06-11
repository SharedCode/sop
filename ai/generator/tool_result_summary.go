package generator

import (
	"encoding/json"
	"fmt"
	"strings"
)

func SummarizeToolResultForLLM(toolName, resultText string, finalToolResult bool) string {
	trimmedTool := strings.TrimSpace(toolName)
	trimmedResult := strings.TrimSpace(resultText)
	if !strings.EqualFold(trimmedTool, "execute_script") {
		return resultText
	}
	if trimmedResult == "" {
		return "execute_script completed with no textual payload. Results, if any, were already streamed to the client."
	}

	if !finalToolResult {
		var rows []json.RawMessage
		if err := json.Unmarshal([]byte(trimmedResult), &rows); err == nil {
			previewRows := rows
			if len(previewRows) > 10 {
				previewRows = previewRows[:10]
			}
			preview, _ := json.Marshal(previewRows)
			return fmt.Sprintf("execute_script returned %d row(s). Preview (first %d rows): %s", len(rows), len(previewRows), string(preview))
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(trimmedResult), &record); err == nil {
			preview, _ := json.Marshal(record)
			if len(preview) > 400 {
				preview = preview[:400]
			}
			return fmt.Sprintf("execute_script returned one structured record. Preview: %s", string(preview))
		}
		if len(trimmedResult) > 500 {
			return fmt.Sprintf("execute_script returned a large textual payload (%d chars). Preview: %q", len(trimmedResult), trimmedResult[:500])
		}
		return resultText
	}

	var rows []json.RawMessage
	if err := json.Unmarshal([]byte(trimmedResult), &rows); err == nil {
		return fmt.Sprintf("execute_script completed successfully and returned %d row(s). The full row payload was already streamed to the client. Do not restate the rows; provide at most a brief summary.", len(rows))
	}

	var record map[string]any
	if err := json.Unmarshal([]byte(trimmedResult), &record); err == nil {
		return "execute_script completed successfully and returned one structured record. The full payload was already streamed to the client. Do not restate the record; provide at most a brief summary."
	}

	if len(trimmedResult) > 1000 {
		return fmt.Sprintf("execute_script completed successfully and returned a large textual payload (%d chars). The full payload was already streamed to the client. Do not restate it; provide at most a brief summary.", len(trimmedResult))
	}

	return resultText
}

func ExtractToolResultText(response any) string {
	switch v := response.(type) {
	case string:
		return v
	case map[string]any:
		if result, ok := v["result"].(string); ok {
			return result
		}
		if result, ok := v["tool_result"].(string); ok {
			return result
		}
		bytes, _ := json.Marshal(v)
		return string(bytes)
	default:
		bytes, _ := json.Marshal(response)
		return string(bytes)
	}
}
