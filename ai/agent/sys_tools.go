package agent

import (
	"context"
)

const (
	SYSTEM_TOOLS = "System_Tools"
)

func (a *CopilotAgent) getSystemToolsContext(ctx context.Context) string {
	if toolsDef, ok := a.getMRUCategoryBySource(SYSTEM_TOOLS, MRUSourceSystemTools, true); ok && toolsDef != "" {
		return toolsDef
	}
	return ""
}
