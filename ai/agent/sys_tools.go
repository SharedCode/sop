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

func (a *CopilotAgent) injectToolsForDomain(ctx context.Context, taskCtx *TaskContextClassification) {
	if taskCtx == nil {
		return
	}

	if focused := a.buildFocusedToolContext(taskCtx); focused != "" {
		a.markMRUCategoryWithSource(SYSTEM_TOOLS, "\n"+focused, MRUSourceSystemTools)
	}
}
