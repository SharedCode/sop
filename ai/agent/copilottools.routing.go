package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

func (a *CopilotAgent) toolRouteToMultiKB(ctx context.Context, args map[string]any) (string, error) {
	kbNamesRaw, ok := args["kb_names"]
	if !ok {
		return "", fmt.Errorf("missing required argument 'kb_names'")
	}
	queryRaw, ok := args["optimized_query"]
	if !ok {
		return "", fmt.Errorf("missing required argument 'optimized_query'")
	}

	kbNames, ok := kbNamesRaw.([]any)
	if !ok {
		return "", fmt.Errorf("'kb_names' must be a list of strings")
	}

	query, ok := queryRaw.(string)
	if !ok {
		return "", fmt.Errorf("'optimized_query' must be a string")
	}

	limit := 5
	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []string
	var errs []error

	for _, nameAny := range kbNames {
		nameStr, ok := nameAny.(string)
		if !ok {
			continue
		}

		wg.Add(1)
		go func(kbName string) {
			defer wg.Done()

			db := a.resolveDBForKB(ctx, kbName)
			if db == nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("could not resolve DB for KB '%s'", kbName))
				mu.Unlock()
				return
			}

			res, err := a.searchKnowledgeBase(ctx, db, kbName, query, "", "", true, limit)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, fmt.Errorf("error searching KB '%s': %w", kbName, err))
			} else {
				results = append(results, fmt.Sprintf("=== Results from KB: %s ===\n%s", kbName, res))
			}
		}(nameStr)
	}

	wg.Wait()

	if len(results) == 0 {
		if len(errs) > 0 {
			return "", fmt.Errorf("all searches failed. First error: %w", errs[0])
		}
		return "No results found across specified KBs.", nil
	}

	return strings.Join(results, "\n\n"), nil
}

func (a *CopilotAgent) toolHandoffToAvatar(ctx context.Context, args map[string]any) (string, error) {
	avatarNameRaw, ok := args["avatar_kb_name"]
	if !ok {
		return "", fmt.Errorf("missing required argument 'avatar_kb_name'")
	}
	taskContextRaw, ok := args["task_context"]
	if !ok {
		return "", fmt.Errorf("missing required argument 'task_context'")
	}

	avatarName, ok := avatarNameRaw.(string)
	if !ok {
		return "", fmt.Errorf("'avatar_kb_name' must be a string")
	}

	taskContextBytes, _ := json.Marshal(taskContextRaw)

	// Delegate to the Sub-Agent / Avatar logic
	return a.executeAvatarSubAgent(ctx, avatarName, string(taskContextBytes))
}

func (a *CopilotAgent) registerRoutingTools(ctx context.Context) {
	a.registry.RegisterWithUI("route_to_multi_kb", "Routes a query to multiple specific knowledge bases.", "Executes query across given KBs", "(kb_names: Array<string>, optimized_query: string)", a.toolRouteToMultiKB)
	a.registry.RegisterWithUI("handoff_to_avatar", "Yields control to an Avatar-specific Knowledge Base to execute a task.", "Handoff to an Avatar", "(avatar_kb_name: string, task_context: object)", a.toolHandoffToAvatar)

	a.registry.Register("conclude_topic", "Conclusion of the current conversation thread. Use this when the user is satisfied, a resolution is reached, or to summarize before moving to a new topic. This saves the summary to memory and cleans up the context.", "(summary: string, topic_label: string)", a.toolConcludeTopic)
}
