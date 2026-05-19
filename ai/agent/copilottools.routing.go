package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
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

			res, err := a.searchKnowledgeBase(ctx, db, kbName, query, limit)

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

func (a *CopilotAgent) executeAvatarSubAgent(ctx context.Context, avatarName, taskContext string) (string, error) {
	avatarPrompt := ""
	var allowedTools []string

	if a.systemDB != nil {
		if tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading); err == nil {
			if kb, err := a.systemDB.OpenKnowledgeBase(ctx, avatarName, tx, nil, nil, false); err == nil {
				if cfg, err := kb.GetConfig(ctx); err == nil && cfg != nil {
					if cfg.SystemPrompt != "" {
						avatarPrompt = cfg.SystemPrompt
					}
					allowedTools = cfg.AllowedTools
				}
			}
			tx.Rollback(ctx)
		}
	}

	if avatarPrompt == "" {
		avatarPrompt = fmt.Sprintf("You are the %s Avatar. Your task is strictly limited to your domain.", avatarName)
	}

	avatarPrompt += fmt.Sprintf("\n\nTask Context from Omni Supervisor:\n%s", taskContext)

	engine := &NativeReActEngine{
		EnableObfuscation: false, // Inherit or check ctx if needed
	}

	// Wrapper to restrict tools based on the KB config.
	var executor ai.ToolExecutor = a
	if allowedTools != nil {
		executor = &restrictedExecutor{
			base:         a,
			allowedTools: allowedTools,
		}
	}

	req := ai.ReasoningRequest{
		SystemPrompt: avatarPrompt,
		UserQuery:    "Execute the task outlined in your context.",
		Executor:     executor,
		Generator:    a.resolveGenerator(ctx),
	}

	resp, err := engine.Run(ctx, req)
	if err != nil {
		return "", fmt.Errorf("avatar execution failed: %w", err)
	}

	return fmt.Sprintf("Avatar %s completed task: \n%s", avatarName, resp.FinalText), nil
}

type restrictedExecutor struct {
	base         ai.ToolExecutor
	allowedTools []string
}

func (re *restrictedExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	allowed := false
	for _, t := range re.allowedTools {
		if t == toolName {
			allowed = true
			break
		}
	}
	if !allowed {
		return "", fmt.Errorf("access denied: tool '%s' is not in the allowed list for this Avatar", toolName)
	}
	return re.base.Execute(ctx, toolName, args)
}

func (re *restrictedExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	all, err := re.base.ListTools(ctx)
	if err != nil {
		return nil, err
	}
	var filtered []ai.ToolDefinition
	for _, def := range all {
		allowed := false
		for _, t := range re.allowedTools {
			if t == def.Name {
				allowed = true
				break
			}
		}
		if allowed {
			filtered = append(filtered, def)
		}
	}
	return filtered, nil
}
