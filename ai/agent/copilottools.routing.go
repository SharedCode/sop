package agent

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"strings"

	"github.com/sharedcode/sop/ai"
)

const searchKBArgsSchema = `{"type":"object","properties":{"kb_name":{"type":"string","description":"Single knowledge base to search."},"query":{"type":"string","description":"Natural language query to run in the target knowledge base."},"limit":{"type":"integer","description":"Maximum number of hits to return. Defaults to 5."}},"required":["kb_name","query"]}`

const handoffToAvatarArgsSchema = `{"type":"object","properties":{"avatar_kb_name":{"type":"string","description":"Avatar knowledge base name that should receive the delegated task."},"task_context":{"type":"object","description":"Structured task payload to hand off to the avatar."}},"required":["avatar_kb_name","task_context"]}`

const concludeTopicArgsSchema = `{"type":"object","properties":{"summary":{"type":"string","description":"Compact summary of the resolved topic or thread."},"topic_label":{"type":"string","description":"Short label for the topic being concluded."}},"required":["summary","topic_label"]}`

func (a *CopilotAgent) toolSearchKB(ctx context.Context, args map[string]any) (string, error) {
	kbNameRaw, ok := args["kb_name"]
	if !ok {
		return "", fmt.Errorf("missing required argument 'kb_name'")
	}
	queryRaw, ok := args["query"]
	if !ok {
		return "", fmt.Errorf("missing required argument 'query'")
	}

	kbName, ok := kbNameRaw.(string)
	if !ok || strings.TrimSpace(kbName) == "" {
		return "", fmt.Errorf("'kb_name' must be a non-empty string")
	}
	kbName = ai.CanonicalKBName(kbName)
	query, ok := queryRaw.(string)
	if !ok || strings.TrimSpace(query) == "" {
		return "", fmt.Errorf("'query' must be a non-empty string")
	}
	query = stripRoutingPrefix(query, kbName)

	limit := 5
	if rawLimit, ok := args["limit"].(float64); ok && rawLimit > 0 {
		limit = int(rawLimit)
	}

	log.Info("search_space tool invoked", "kb_name", kbName, "query", query, "limit", limit)

	db := a.resolveDBForKB(ctx, kbName)
	if db == nil {
		return "", fmt.Errorf("could not resolve DB for KB '%s'", kbName)
	}

	return a.searchKnowledgeBase(ctx, db, kbName, query, "", "", limit)
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
	a.registry.RegisterWithUI("search_space", "Searches one knowledge base.", "Use this when the user names one KB explicitly, such as 'SOP'.", searchKBArgsSchema, wrapStringTool(a.toolSearchKB))
	a.registry.RegisterWithUI("handoff_to_avatar", "Yields control to an Avatar-specific Knowledge Base to execute a task.", "Handoff to an Avatar", handoffToAvatarArgsSchema, wrapStringTool(a.toolHandoffToAvatar))

	a.registry.Register("conclude_topic", "Conclusion of the current conversation thread. Use this when the user is satisfied, a resolution is reached, or to summarize before moving to a new topic. This saves the summary to memory and cleans up the context.", concludeTopicArgsSchema, wrapStringTool(a.toolConcludeTopic))
}
