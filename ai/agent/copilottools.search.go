package agent

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"regexp"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
)

// Only strip routing tokens that the system recognizes at the start of the query.
var routingTokenPattern = regexp.MustCompile(`(?i)^(?:omni|sop)(?:\s*[:/]\s*|\s*->\s*)`)

// stripRoutingPrefix removes only system-recognized routing prefixes.
// It strips the active KB name prefix when present and then strips
// leading OMNI/SOP routing tokens, but leaves unknown prefixes untouched.
func stripRoutingPrefix(query, kbName string) string {
	trimmed := strings.TrimSpace(query)
	trimmed = strings.Trim(trimmed, "\"'`")
	if trimmed == "" {
		return trimmed
	}

	startsWithOMNI := routingTokenPattern.MatchString(trimmed) && strings.EqualFold(strings.FieldsFunc(trimmed, func(r rune) bool { return r == ':' || r == '/' || r == '-' || r == '>' })[0], "omni")

	if startsWithOMNI {
		trimmed = strings.TrimSpace(routingTokenPattern.ReplaceAllString(trimmed, ""))
		trimmed = strings.TrimSpace(routingTokenPattern.ReplaceAllString(trimmed, ""))
	} else if routingTokenPattern.MatchString(trimmed) {
		trimmed = strings.TrimSpace(routingTokenPattern.ReplaceAllString(trimmed, ""))
	}

	canonicalName := strings.ToLower(ai.CanonicalKBName(kbName))
	for _, prefix := range []string{"sop:" + canonicalName + ":", canonicalName + ":", "sop:"} {
		if strings.HasPrefix(strings.ToLower(trimmed), strings.ToLower(prefix)) {
			trimmed = strings.TrimSpace(trimmed[len(prefix):])
			break
		}
	}

	return strings.TrimSpace(trimmed)
}

func normalizeCategoryPathSeparators(query string) string {
	trimmed := strings.TrimSpace(query)
	trimmed = strings.Trim(trimmed, "\"'`")
	if trimmed == "" {
		return trimmed
	}

	match := regexp.MustCompile(`(?i)^(?P<path>.+?)(?:\s*:\s*llm\b\s*(?P<instruction>.*))?$`).FindStringSubmatch(trimmed)
	if len(match) == 3 {
		path := strings.TrimSpace(match[1])
		return strings.ReplaceAll(path, ":", "/")
	}

	return strings.ReplaceAll(trimmed, ":", "/")
}

func looksLikeCategoryPath(query string) bool {
	trimmed := strings.TrimSpace(query)
	trimmed = strings.Trim(trimmed, "\"'`")
	if trimmed == "" {
		return false
	}

	normalized := normalizeCategoryPathSeparators(trimmed)
	if strings.ContainsAny(normalized, "/\\") {
		parts := strings.FieldsFunc(normalized, func(r rune) bool {
			return r == '/' || r == '\\'
		})
		return len(parts) >= 2
	}
	return false
}

func extractCategoryPathQuery(query string) string {
	trimmed := strings.TrimSpace(query)
	trimmed = strings.Trim(trimmed, "\"'`")
	trimmed = stripRoutingPrefix(trimmed, "")

	path, _ := splitCategoryPathInstruction(trimmed)
	if path != "" {
		return path
	}

	normalized := normalizeCategoryPathSeparators(trimmed)
	if strings.Contains(normalized, "/") || strings.Contains(normalized, "\\") {
		return normalized
	}

	return ""
}

func splitCategoryPathInstruction(query string) (string, string) {
	trimmed := strings.TrimSpace(query)
	trimmed = strings.Trim(trimmed, "\"'`")
	if trimmed == "" {
		return "", ""
	}

	trimmed = stripRoutingPrefix(trimmed, "")

	re := regexp.MustCompile(`(?i)^(?P<path>.+?)(?:\s*:\s*llm\b\s*(?P<instruction>.*))?$`)
	matches := re.FindStringSubmatch(trimmed)
	if len(matches) != 3 {
		return "", ""
	}

	path := strings.TrimSpace(matches[1])
	instruction := strings.TrimSpace(matches[2])
	normalizedPath := normalizeCategoryPathSeparators(path)
	if normalizedPath == "" || !looksLikeCategoryPath(normalizedPath) {
		return "", ""
	}

	return normalizedPath, instruction
}

func (a *CopilotAgent) searchKnowledgeBase(ctx context.Context, db *database.Database, kbName string, query string, catPath string, category string, limit int) (string, error) {
	if db == nil {
		return "", fmt.Errorf("database is null")
	}
	kbName = ai.CanonicalKBName(kbName)
	query = stripRoutingPrefix(query, kbName)
	pathQuery, _ := splitCategoryPathInstruction(query)

	log.Info("searchKnowledgeBase start", "kb_name", kbName, "query", query, "category_path_query", pathQuery, "category", category, "category_path", catPath, "limit", limit)

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil && a.service.Domain().Embedder() != nil {
		embedder = a.service.Domain().Embedder()
	}

	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		log.Error("searchKnowledgeBase open failed", "kb_name", kbName, "error", err)
		return "", fmt.Errorf("failed to open kb %s: %w", kbName, err)
	}

	// Determine effective category path: pathQuery takes precedence, then catPath, then category
	effectivePath := pathQuery
	if effectivePath == "" {
		effectivePath = catPath
	}
	if effectivePath == "" {
		effectivePath = category
	}

	// Single unified search - let Search() handle all intelligent routing:
	// - CategoryPath resolution (lexical + semantic fallback)
	// - Text-based category discovery
	// - Vector search across all candidate categories
	searchReq := memory.SearchRequest[map[string]any]{
		Text:         query,
		CategoryPath: effectivePath,
		Limit:        limit,
	}

	batch, err := kb.Search(ctx, []memory.SearchRequest[map[string]any]{searchReq})
	if err != nil {
		return "", err
	}

	if len(batch) == 0 || len(batch[0]) == 0 {
		return "No results found.", nil
	}

	// Unified result formatting
	var results []string
	for _, h := range batch[0] {
		text := extractHitText(h.Payload)
		categoryVal := extractHitCategory(h.Payload)
		docID := memory.DocIDs(h.DocID).First()
		link := ""
		if docID != "" {
			link = fmt.Sprintf("\n[View Source Document](/viewer?docID=%s)", docID)
		}
		results = append(results, fmt.Sprintf("Score: %.2f | CategoryPath: %s\nText: %s%s", h.Score, categoryVal, text, link))
	}

	a.MarkMRUCategory(kbName, fmt.Sprintf("Last searched query: %s", query))
	return strings.Join(results, "\n\n"), nil
}

func extractHitText(v any) string {
	switch data := v.(type) {
	case map[string]any:
		for _, field := range []string{"text", "description", "content", "page_content", "_raw_content"} {
			if text, ok := data[field].(string); ok && strings.TrimSpace(text) != "" {
				return text
			}
		}
		b, _ := json.Marshal(data)
		return string(b)
	case string:
		return data
	default:
		b, _ := json.Marshal(data)
		return string(b)
	}
}

func extractHitCategory(v any) string {
	switch data := v.(type) {
	case map[string]any:
		// Prefer category_path (full hierarchical path) over category (leaf name only)
		if categoryPath, ok := data["category_path"].(string); ok && strings.TrimSpace(categoryPath) != "" {
			return categoryPath
		}
		if category, ok := data["category"].(string); ok {
			return category
		}
		if category, ok := data["path"].(string); ok {
			return category
		}
	}
	return ""
}

func (a *CopilotAgent) resolveDBForKB(ctx context.Context, kbName string) *database.Database {
	p := ai.GetSessionPayload(ctx)
	if p != nil && p.CurrentDB != "" {
		if opts, ok := a.databases[p.CurrentDB]; ok {
			domainDB := database.NewDatabase(opts)
			if exists, _ := domainDB.KnowledgeBaseExists(ctx, kbName); exists {
				return domainDB
			}
		}
	}

	return a.systemDB
}
