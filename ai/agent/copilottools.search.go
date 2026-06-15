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

func looksLikeCategoryPath(query string) bool {
	trimmed := strings.TrimSpace(query)
	trimmed = strings.Trim(trimmed, "\"'`")
	if trimmed == "" {
		return false
	}

	if strings.ContainsAny(trimmed, "/\\") {
		parts := strings.FieldsFunc(trimmed, func(r rune) bool {
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

	if strings.Contains(trimmed, "/") || strings.Contains(trimmed, "\\") {
		return trimmed
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
	if path == "" || !looksLikeCategoryPath(path) {
		return "", ""
	}

	return path, instruction
}

func embedCategoryPath(ctx context.Context, catPath string, embedder ai.Embeddings) ([][]float32, error) {
	parts := strings.Split(catPath, "/")
	if len(parts) == 0 || (len(parts) == 1 && strings.TrimSpace(parts[0]) == "") {
		parts = strings.Split(catPath, "\\")
	}
	if len(parts) == 0 || (len(parts) == 1 && strings.TrimSpace(parts[0]) == "") {
		return nil, nil
	}
	vecs, err := embedder.EmbedTexts(ctx, parts)
	if err != nil {
		return nil, err
	}
	return vecs, nil
}

func (a *CopilotAgent) searchKnowledgeBase(ctx context.Context, db *database.Database, kbName string, query string, catPath string, category string, textSearchEnabled bool, limit int) (string, error) {
	if db == nil {
		return "", fmt.Errorf("database is null")
	}
	kbName = ai.CanonicalKBName(kbName)
	query = stripRoutingPrefix(query, kbName)
	pathQuery, llmInstruction := splitCategoryPathInstruction(query)
	pathPrompt := pathQuery != "" || looksLikeCategoryPath(query)
	log.Info("searchKnowledgeBase start", "kb_name", kbName, "query", query, "category_path_query", pathQuery, "llm_instruction", llmInstruction, "path_prompt", pathPrompt, "category", category, "category_path", catPath, "text_search_enabled", textSearchEnabled, "limit", limit)

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil && a.service.Domain().Embedder() != nil {
		embedder = a.service.Domain().Embedder()
	}

	// We pass documentMode=false. TextSearch configuration is now inferred natively within the DB.
	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		log.Error("searchKnowledgeBase open failed", "kb_name", kbName, "error", err)
		return "", fmt.Errorf("failed to open kb %s: %w", kbName, err)
	}

	var results []string

	if pathQuery != "" {
		searchText := ""
		if !pathPrompt {
			searchText = query
		}
		if embedder != nil {
			vecs, err := embedCategoryPath(ctx, pathQuery, embedder)
			if err == nil && len(vecs) > 0 {
				cats, err := kb.Store.SemanticCategoryByPath(ctx, vecs)
				if err == nil && len(cats) > 0 {
					results = append(results, "--- Semantic Category Candidates ---")
					for _, cat := range cats {
						if cat == nil {
							continue
						}
						uri := cat.Path
						if strings.TrimSpace(uri) == "" {
							uri = cat.ID.String()
						}
						results = append(results, fmt.Sprintf("SemanticCategoryURI: %s\nCategoryPath: %s\nCategoryID: %s", uri, cat.Path, cat.ID.String()))
					}
				}
			}
		}
		pathHits, err := kb.SearchByPath(ctx, []memory.PathSearchParam{{CategoryPath: pathQuery, SearchText: searchText}})
		if err == nil && len(pathHits) > 0 {
			results = append(results, "--- Category Path Matches ---")
			for _, h := range pathHits {
				if len(results) >= limit+1 {
					break
				}
				text := ""
				categoryVal := ""
				if descVal, ok := h.Data["description"].(string); ok {
					text = descVal
				} else if textVal, ok := h.Data["text"].(string); ok {
					text = textVal
				}
				if text == "" {
					b, _ := json.Marshal(h.Data)
					text = string(b)
				}
				if catVal, ok := h.Data["category"].(string); ok {
					categoryVal = catVal
				}
				docID := h.DocID.First()
				link := ""
				if docID != "" {
					link = fmt.Sprintf("\n[View Source Document](/viewer?docID=%s)", docID)
				}
				results = append(results, fmt.Sprintf("CategoryPath: %s\nText: %s%s", categoryVal, text, link))
			}
		}
	}

	if catPath != "" {
		pathHits, err := kb.SearchByPath(ctx, []memory.PathSearchParam{{CategoryPath: catPath, SearchText: query}})
		if err == nil && len(pathHits) > 0 {
			results = append(results, "--- Lexical Path Matches ---")
			limitHit := 0
			for _, h := range pathHits {
				if limitHit >= limit {
					break
				}
				text := ""
				categoryVal := ""
				if h.Data != nil {
					if descVal, ok := h.Data["description"].(string); ok {
						text = descVal
					} else if textVal, ok := h.Data["text"].(string); ok {
						text = textVal
					}
					if text == "" {
						b, _ := json.Marshal(h.Data)
						text = string(b)
					}
					if catVal, ok := h.Data["category"].(string); ok {
						categoryVal = catVal
					}
				} else if len(h.Summaries) > 0 {
					text = strings.Join(h.Summaries, " ")
				}

				if text == "" {
					continue
				}

				docID := h.DocID.First()
				link := ""
				if docID != "" {
					link = fmt.Sprintf("\n[View Source Document](/viewer?docID=%s)", docID)
				}

				results = append(results, fmt.Sprintf("CategoryPath: %s\nText: %s%s", categoryVal, text, link))
				limitHit++
			}
		}
	} else if category != "" && embedder != nil {
		vecs, err := embedder.EmbedTexts(ctx, []string{query})
		if err == nil && len(vecs) > 0 {
			hits, err := kb.SearchSemantics(ctx, vecs[0], &memory.SearchOptions[map[string]any]{CategoryPath: category, Limit: limit})
			if err == nil && len(hits) > 0 {
				results = append(results, "--- Semantic Matches ---")
				for _, h := range hits {
					text := ""
					categoryVal := ""
					if descVal, ok := h.Payload["description"].(string); ok {
						text = descVal
					} else if textVal, ok := h.Payload["text"].(string); ok {
						text = textVal
					}
					if text == "" {
						b, _ := json.Marshal(h.Payload)
						text = string(b)
					}
					if catVal, ok := h.Payload["category"].(string); ok {
						categoryVal = catVal
					}

					docID := memory.DocIDs(h.DocID).First()
					link := ""
					if docID != "" {
						link = fmt.Sprintf("\n[View Source Document](/viewer?docID=%s)", docID)
					}

					results = append(results, fmt.Sprintf("Score: %.2f | CategoryPath: %s\nText: %s%s", h.Score, categoryVal, text, link))
				}
			}
		}
	} else if textSearchEnabled {
		keywordHits, err := kb.SearchKeywords(ctx, query, &memory.SearchOptions[map[string]any]{Limit: limit})
		if err == nil && len(keywordHits) > 0 {
			results = append(results, "--- Keyword Matches ---")
			for _, h := range keywordHits {
				text := ""
				categoryVal := ""
				if descVal, ok := h.Payload["description"].(string); ok {
					text = descVal
				} else if textVal, ok := h.Payload["text"].(string); ok {
					text = textVal
				}
				if text == "" {
					b, _ := json.Marshal(h.Payload)
					text = string(b)
				}
				if catVal, ok := h.Payload["category"].(string); ok {
					categoryVal = catVal
				}

				docID := memory.DocIDs(h.DocID).First()
				link := ""
				if docID != "" {
					link = fmt.Sprintf("\n[View Source Document](/viewer?docID=%s)", docID)
				}

				results = append(results, fmt.Sprintf("Score: %.2f | CategoryPath: %s\nText: %s%s", h.Score, categoryVal, text, link))
			}
		}
	} else if embedder != nil {
		vecs, err := embedder.EmbedTexts(ctx, []string{query})
		if err == nil && len(vecs) > 0 {
			hits, err := kb.SearchSemantics(ctx, vecs[0], &memory.SearchOptions[map[string]any]{Limit: limit})
			if err == nil && len(hits) > 0 {
				results = append(results, "--- Semantic Matches ---")
				for _, h := range hits {
					text := ""
					categoryVal := ""
					if descVal, ok := h.Payload["description"].(string); ok {
						text = descVal
					} else if textVal, ok := h.Payload["text"].(string); ok {
						text = textVal
					}
					if text == "" {
						b, _ := json.Marshal(h.Payload)
						text = string(b)
					}
					if catVal, ok := h.Payload["category"].(string); ok {
						categoryVal = catVal
					}

					docID := memory.DocIDs(h.DocID).First()
					link := ""
					if docID != "" {
						link = fmt.Sprintf("\n[View Source Document](/viewer?docID=%s)", docID)
					}

					results = append(results, fmt.Sprintf("Score: %.2f | CategoryPath: %s\nText: %s%s", h.Score, categoryVal, text, link))
				}
			}
		}
	}

	if len(results) == 0 {
		return "No results found.", nil
	}

	a.MarkMRUCategory(kbName, fmt.Sprintf("Last searched query: %s", query))
	return strings.Join(results, "\n\n"), nil
}

// resolveDBForKB implements Namespace Shadowing Logic: Tenant DB overrides System DB
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
