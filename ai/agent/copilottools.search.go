package agent

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
)

// searchKnowledgeBase searches a specified knowledge base in the given DB.
func stripRoutingPrefix(query, kbName string) string {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return trimmed
	}

	canonicalName := strings.ToLower(ai.CanonicalKBName(kbName))
	prefix := canonicalName + ":"
	if canonicalName == "" || !strings.HasPrefix(strings.ToLower(trimmed), prefix) {
		return trimmed
	}

	return strings.TrimSpace(trimmed[len(canonicalName)+1:])
}

func (a *CopilotAgent) searchKnowledgeBase(ctx context.Context, db *database.Database, kbName string, query string, catPath string, category string, textSearchEnabled bool, limit int) (string, error) {
	if db == nil {
		return "", fmt.Errorf("database is null")
	}
	kbName = ai.CanonicalKBName(kbName)
	query = stripRoutingPrefix(query, kbName)
	log.Info("searchKnowledgeBase start", "kb_name", kbName, "query", query, "category", category, "category_path", catPath, "text_search_enabled", textSearchEnabled, "limit", limit)

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

				link := ""
				if h.DocID != "" {
					link = fmt.Sprintf("\n[View Source Document](/viewer?docID=%s)", h.DocID)
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

					link := ""
					if h.DocID != "" {
						link = fmt.Sprintf("\n[View Source Document](/viewer?docID=%s)", h.DocID)
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

				link := ""
				if h.DocID != "" {
					link = fmt.Sprintf("\n[View Source Document](/viewer?docID=%s)", h.DocID)
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

					link := ""
					if h.DocID != "" {
						link = fmt.Sprintf("\n[View Source Document](/viewer?docID=%s)", h.DocID)
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
