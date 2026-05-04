package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
)

// searchKnowledgeBase searches a specified knowledge base in the given DB.
func (a *CopilotAgent) searchKnowledgeBase(ctx context.Context, db *database.Database, kbName string, query string, limit int) (string, error) {
	if db == nil {
		return "", fmt.Errorf("database is null")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil && a.service.Domain().Embedder() != nil {
		embedder = a.service.Domain().Embedder()
	}

	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder)
	if err != nil {
		// KB might not exist, silently return empty or error
		return "", fmt.Errorf("failed to open kb %s: %w", kbName, err)
	}

	var results []string

	// 1. Semantic Search
	if embedder != nil {
		vecs, err := embedder.EmbedTexts(ctx, []string{query})
		if err == nil && len(vecs) > 0 {
			hits, err := kb.SearchSemantics(ctx, vecs[0], &memory.SearchOptions[map[string]any]{Limit: limit})
			if err == nil && len(hits) > 0 {
				results = append(results, "--- Semantic Matches ---")
				for _, h := range hits {
					// We only have Payload dynamically. In KnowledgeBase, it natively returns map[string]any payload representing the thought?
					// Wait, the payload might contain the category and text? Yes, IngestThought puts it there or it's the raw data.
					text := ""
					category := ""
					if textVal, ok := h.Payload["text"].(string); ok {
						text = textVal
					}
					if catVal, ok := h.Payload["category"].(string); ok {
						category = catVal
					}
					results = append(results, fmt.Sprintf("Score: %.2f | Category: %s\nText: %s", h.Score, category, text))
				}
			}
		}
	}

	// 2. Keyword Search
	keywordHits, err := kb.SearchKeywords(ctx, query, &memory.SearchOptions[map[string]any]{Limit: limit})
	if err == nil && len(keywordHits) > 0 {
		results = append(results, "--- Keyword Matches ---")
		for _, h := range keywordHits {
			text := ""
			category := ""
			if textVal, ok := h.Payload["text"].(string); ok {
				text = textVal
			}
			if catVal, ok := h.Payload["category"].(string); ok {
				category = catVal
			}
			results = append(results, fmt.Sprintf("Score: %.2f | Category: %s\nText: %s", h.Score, category, text))
		}
	}

	if len(results) == 0 {
		return "No results found.", nil
	}

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

// toolSearchSopKB scans the SystemDB for SOP platform instructions.
func (a *CopilotAgent) toolSearchSopKB(ctx context.Context, args map[string]any) (string, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return "", fmt.Errorf("query is required")
	}
	limit := 5
	if l, ok := args["limit"].(float64); ok {
		limit = int(l)
	}

	// Hardcoded to only scan SystemDB for "system_knowledge" / SOP docs
	// Assuming the SOP tool KB is named "system_knowledge" or similar.
	// Will use "system_knowledge" for SystemDB.
	kbName := "system_knowledge"

	// Tier 1 Hardcodes searching exactly the system DB.
	db := a.systemDB

	return a.searchKnowledgeBase(ctx, db, kbName, query, limit)
}

// toolSearchDomainKB maps to the primary KB selected by the user.
func (a *CopilotAgent) toolSearchDomainKB(ctx context.Context, args map[string]any) (string, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return "", fmt.Errorf("query is required")
	}
	limit := 5
	if l, ok := args["limit"].(float64); ok {
		limit = int(l)
	}

	p := ai.GetSessionPayload(ctx)
	if p == nil || p.ActiveDomain == "" {
		return "No active domain KB selected by user.", nil
	}

	db := a.resolveDBForKB(ctx, p.ActiveDomain)
	return a.searchKnowledgeBase(ctx, db, p.ActiveDomain, query, limit)
}

// toolSearchCustomKBs iterates over the SelectedKBs array.
func (a *CopilotAgent) toolSearchCustomKBs(ctx context.Context, args map[string]any) (string, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return "", fmt.Errorf("query is required")
	}
	limit := 3
	if l, ok := args["limit"].(float64); ok {
		limit = int(l)
	}

	p := ai.GetSessionPayload(ctx)
	if p == nil || len(p.SelectedKBs) == 0 {
		return "No custom KBs selected.", nil
	}

	var allResults []string
	for _, kbName := range p.SelectedKBs {
		db := a.resolveDBForKB(ctx, kbName)
		res, err := a.searchKnowledgeBase(ctx, db, kbName, query, limit)
		if err == nil && res != "No results found." {
			allResults = append(allResults, fmt.Sprintf("=== Results from %s ===\n%s", kbName, res))
		}
	}

	if len(allResults) == 0 {
		return "No results found in any selected KBs.", nil
	}

	return strings.Join(allResults, "\n\n"), nil
}
