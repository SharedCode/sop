package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
)

func (a *CopilotAgent) registerSpaceTools(ctx context.Context) {
	a.registry.RegisterWithUI("mint_to_space", "Mints a declarative fact, observation, or solution directly into a designated UI Knowledge Base (Space) to scale learning to other users.", "Directly stores information as a permanent KnowledgeChunk.", "(kb_name: string, content: string, category?: string)", a.toolMintToSpace)
	a.registry.RegisterWithUI("delete_space", "Deletes an entire Space (Knowledge Base).", "Removes a space and all of its contents.", "(kb_name: string)", a.toolDeleteSpace)
	a.registry.RegisterWithUI("enrich_space", "Forces the background process or directly executes knowledge base enrichment on a given item or entire KB.", "Executes knowledge base sleep cycle compilation.", "(kb_name: string)", a.toolEnrichSpace)
	a.registry.RegisterWithUI("update_space_config", "Updates the configuration of a specified Space (Knowledge Base). Needs JSON config map.", "Sets the routing rules, system prompts, and tool access for a specific Space.", "(kb_name: string, config: object)", a.toolUpdateSpaceConfig)
	a.registry.RegisterWithUI("read_space_config", "Retrieves the current configuration rules of a specified Space (Knowledge Base).", "Returns the routing rules, system prompts, and tool access applied to the space.", "(kb_name: string)", a.toolReadSpaceConfig)
	a.registry.RegisterWithUI("vectorize_space", "Triggers vectorization for the entire Space (Knowledge Base) to compute semantic embeddings.", "Generates AI embeddings for all unvectorized items in the space.", "(kb_name: string)", a.toolVectorizeSpace)
	a.registry.RegisterWithUI("vectorize_space_categories", "Triggers vectorization for specific Categories within the Space.", "Generates AI embeddings for items scoped to specific categories.", "(kb_name: string, categories: []string)", a.toolVectorizeCategories)
	a.registry.RegisterWithUI("vectorize_space_items", "Triggers vectorization for specific Items within a Category.", "Generates AI embeddings for specific items.", "(kb_name: string, category: string, item_names: []string)", a.toolVectorizeItems)
}

func emitSpaceMutationEvent(ctx context.Context, action string, databaseName string, kbName string) {
	if strings.TrimSpace(kbName) == "" {
		return
	}
	if streamer, ok := ctx.Value(ai.CtxKeyEventStreamer).(func(string, any)); ok && streamer != nil {
		streamer("space_mutation", map[string]any{
			"action":   action,
			"database": strings.TrimSpace(databaseName),
			"kb_name":  strings.TrimSpace(kbName),
		})
	}
}

// Helper to get db
func (a *CopilotAgent) getDBForSpaceContext(ctx context.Context, args map[string]any) (*database.Database, error) {
	p := ai.GetSessionPayload(ctx)
	var dbName string
	if val, ok := args["database"].(string); ok && val != "" {
		dbName = val
	} else if p != nil {
		dbName = p.CurrentDB
	}

	if dbName != "" {
		if dbName == "system" && a.systemDB != nil {
			return a.systemDB, nil
		} else if opts, ok := a.databases[dbName]; ok {
			return database.NewDatabase(opts), nil
		}
	}
	return nil, fmt.Errorf("database not found: %s", dbName)
}

func (a *CopilotAgent) toolCreateSpace(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)
	if kbName == "" {
		return "", fmt.Errorf("kb_name required")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil && a.service.Domain().Embedder() != nil {
		embedder = a.service.Domain().Embedder()
	}

	_, err = db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		return "", err
	}
	tx.Commit(ctx)
	databaseName := ""
	if p := ai.GetSessionPayload(ctx); p != nil {
		databaseName = p.CurrentDB
	}
	emitSpaceMutationEvent(ctx, "create", databaseName, kbName)
	return fmt.Sprintf("Space/KnowledgeBase '%s' created/opened successfully.\n[[REFRESH_SPACES:%s]]", kbName, kbName), nil
}

func (a *CopilotAgent) toolDeleteSpace(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	p := ai.GetSessionPayload(ctx)
	kbName, _ := args["kb_name"].(string)
	if kbName == "" {
		if fallback, ok := args["name"].(string); ok && fallback != "" {
			kbName = fallback
		} else if fallback, ok := args["space_name"].(string); ok && fallback != "" {
			kbName = fallback
		} else if fallback, ok := args["space"].(string); ok && fallback != "" {
			kbName = fallback
		} else if p != nil {
			if inferred, ok := parseDeleteSpaceRequest(p.CurrentUserQuery); ok {
				kbName = inferred
			}
		}
	}
	if kbName == "" {
		return "", fmt.Errorf("kb_name required")
	}

	err = db.RemoveKnowledgeBase(ctx, kbName)
	if err != nil {
		return "", err
	}
	databaseName := ""
	if p != nil {
		databaseName = p.CurrentDB
	}
	emitSpaceMutationEvent(ctx, "delete", databaseName, kbName)
	return fmt.Sprintf("Space/KnowledgeBase '%s' deleted successfully.\n[[REFRESH_SPACES]]", kbName), nil
}

func (a *CopilotAgent) toolUpdateSpaceConfig(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)
	if kbName == "" {
		return "", fmt.Errorf("kb_name required")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}
	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		return "", err
	}

	configMap, _ := args["config"].(map[string]any)
	configBytes, _ := json.Marshal(configMap)
	var config memory.KnowledgeBaseConfig
	json.Unmarshal(configBytes, &config)

	err = kb.SetConfig(ctx, &config)
	if err != nil {
		return "", err
	}
	tx.Commit(ctx)
	return fmt.Sprintf("Config updated for '%s'", kbName), nil
}

func (a *CopilotAgent) toolReadSpaceConfig(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)
	if kbName == "" {
		return "", fmt.Errorf("kb_name required")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}
	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		return "", err
	}

	cfg, err := kb.GetConfig(ctx)
	if err != nil {
		return "", err
	}
	b, _ := json.Marshal(cfg)
	return string(b), nil
}

func (a *CopilotAgent) toolVectorizeSpace(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)
	batchSize := 100
	if val, ok := args["batch_size"].(float64); ok && val > 0 {
		batchSize = int(val)
	}

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}

	err = db.Vectorize(ctx, kbName, a.brain, embedder, batchSize)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Space '%s' vectorized.", kbName), nil
}

func (a *CopilotAgent) toolVectorizeCategories(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)
	batchSize := 100
	if val, ok := args["batch_size"].(float64); ok && val > 0 {
		batchSize = int(val)
	}

	idsRaw, _ := args["category_ids"].([]any)
	var ids []sop.UUID
	for _, v := range idsRaw {
		if s, ok := v.(string); ok {
			u, e := sop.ParseUUID(s)
			if e == nil {
				ids = append(ids, u)
			}
		}
	}

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}

	err = db.VectorizeCategories(ctx, kbName, a.brain, embedder, batchSize, ids)
	if err != nil {
		return "", err
	}
	return "Categories vectorized.", nil
}

func (a *CopilotAgent) toolVectorizeItems(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)
	batchSize := 100
	if val, ok := args["batch_size"].(float64); ok {
		batchSize = int(val)
	}

	var catID sop.UUID
	if val, ok := args["category_id"].(string); ok {
		catID, _ = sop.ParseUUID(val)
	}

	idsRaw, _ := args["item_ids"].([]any)
	var itemIDs []sop.UUID
	for _, v := range idsRaw {
		if s, ok := v.(string); ok {
			u, e := sop.ParseUUID(s)
			if e == nil {
				itemIDs = append(itemIDs, u)
			}
		}
	}

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}

	err = db.VectorizeItems(ctx, kbName, a.brain, embedder, batchSize, catID, itemIDs)
	if err != nil {
		return "", err
	}
	return "Items vectorized.", nil
}

func (a *CopilotAgent) toolUpsertCategories(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}
	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		return "", err
	}

	paramsRaw, _ := args["parameters"].([]any)
	b, _ := json.Marshal(paramsRaw)
	var params []memory.UpsertCategoryParam
	json.Unmarshal(b, &params)

	err = kb.UpsertCategories(ctx, params)
	if err != nil {
		return "", err
	}
	tx.Commit(ctx)
	return "Categories upserted.", nil
}

func (a *CopilotAgent) toolDeleteCategories(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}
	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		return "", err
	}

	idsRaw, _ := args["category_ids"].([]any)
	var ids []sop.UUID
	for _, v := range idsRaw {
		if s, ok := v.(string); ok {
			u, _ := sop.ParseUUID(s)
			ids = append(ids, u)
		}
	}

	err = kb.DeleteCategories(ctx, ids)
	if err != nil {
		return "", err
	}
	tx.Commit(ctx)
	return "Categories deleted.", nil
}

func (a *CopilotAgent) toolListCategories(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}
	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		return "", err
	}

	var p memory.ListCategoriesParam
	p.Limit = 100
	if val, ok := args["limit"].(float64); ok && val > 0 {
		p.Limit = int(val)
	}
	if val, ok := args["offset"].(float64); ok && val > 0 {
		p.Offset = int(val)
	}
	if val, ok := args["parent_path"].(string); ok {
		p.ParentPath = val
	}

	cats, total, err := kb.ListCategories(ctx, p)
	if err != nil {
		return "", err
	}
	res := map[string]any{"categories": cats, "total": total}
	b, _ := json.Marshal(res)
	return string(b), nil
}

func (a *CopilotAgent) toolUpsertItems(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}
	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		return "", err
	}

	paramsRaw, _ := args["parameters"].([]any)
	b, _ := json.Marshal(paramsRaw)
	var params []memory.UpsertItemParam[map[string]any]
	json.Unmarshal(b, &params)

	err = kb.UpsertItems(ctx, params)
	if err != nil {
		return "", err
	}
	tx.Commit(ctx)
	return "Items upserted.", nil
}

func (a *CopilotAgent) toolDeleteItems(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}
	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		return "", err
	}

	idsRaw, _ := args["item_keys"].([]any)
	var ids []memory.ItemKey
	for _, v := range idsRaw {
		if m, ok := v.(map[string]any); ok {
			var k memory.ItemKey
			if catStr, ok := m["category_id"].(string); ok {
				k.CategoryID, _ = sop.ParseUUID(catStr)
			}
			if itmStr, ok := m["item_id"].(string); ok {
				k.ItemID, _ = sop.ParseUUID(itmStr)
			}
			ids = append(ids, k)
		}
	}

	err = kb.DeleteItems(ctx, ids)
	if err != nil {
		return "", err
	}
	tx.Commit(ctx)
	return "Items deleted.", nil
}

func (a *CopilotAgent) toolListItems(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}
	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		return "", err
	}

	var p memory.ListItemsParam
	p.Limit = 100
	if val, ok := args["limit"].(float64); ok && val > 0 {
		p.Limit = int(val)
	}
	if val, ok := args["offset"].(float64); ok && val > 0 {
		p.Offset = int(val)
	}
	if val, ok := args["category_path"].(string); ok {
		p.CategoryPath = val
	}

	items, total, err := kb.ListItems(ctx, p)
	if err != nil {
		return "", err
	}
	res := map[string]any{"items": items, "total": total}
	b, _ := json.Marshal(res)
	return string(b), nil
}

func (a *CopilotAgent) toolSearchItemsByPath(ctx context.Context, args map[string]any) (string, error) {
	db, err := a.getDBForSpaceContext(ctx, args)
	if err != nil {
		return "", err
	}
	kbName, _ := args["kb_name"].(string)

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil {
		embedder = a.service.Domain().Embedder()
	}
	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, a.brain, embedder, false)
	if err != nil {
		return "", err
	}

	b, _ := json.Marshal(args["parameters"])
	var params []memory.PathSearchParam
	json.Unmarshal(b, &params)

	items, err := kb.SearchByPath(ctx, params)
	if err != nil {
		return "", err
	}
	res := map[string]any{"items": items}
	resB, _ := json.Marshal(res)
	return string(resB), nil
}
