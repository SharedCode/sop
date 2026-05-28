package agent

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/jsondb"
)

var crudOrder = []string{"C", "R", "U", "D"}

func collectCRUDFlags(layers []LayerInfo) map[string]bool {
	flags := make(map[string]bool)
	for _, layer := range layers {
		for _, crud := range layer.CRUD {
			crud = strings.ToUpper(strings.TrimSpace(crud))
			if crud != "" {
				flags[crud] = true
			}
		}
	}
	return flags
}

func orderedCRUDFlags(flags map[string]bool) []string {
	ordered := make([]string, 0, len(flags))
	for _, crud := range crudOrder {
		if flags[crud] {
			ordered = append(ordered, crud)
		}
	}
	return ordered
}

func (a *CopilotAgent) getFocusedExecutionContext(ctx context.Context, taskClassification TaskContextClassification) string {
	if isCrossDomain(taskClassification.Layers) {
		return a.buildCrossDomainFocusedExecutionContext(ctx, taskClassification)
	}

	switch {
	case strings.EqualFold(taskClassification.Domain, StoresDomain):
		return a.buildStoresFocusedExecutionContext(ctx, taskClassification)
	case strings.EqualFold(taskClassification.Domain, SpacesDomain):
		return a.buildSpacesFocusedExecutionContext(taskClassification)
	default:
		return ""
	}
}

func (a *CopilotAgent) buildCrossDomainFocusedExecutionContext(ctx context.Context, taskClassification TaskContextClassification) string {
	storesArtifacts := taskClassification.StoresArtifacts
	if len(storesArtifacts) == 0 && strings.EqualFold(taskClassification.Domain, StoresDomain) {
		storesArtifacts = taskClassification.DBArtifacts
	}

	spacesArtifacts := taskClassification.SpacesArtifacts
	if len(spacesArtifacts) == 0 && strings.EqualFold(taskClassification.Domain, SpacesDomain) {
		spacesArtifacts = taskClassification.DBArtifacts
	}

	storesCtx := a.buildStoresFocusedExecutionContext(ctx, TaskContextClassification{
		Domain:      StoresDomain,
		DBArtifacts: storesArtifacts,
		Layers:      taskClassification.Layers,
	})
	spacesCtx := a.buildSpacesFocusedExecutionContext(TaskContextClassification{
		Domain:      SpacesDomain,
		DBArtifacts: spacesArtifacts,
		Layers:      taskClassification.Layers,
	})

	sections := make([]string, 0, 2)
	if storesCtx != "" {
		sections = append(sections, "Stores Execution Context:\n"+storesCtx)
	}
	if spacesCtx != "" {
		sections = append(sections, "Spaces Execution Context:\n"+spacesCtx)
	}

	return strings.Join(sections, "\n\n")
}

func (a *CopilotAgent) buildStoresFocusedExecutionContext(ctx context.Context, taskClassification TaskContextClassification) string {
	crudFlags := collectCRUDFlags(taskClassification.Layers)
	var sb strings.Builder

	sb.WriteString("Domain: Stores\n")
	if ordered := orderedCRUDFlags(crudFlags); len(ordered) > 0 {
		sb.WriteString(fmt.Sprintf("CRUD Scope: %s\n", strings.Join(ordered, ", ")))
	}

	db, currentDB := a.getActiveSessionDatabase(ctx)
	if currentDB != "" {
		sb.WriteString(fmt.Sprintf("Active Database: %s\n", currentDB))
	}

	if len(taskClassification.DBArtifacts) > 0 {
		sb.WriteString("Target Stores:\n")
		storeCtx := a.describeFocusedStores(ctx, db, taskClassification.DBArtifacts)
		if storeCtx != "" {
			sb.WriteString(storeCtx)
		} else {
			for _, artifact := range taskClassification.DBArtifacts {
				sb.WriteString(fmt.Sprintf("- %s\n", artifact))
			}
		}
	}

	if ops := buildStoresCRUDOperationsContext(crudFlags); ops != "" {
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString("Relevant Store Operations:\n")
		sb.WriteString(ops)
	}

	if taskClassification.ScriptAuthoring {
		if sb.Len() > 0 {
			sb.WriteString("\n\n")
		}
		sb.WriteString("Relevant Script Authoring Operations:\n")
		sb.WriteString(buildScriptAuthoringContext(taskClassification.Domain, crudFlags))
	}

	return strings.TrimSpace(sb.String())
}

func (a *CopilotAgent) buildSpacesFocusedExecutionContext(taskClassification TaskContextClassification) string {
	crudFlags := collectCRUDFlags(taskClassification.Layers)
	var sb strings.Builder

	sb.WriteString("Domain: Spaces\n")
	if ordered := orderedCRUDFlags(crudFlags); len(ordered) > 0 {
		sb.WriteString(fmt.Sprintf("CRUD Scope: %s\n", strings.Join(ordered, ", ")))
	}
	if len(taskClassification.DBArtifacts) > 0 {
		sb.WriteString(fmt.Sprintf("Target Spaces: %s\n", strings.Join(taskClassification.DBArtifacts, ", ")))
	}

	if ops := buildSpacesCRUDOperationsContext(crudFlags); ops != "" {
		sb.WriteString("\nRelevant Space Operations:\n")
		sb.WriteString(ops)
	}

	return strings.TrimSpace(sb.String())
}

func (a *CopilotAgent) getActiveSessionDatabase(ctx context.Context) (*database.Database, string) {
	p := ai.GetSessionPayload(ctx)
	if p == nil || p.CurrentDB == "" {
		return nil, ""
	}
	if p.CurrentDB == SystemDBName {
		return a.systemDB, p.CurrentDB
	}
	if dbOpts, ok := a.databases[p.CurrentDB]; ok {
		return database.NewDatabase(dbOpts), p.CurrentDB
	}
	return nil, p.CurrentDB
}

func (a *CopilotAgent) describeFocusedStores(ctx context.Context, db *database.Database, artifacts []string) string {
	if db == nil || len(artifacts) == 0 {
		return ""
	}

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return ""
	}
	defer tx.Rollback(ctx)

	stores, err := tx.GetStores(ctx)
	if err != nil {
		return ""
	}

	available := make(map[string]string, len(stores))
	for _, store := range stores {
		if strings.Contains(store, "/") {
			continue
		}
		available[strings.ToLower(store)] = store
	}

	var sb strings.Builder
	for _, artifact := range artifacts {
		resolved := strings.TrimSpace(artifact)
		if resolved == "" {
			continue
		}
		if exact, ok := available[strings.ToLower(resolved)]; ok {
			resolved = exact
		}

		storeAccessor, err := jsondb.OpenStore(ctx, db.Config(), resolved, tx)
		if err != nil {
			sb.WriteString(fmt.Sprintf("- %s (not found in active database)\n", artifact))
			continue
		}

		info := storeAccessor.GetStoreInfo()
		parts := make([]string, 0, 4)
		if info.Description != "" {
			parts = append(parts, fmt.Sprintf("description=%q", info.Description))
		}
		if ok, _ := storeAccessor.First(ctx); ok {
			key := storeAccessor.GetCurrentKey()
			if key != nil {
				if val, err := storeAccessor.GetCurrentValue(ctx); err == nil {
					flat := flattenItem(key, val)
					parts = append(parts, fmt.Sprintf("schema=%s", formatSchema(inferSchema(flat))))
				}
			}
		}
		if len(info.Relations) > 0 {
			rels := make([]string, 0, len(info.Relations))
			for _, rel := range info.Relations {
				rels = append(rels, fmt.Sprintf("[%s] -> %s([%s])", strings.Join(rel.SourceFields, ", "), rel.TargetStore, strings.Join(rel.TargetFields, ", ")))
			}
			sort.Strings(rels)
			parts = append(parts, fmt.Sprintf("Relations: %s", strings.Join(rels, "; ")))
		}
		if info.MapKeyIndexSpecification != "" {
			parts = append(parts, fmt.Sprintf("Key Schema: %s", info.MapKeyIndexSpecification))
		}

		if len(parts) == 0 {
			sb.WriteString(fmt.Sprintf("- %s\n", resolved))
			continue
		}
		sb.WriteString(fmt.Sprintf("- %s %s\n", resolved, strings.Join(parts, " ")))
	}

	return sb.String()
}

func buildStoresCRUDOperationsContext(flags map[string]bool) string {
	sections := make([]string, 0, len(flags))
	if flags["R"] {
		sections = append(sections,
			"- R = Read. Prefer read-only transactions. MANDATORY Sequence: begin_tx(mode=read) -> open_store -> scan/filter/project/sort/limit -> commit_tx or rollback_tx.",
			"- Note: Calling open_db is OPTIONAL because begin_tx automatically uses the active Current Database by default.",
			"- If you still emit open_db, use the active Current Database name from context instead of inventing one. In AST, either omit open_db or set open_db.args.name to the active database.",
			"- For filter, keep the real predicate shape with operator and value, for example {condition:{first_name:{\"$eq\":\"John\"}}} or {condition:{orders.total_amount:{\"$gt\":500}}}. Do not emit boolean placeholders like {first_name:true}.",
			"- For bridge joins, keep exact dotted field paths in the on map, for example join users to users_orders with {\"users.key\":\"key\"} and then users_orders to orders with {\"users_orders.value\":\"key\"}. Do not flatten dotted field paths into underscore names.",
			"- If a filter or join shape is rejected, preserve the valid store names, field paths, and operators you already have. Only replace the malformed placeholder or join mapping with the corrected AST shape.",
			"- Read AST ops: begin_tx, open_store, find, get_current_value, scan, filter, sort, project, limit, join, join_right, return, commit_tx, rollback_tx.",
		)
	}
	if flags["C"] {
		sections = append(sections,
			"- C = Create. Use write transactions and persist via create/update-oriented scripts after opening the target store.",
			"- Note: Calling open_db is OPTIONAL because begin_tx automatically uses the active Current Database by default.",
			"- If you still emit open_db, use the active Current Database name from context instead of inventing one. In AST, either omit open_db or set open_db.args.name to the active database.",
			"- Create AST ops: begin_tx(mode=write), open_store, add, update, commit_tx, rollback_tx.",
		)
	}
	if flags["U"] {
		sections = append(sections,
			"- U = Update. Use write transactions and pipe the filtered records into update for targeted mutations.",
			"- Note: Calling open_db is OPTIONAL because begin_tx automatically uses the active Current Database by default.",
			"- If you still emit open_db, use the active Current Database name from context instead of inventing one. In AST, either omit open_db or set open_db.args.name to the active database.",
			"- Update AST ops: begin_tx(mode=write), open_store, scan, filter, update, commit_tx, rollback_tx.",
		)
	}
	if flags["D"] {
		sections = append(sections,
			"- D = Delete. Use write transactions and narrow the record set before delete.",
			"- Note: Calling open_db is OPTIONAL because begin_tx automatically uses the active Current Database by default.",
			"- If you still emit open_db, use the active Current Database name from context instead of inventing one. In AST, either omit open_db or set open_db.args.name to the active database.",
			"- Delete AST ops: begin_tx(mode=write), open_store, scan, filter, delete, commit_tx, rollback_tx.",
		)
	}
	if len(sections) == 0 {
		sections = append(sections, "- No CRUD flags were classified. Default to read-first inspection before emitting mutating steps.")
	}
	return strings.Join(sections, "\n")
}

func buildSpacesCRUDOperationsContext(flags map[string]bool) string {
	sections := make([]string, 0, len(flags))
	if flags["R"] {
		sections = append(sections,
			"- R = Read. Use list_space_categories, list_space_items, search_space, and read_space_config for discovery before asking follow-up questions.",
			"- Note: Space reading operations require an enclosing read transaction. MANDATORY Sequence: begin_tx(mode=read) -> [Space Operation] -> commit_tx.",
		)
	}
	if flags["C"] {
		sections = append(sections,
			"- C = Create. Use mint_to_space to add generated content to the target Space.",
			"- Note: When the user asks to generate content and put it into a Space, do not attempt an external import workflow. Generate the content first, then call mint_to_space.",
			"- Note: When the user names a Space, pass that exact name in mint_to_space.kb_name even if the Space does not exist yet.",
			"- Note: mint_to_space manages its own write transaction. Do NOT wrap it in begin_tx/commit_tx.",
			"- Note: Do NOT call vectorize_space_items, vectorize_space_categories, or vectorize_space unless the user explicitly asks for vectorization, embeddings, or semantic search refresh.",
			"- Note: If vectorization is explicitly requested, vectorization APIs manage their own transactions and should be called outside the enclosing write transaction.",
		)
	}
	if flags["U"] {
		sections = append(sections,
			"- U = Update. Use mint_to_space for newly generated knowledge content or update_space_config for routing/persona changes.",
			"- Note: update_space_config requires its normal write path, but mint_to_space manages its own transaction internally.",
			"- Note: Do NOT call vectorization APIs unless the user explicitly asks for vectorization, embeddings, or semantic search refresh.",
			"- Note: If vectorization is explicitly requested, vectorization APIs manage their own transactions and should be called outside the enclosing write transaction.",
		)
	}
	if flags["D"] {
		sections = append(sections,
			"- D = Delete. Use delete_space for full Space deletion when the user explicitly asks to remove the entire Space.",
			"- Note: Full Space deletion is destructive. Prefer a confirmation step with the user before executing it.",
			"- Note: delete_space manages its own deletion path and should not be wrapped in begin_tx/commit_tx.",
		)
	}
	if len(sections) == 0 {
		sections = append(sections, "- No CRUD flags were classified. Default to search_space or list operations before mutating a space. Assume all non-vectorization Operations require a transaction.")
	}
	return strings.Join(sections, "\n")
}

func buildScriptAuthoringContext(domain string, flags map[string]bool) string {
	sections := []string{
		"- Use create_script for a new named reusable script; use save_script only to replace an existing full script definition.",
		"- Provide reusable script steps under the `script` field. Legacy alias `steps` is accepted but should not be preferred.",
		"- For reusable data workflows, prefer a command step whose command is execute_script and whose args.script contains the inner AST.",
		"- In stored execute_script AST, preserve real filter predicates and exact dotted field paths for joins and joined-field filters. Do not turn conditions into boolean placeholders or flatten dotted paths into underscore names.",
		"- When correcting a stored execute_script AST, keep the valid stores, step order, and field names intact. Only rewrite the invalid filter or join shape that caused the validation failure.",
	}
	if strings.EqualFold(domain, StoresDomain) && flags["R"] && !flags["C"] && !flags["U"] && !flags["D"] {
		sections = append(sections, "- This request is read-oriented. Keep the stored execute_script AST read-only unless the user explicitly asks for mutations.")
	}
	return strings.Join(sections, "\n")
}

func isOnlyLayer1(layers []LayerInfo) bool {
	if len(layers) == 0 {
		return false
	}
	for _, layer := range layers {
		if !strings.Contains(layer.Name, "Single-Domain") {
			return false
		}
	}
	return true
}

func isCrossDomain(layers []LayerInfo) bool {
	for _, layer := range layers {
		if strings.Contains(layer.Name, "Cross-Domain") {
			return true
		}
	}
	return false
}

func (a *CopilotAgent) buildFocusedToolContext(taskCtx *TaskContextClassification) string {
	if taskCtx == nil {
		return ""
	}

	crudFlags := collectCRUDFlags(taskCtx.Layers)

	if isCrossDomain(taskCtx.Layers) {
		scriptSection := ""
		if taskCtx.ScriptAuthoring {
			scriptSection = "Structured Context: Script Authoring Tools\n" + toolsScriptsManual + "\n\n"
		}
		return "Structured Context: Cross-Domain Tools (Stores & Spaces)\n" +
			scriptSection + toolsStoresManual + "\n\n" + toolsSpacesManual +
			"\n## Execution Flow Engine Guardrails (Stores)\n" + buildStoresCRUDOperationsContext(crudFlags) +
			"\n## Execution Flow Engine Guardrails (Spaces)\n" + buildSpacesCRUDOperationsContext(crudFlags)
	}

	switch {
	case strings.EqualFold(taskCtx.Domain, StoresDomain):
		manual := "Structured Context: Stores Tools\n" + toolsStoresManual
		if taskCtx.ScriptAuthoring {
			manual = "Structured Context: Script Authoring Tools\n" + toolsScriptsManual + "\n\n" + manual
		}
		return manual + "\n## Execution Flow Engine Guardrails\n" + buildStoresCRUDOperationsContext(crudFlags)
	case strings.EqualFold(taskCtx.Domain, SpacesDomain):
		return "Structured Context: Spaces Tools\n" + toolsSpacesManual + "\n## Execution Flow Engine Guardrails\n" + buildSpacesCRUDOperationsContext(crudFlags)
	default:
		return ""
	}
}
