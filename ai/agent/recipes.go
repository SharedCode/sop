package agent

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sharedcode/sop/ai"
)

const (
	RecipeKindExplicit = "explicit"
	RecipeKindImplicit = "implicit"
)

const (
	RecipeScopeMicro  = "micro"
	RecipeScopeMacro  = "macro"
	RecipeScopeSpace  = "space"
	RecipeScopeGlobal = "global"
)

const maxRecipeSnapshotEntries = 8

// RecipeItem captures a reusable protocol that can be selected independently of facts.
// Facts tell the model what is true; recipes tell it how to proceed.
type RecipeItem struct {
	ID          string
	Kind        string
	Scope       string
	Domain      string
	Topic       string
	Trigger     string
	Protocol    []string
	Invariants  []string
	AntiPattern []string
	Tags        []string
	Confidence  float64
	Source      string
}

func (a *CopilotAgent) getRecipeContext(taskClassification TaskContextClassification) string {
	items := buildExplicitRecipes(taskClassification)
	items = append(items, a.getImplicitRecipes(taskClassification)...)
	return formatRecipeContext(items)
}

func (a *CopilotAgent) getImplicitRecipes(taskClassification TaskContextClassification) []RecipeItem {
	if a == nil || a.service == nil || a.service.session == nil || a.service.session.Memory == nil {
		return nil
	}
	snapshot := a.service.session.Memory.GetRecipeSnapshot()
	if len(snapshot) == 0 {
		return nil
	}
	items := make([]RecipeItem, 0, len(snapshot))
	for _, item := range snapshot {
		if recipeMatchesTask(item, taskClassification) {
			items = append(items, item)
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Confidence != items[j].Confidence {
			return items[i].Confidence > items[j].Confidence
		}
		return items[i].ID < items[j].ID
	})
	return items
}

func recipeMatchesTask(item RecipeItem, taskClassification TaskContextClassification) bool {
	domain := strings.TrimSpace(strings.ToLower(item.Domain))
	switch domain {
	case "", strings.ToLower(taskClassification.Domain):
		return true
	case "cross-domain":
		return isCrossDomain(taskClassification.Layers)
	case strings.ToLower(StoresDomain):
		return hasStoresRecipeNeed(taskClassification)
	case strings.ToLower(SpacesDomain):
		return hasSpacesRecipeNeed(taskClassification)
	default:
		return false
	}
}

func recipeItemsFromLearned(recipes []ai.LearnedRecipe) []RecipeItem {
	if len(recipes) == 0 {
		return nil
	}
	items := make([]RecipeItem, 0, len(recipes))
	seen := make(map[string]bool, len(recipes))
	for _, recipe := range recipes {
		id := strings.TrimSpace(recipe.ID)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		items = append(items, RecipeItem{
			ID:         id,
			Kind:       recipe.Kind,
			Scope:      recipe.Scope,
			Domain:     recipe.Domain,
			Topic:      recipe.Topic,
			Trigger:    recipe.Trigger,
			Protocol:   append([]string(nil), recipe.Protocol...),
			Invariants: append([]string(nil), recipe.Invariants...),
			Confidence: recipe.Confidence,
			Source:     recipe.Source,
		})
	}
	return items
}

func mergeRecipeSnapshots(existing []RecipeItem, incoming []RecipeItem) []RecipeItem {
	if len(existing) == 0 && len(incoming) == 0 {
		return nil
	}

	merged := make(map[string]RecipeItem, len(existing)+len(incoming))
	for _, item := range existing {
		id := strings.TrimSpace(item.ID)
		if id == "" {
			continue
		}
		merged[id] = cloneRecipeItem(item)
	}
	for _, item := range incoming {
		id := strings.TrimSpace(item.ID)
		if id == "" {
			continue
		}
		if current, ok := merged[id]; ok {
			merged[id] = mergeRecipeItem(current, item)
			continue
		}
		merged[id] = cloneRecipeItem(item)
	}

	items := make([]RecipeItem, 0, len(merged))
	for _, item := range merged {
		items = append(items, item)
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Confidence != items[j].Confidence {
			return items[i].Confidence > items[j].Confidence
		}
		return items[i].ID < items[j].ID
	})
	if len(items) > maxRecipeSnapshotEntries {
		items = items[:maxRecipeSnapshotEntries]
	}
	return items
}

func mergeRecipeItem(existing RecipeItem, incoming RecipeItem) RecipeItem {
	merged := cloneRecipeItem(existing)
	if strings.TrimSpace(incoming.Kind) != "" {
		merged.Kind = incoming.Kind
	}
	if strings.TrimSpace(incoming.Scope) != "" {
		merged.Scope = incoming.Scope
	}
	if strings.TrimSpace(incoming.Domain) != "" {
		merged.Domain = incoming.Domain
	}
	if strings.TrimSpace(incoming.Topic) != "" {
		merged.Topic = incoming.Topic
	}
	if strings.TrimSpace(incoming.Trigger) != "" {
		merged.Trigger = incoming.Trigger
	}
	merged.Protocol = mergeRecipeStrings(merged.Protocol, incoming.Protocol)
	merged.Invariants = mergeRecipeStrings(merged.Invariants, incoming.Invariants)
	merged.AntiPattern = mergeRecipeStrings(merged.AntiPattern, incoming.AntiPattern)
	merged.Tags = mergeRecipeStrings(merged.Tags, incoming.Tags)
	if incoming.Confidence > merged.Confidence {
		merged.Confidence = incoming.Confidence
	}
	if strings.TrimSpace(incoming.Source) != "" {
		merged.Source = incoming.Source
	}
	return merged
}

func cloneRecipeItem(item RecipeItem) RecipeItem {
	item.Protocol = append([]string(nil), item.Protocol...)
	item.Invariants = append([]string(nil), item.Invariants...)
	item.AntiPattern = append([]string(nil), item.AntiPattern...)
	item.Tags = append([]string(nil), item.Tags...)
	return item
}

func mergeRecipeStrings(existing []string, incoming []string) []string {
	if len(existing) == 0 && len(incoming) == 0 {
		return nil
	}
	merged := make([]string, 0, len(existing)+len(incoming))
	seen := make(map[string]bool, len(existing)+len(incoming))
	for _, value := range append(append([]string(nil), existing...), incoming...) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		merged = append(merged, trimmed)
	}
	return merged
}

func buildExplicitRecipes(taskClassification TaskContextClassification) []RecipeItem {
	items := make([]RecipeItem, 0, 7)

	if taskClassification.ScriptAuthoring {
		items = append(items, RecipeItem{
			ID:         "explicit.script_authoring.reusable_workflow",
			Kind:       RecipeKindExplicit,
			Scope:      RecipeScopeMicro,
			Domain:     StoresDomain,
			Topic:      "Reusable script authoring",
			Trigger:    "The user wants a named reusable script instead of only running a one-off query.",
			Protocol:   []string{"choose create_script for a new named script or save_script for a full overwrite", "store reusable steps under the script field", "prefer a single execute_script command step for reusable database workflows"},
			Invariants: []string{"keep stored execute_script ASTs reusable and self-contained", "keep read-oriented stored ASTs read-only unless the user explicitly asks for mutations"},
			Tags:       []string{"script", "authoring", "reusable"},
			Confidence: 1.0,
			Source:     "tools_scripts.md",
		})
	}

	if hasStoresRecipeNeed(taskClassification) {
		items = append(items, RecipeItem{
			ID:         "explicit.stores.research_schema_first",
			Kind:       RecipeKindExplicit,
			Scope:      RecipeScopeMicro,
			Domain:     StoresDomain,
			Topic:      "Stores schema-first research",
			Trigger:    "The ask needs store reads, filters, joins, or repair around schema, field, or relation ambiguity.",
			Protocol:   []string{"research with list_stores first when schema, field paths, value types, or join mappings are ambiguous", "scope list_stores with stores:[...] when likely target stores are already known", "treat list_stores relations as the source of truth for related stores and join key mapping details", "use gettoolinfo('execute_script') only when the AST shape itself is unclear"},
			Invariants: []string{"preserve confirmed schema, relation, and MRU facts during repair", "do not guess missing join mappings when list_stores can ground them"},
			Tags:       []string{"stores", "research", "schema", "research_first"},
			Confidence: 1.0,
			Source:     "tools_stores.md",
		})
		items = append(items, RecipeItem{
			ID:         "explicit.stores.read_transaction_flow",
			Kind:       RecipeKindExplicit,
			Scope:      RecipeScopeMicro,
			Domain:     StoresDomain,
			Topic:      "Stores read transaction flow",
			Trigger:    "The ask is ready to execute a grounded read flow with execute_script.",
			Protocol:   []string{"wrap store reads in begin_tx(mode=read) before open_store", "execute scan/find/filter/project/sort/limit/join inside the same read transaction", "commit_tx or rollback_tx before returning the final shaped result"},
			Invariants: []string{"keep execute_script focused on orchestration rather than conversational explanation", "replace only the malformed filter or join slice instead of rewriting the whole plan"},
			Tags:       []string{"stores", "read", "transaction", "execute_script"},
			Confidence: 1.0,
			Source:     "tools_stores.md",
		})
		items = append(items, RecipeItem{
			ID:         "explicit.stores.join_slice_repair",
			Kind:       RecipeKindExplicit,
			Scope:      RecipeScopeMicro,
			Domain:     StoresDomain,
			Topic:      "Stores join slice repair",
			Trigger:    "A store join is partially correct but the join mapping or related-store slice is malformed.",
			Protocol:   []string{"reuse researched relations before rewriting a join", "replace only the malformed join slice instead of restarting the whole AST", "keep valid stores, step order, and field paths intact while repairing the join"},
			Invariants: []string{"do not flatten dotted join field paths into invented names", "do not discard already confirmed relation mappings"},
			Tags:       []string{"stores", "join", "repair", "execute_script"},
			Confidence: 1.0,
			Source:     "tools_stores.md",
		})
		items = append(items, RecipeItem{
			ID:         "explicit.stores.predicate_grounding",
			Kind:       RecipeKindExplicit,
			Scope:      RecipeScopeMicro,
			Domain:     StoresDomain,
			Topic:      "Stores predicate grounding",
			Trigger:    "A store filter or predicate is being authored or repaired for execute_script.",
			Protocol:   []string{"use concrete predicate objects for filters", "preserve exact dotted field paths for joined-field predicates", "ground predicate fields and operators in researched schema before retrying"},
			Invariants: []string{"do not use boolean placeholders or nulls as predicate stand-ins", "do not broaden the filter scope while repairing one malformed condition"},
			Tags:       []string{"stores", "filter", "predicate", "grounding"},
			Confidence: 1.0,
			Source:     "tools_stores.md",
		})
	}

	if hasSpacesRecipeNeed(taskClassification) {
		items = append(items, RecipeItem{
			ID:         "explicit.spaces.read_discovery_and_mutation",
			Kind:       RecipeKindExplicit,
			Scope:      RecipeScopeMicro,
			Domain:     SpacesDomain,
			Topic:      "Spaces discovery and mutation",
			Trigger:    "The ask needs Space discovery, generated knowledge storage, configuration lookup, or vectorization decisions.",
			Protocol:   []string{"use begin_tx(mode=read) around list_space_categories, list_space_items, search_space, and read_space_config", "use mint_to_space for generated content and pass the exact kb_name the user asked for", "only call vectorization tools when the user explicitly asks for embeddings, reindexing, or semantic refresh"},
			Invariants: []string{"mint_to_space manages its own transaction", "delete_space runs directly and should not be wrapped in begin_tx or commit_tx"},
			Tags:       []string{"spaces", "read", "write", "vectorization"},
			Confidence: 1.0,
			Source:     "tools_spaces.md",
		})
	}

	if isCrossDomain(taskClassification.Layers) {
		items = append(items, RecipeItem{
			ID:         "explicit.cross_domain.research_then_narrow",
			Kind:       RecipeKindExplicit,
			Scope:      RecipeScopeMacro,
			Domain:     "cross-domain",
			Topic:      "Cross-domain narrowing",
			Trigger:    "The ask spans Stores and Spaces or needs evidence from more than one domain before execution.",
			Protocol:   []string{"use the first tool call to narrow ambiguity in the domain that holds the missing evidence", "carry the narrowed result into the second domain instead of replaying broad manuals", "keep each domain on its native protocol once the cross-domain handoff is resolved"},
			Invariants: []string{"do not duplicate execution guardrails across domains", "reuse confirmed outcomes and successful patterns before broadening scope"},
			Tags:       []string{"cross-domain", "handoff", "narrowing"},
			Confidence: 1.0,
			Source:     "implementation",
		})
	}

	return items
}

func hasStoresRecipeNeed(taskClassification TaskContextClassification) bool {
	if strings.EqualFold(taskClassification.Domain, StoresDomain) {
		return true
	}
	return len(taskClassification.DBArtifacts) > 0 || len(taskClassification.StoresArtifacts) > 0
}

func hasSpacesRecipeNeed(taskClassification TaskContextClassification) bool {
	if strings.EqualFold(taskClassification.Domain, SpacesDomain) {
		return true
	}
	return len(taskClassification.SpacesArtifacts) > 0
}

func formatRecipeContext(items []RecipeItem) string {
	if len(items) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("Workflow Recipes:\n")
	for _, item := range items {
		sb.WriteString(fmt.Sprintf("- Recipe: %s [%s/%s]\n", item.Topic, item.Kind, item.Scope))
		if item.Trigger != "" {
			sb.WriteString(fmt.Sprintf("  Trigger: %s\n", item.Trigger))
		}
		if len(item.Protocol) > 0 {
			sb.WriteString(fmt.Sprintf("  Protocol: %s\n", strings.Join(item.Protocol, " -> ")))
		}
		if len(item.Invariants) > 0 {
			sb.WriteString(fmt.Sprintf("  Invariants: %s\n", strings.Join(item.Invariants, "; ")))
		}
	}
	return strings.TrimSpace(sb.String())
}
