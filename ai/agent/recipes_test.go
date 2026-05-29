package agent

import (
	"strings"
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestRecipeMatchesTask(t *testing.T) {
	tests := []struct {
		name  string
		item  RecipeItem
		task  TaskContextClassification
		match bool
	}{
		{
			name:  "empty domain matches any task",
			item:  RecipeItem{Domain: ""},
			task:  TaskContextClassification{Domain: StoresDomain},
			match: true,
		},
		{
			name:  "same domain matches case insensitively",
			item:  RecipeItem{Domain: "stores"},
			task:  TaskContextClassification{Domain: StoresDomain},
			match: true,
		},
		{
			name:  "cross domain recipe requires cross domain task",
			item:  RecipeItem{Domain: "cross-domain"},
			task:  TaskContextClassification{Layers: []LayerInfo{{Name: "Cross-Domain: Stores -> Spaces"}}},
			match: true,
		},
		{
			name:  "stores recipe matches artifacts even without explicit domain",
			item:  RecipeItem{Domain: StoresDomain},
			task:  TaskContextClassification{DBArtifacts: []string{"main"}},
			match: true,
		},
		{
			name:  "spaces recipe matches spaces artifacts",
			item:  RecipeItem{Domain: SpacesDomain},
			task:  TaskContextClassification{SpacesArtifacts: []string{"kb_users"}},
			match: true,
		},
		{
			name:  "unknown domain does not match",
			item:  RecipeItem{Domain: "Other"},
			task:  TaskContextClassification{Domain: StoresDomain},
			match: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := recipeMatchesTask(tt.item, tt.task); got != tt.match {
				t.Fatalf("recipeMatchesTask() = %v, want %v", got, tt.match)
			}
		})
	}
}

func TestRecipeItemsFromLearned_DedupesAndClones(t *testing.T) {
	learned := []ai.LearnedRecipe{
		{ID: "r1", Kind: RecipeKindImplicit, Scope: RecipeScopeMicro, Domain: StoresDomain, Topic: "one", Protocol: []string{"a"}, Invariants: []string{"b"}, Confidence: 0.7, Source: "x"},
		{ID: "r1", Kind: RecipeKindImplicit, Scope: RecipeScopeMicro, Domain: StoresDomain, Topic: "duplicate should skip"},
		{ID: "r2", Kind: RecipeKindImplicit, Scope: RecipeScopeMacro, Domain: SpacesDomain, Topic: "two", Protocol: []string{"c"}, Invariants: []string{"d"}, Confidence: 0.9, Source: "y"},
	}

	items := recipeItemsFromLearned(learned)
	if len(items) != 2 {
		t.Fatalf("expected 2 deduped recipe items, got %+v", items)
	}

	learned[0].Protocol[0] = "mutated"
	if items[0].Protocol[0] != "a" {
		t.Fatalf("expected recipeItemsFromLearned to clone slices, got %+v", items[0].Protocol)
	}
}

func TestMergeRecipeItem_MergesFieldsAndDedupesSlices(t *testing.T) {
	existing := RecipeItem{
		ID:          "r1",
		Kind:        RecipeKindImplicit,
		Scope:       RecipeScopeMicro,
		Domain:      StoresDomain,
		Topic:       "Original",
		Trigger:     "old trigger",
		Protocol:    []string{"step one", "step two"},
		Invariants:  []string{"keep scope"},
		AntiPattern: []string{"do not reset"},
		Tags:        []string{"stores"},
		Confidence:  0.4,
		Source:      "old",
	}
	incoming := RecipeItem{
		ID:          "r1",
		Topic:       "Updated",
		Trigger:     "new trigger",
		Protocol:    []string{"step two", "step three"},
		Invariants:  []string{"keep scope", "preserve facts"},
		AntiPattern: []string{"do not reset", "do not broaden"},
		Tags:        []string{"stores", "repair"},
		Confidence:  0.9,
		Source:      "new",
	}

	merged := mergeRecipeItem(existing, incoming)
	if merged.Topic != "Updated" || merged.Trigger != "new trigger" || merged.Source != "new" {
		t.Fatalf("expected incoming scalar fields to win, got %+v", merged)
	}
	if merged.Confidence != 0.9 {
		t.Fatalf("expected confidence to keep the higher value, got %v", merged.Confidence)
	}
	if strings.Join(merged.Protocol, "|") != "step one|step two|step three" {
		t.Fatalf("expected merged protocol with stable dedupe, got %+v", merged.Protocol)
	}
	if strings.Join(merged.Tags, "|") != "stores|repair" {
		t.Fatalf("expected merged tags with dedupe, got %+v", merged.Tags)
	}
}

func TestMergeRecipeSnapshots_SortsCapsAndMergesByID(t *testing.T) {
	existing := []RecipeItem{{ID: "shared", Confidence: 0.2, Protocol: []string{"old"}}, {ID: "a", Confidence: 0.1}}
	incoming := []RecipeItem{
		{ID: "shared", Confidence: 0.8, Protocol: []string{"new"}},
		{ID: "b", Confidence: 0.7},
		{ID: "c", Confidence: 0.6},
		{ID: "d", Confidence: 0.5},
		{ID: "e", Confidence: 0.4},
		{ID: "f", Confidence: 0.3},
		{ID: "g", Confidence: 0.25},
		{ID: "h", Confidence: 0.24},
		{ID: "i", Confidence: 0.23},
	}

	merged := mergeRecipeSnapshots(existing, incoming)
	if len(merged) != maxRecipeSnapshotEntries {
		t.Fatalf("expected merged snapshot to be capped at %d, got %d", maxRecipeSnapshotEntries, len(merged))
	}
	if merged[0].ID != "shared" || merged[0].Confidence != 0.8 {
		t.Fatalf("expected highest-confidence merged recipe first, got %+v", merged)
	}
	if strings.Join(merged[0].Protocol, "|") != "old|new" {
		t.Fatalf("expected shared recipe to merge protocol slices, got %+v", merged[0])
	}
	for _, item := range merged {
		if item.ID == "a" {
			t.Fatalf("expected lowest-confidence item to be trimmed by cap, got %+v", merged)
		}
	}
}

func TestGetImplicitRecipes_FiltersAndSortsSnapshot(t *testing.T) {
	ag := NewCopilotAgent(Config{}, nil, nil)
	ag.service = &Service{session: NewRunnerSession()}
	ag.service.session.Memory.SetRecipeSnapshot([]RecipeItem{
		{ID: "spaces", Domain: SpacesDomain, Confidence: 0.4, Topic: "spaces"},
		{ID: "stores-low", Domain: StoresDomain, Confidence: 0.4, Topic: "stores low"},
		{ID: "stores-high", Domain: StoresDomain, Confidence: 0.9, Topic: "stores high"},
	})

	items := ag.getImplicitRecipes(TaskContextClassification{Domain: StoresDomain})
	if len(items) != 2 {
		t.Fatalf("expected only store recipes, got %+v", items)
	}
	if items[0].ID != "stores-high" || items[1].ID != "stores-low" {
		t.Fatalf("expected store recipes sorted by confidence descending, got %+v", items)
	}
}

func TestBuildExplicitRecipesAndFormatRecipeContext(t *testing.T) {
	items := buildExplicitRecipes(TaskContextClassification{
		Domain:          StoresDomain,
		ScriptAuthoring: true,
		Layers:          []LayerInfo{{Name: "Cross-Domain: Stores -> Spaces"}},
		SpacesArtifacts: []string{"kb"},
	})
	if len(items) != 7 {
		t.Fatalf("expected all explicit recipe families to be present, got %+v", items)
	}

	formatted := formatRecipeContext(items)
	if !strings.Contains(formatted, "Reusable script authoring") || !strings.Contains(formatted, "Cross-domain narrowing") || !strings.Contains(formatted, "Stores schema-first research") || !strings.Contains(formatted, "Stores read transaction flow") || !strings.Contains(formatted, "Stores join slice repair") || !strings.Contains(formatted, "Stores predicate grounding") {
		t.Fatalf("expected formatted recipe context to include recipe topics, got %s", formatted)
	}
	if got := formatRecipeContext(nil); got != "" {
		t.Fatalf("expected empty recipe context for nil input, got %q", got)
	}
}
