package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/btree"
	sopdb "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/jsondb"
)

// KnowledgeKey is the composite key for the Knowledge Store.
type KnowledgeKey struct {
	Category string
	Name     string
}

// KnowledgeStore manages the AI's long-term knowledge base.
type KnowledgeStore struct {
	store btree.BtreeInterface[KnowledgeKey, string]
}

// OpenKnowledgeStore opens the knowledge store using the provided transaction.
func OpenKnowledgeStore(ctx context.Context, trans sop.Transaction, dbOpts sop.DatabaseOptions) (*KnowledgeStore, error) {
	if trans == nil {
		return nil, fmt.Errorf("transaction is required")
	}

	storeName := "llm_knowledge"

	// Define comparer (Required for composite key)
	comparer := func(a, b KnowledgeKey) int {
		if a.Category < b.Category {
			return -1
		}
		if a.Category > b.Category {
			return 1
		}
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	}

	// Configure Store Info
	so := sop.ConfigureStore(storeName, true, btree.DefaultSlotLength, "AI Knowledge Base", sop.MediumData, "")

	// Define Index Specification
	idxSpec := jsondb.IndexSpecification{
		IndexFields: []jsondb.IndexFieldSpecification{
			{FieldName: "Category", AscendingSortOrder: true},
			{FieldName: "Name", AscendingSortOrder: true},
		},
	}
	if b, err := json.Marshal(idxSpec); err == nil {
		so.MapKeyIndexSpecification = string(b)
	}

	// Open or Create B-Tree using sopdb wrapper which handles replication and backend selection
	store, err := sopdb.NewBtree[KnowledgeKey, string](ctx, dbOpts, storeName, trans, comparer, so)
	if err != nil {
		return nil, fmt.Errorf("failed to open knowledge store: %w", err)
	}

	return &KnowledgeStore{store: store}, nil
}

// Upsert saves a piece of knowledge.
func (ks *KnowledgeStore) Upsert(ctx context.Context, category, name, value string) error {
	key := KnowledgeKey{Category: category, Name: name}
	ok, err := ks.store.Upsert(ctx, key, value)
	if err != nil {
		return err
	}
	if !ok {
		// Fallback to Add if Upsert false
		_, err = ks.store.Add(ctx, key, value)
		return err
	}
	return nil
}

// Get retrieves a piece of knowledge.
func (ks *KnowledgeStore) Get(ctx context.Context, category, name string) (string, bool, error) {
	key := KnowledgeKey{Category: category, Name: name}
	// BtreeInterface uses Find, not FindOne
	found, err := ks.store.Find(ctx, key, false)
	if err != nil {
		return "", false, err
	}
	if !found {
		return "", false, nil
	}
	val, err := ks.store.GetCurrentValue(ctx)
	return val, true, err
}

// Remove deletes a piece of knowledge.
func (ks *KnowledgeStore) Remove(ctx context.Context, category, name string) (bool, error) {
	key := KnowledgeKey{Category: category, Name: name}
	return ks.store.Remove(ctx, key)
}

// ListContent returns all items map[Name]Content for a category.
// Optimized to stop early once outside the category.
func (ks *KnowledgeStore) ListContent(ctx context.Context, category string) (map[string]string, error) {
	result := make(map[string]string)

	if ok, err := ks.store.First(ctx); ok && err == nil {
		for {
			item := ks.store.GetCurrentKey()
			// GetCurrentKey returns an Item[TK, TV], so we access Key field
			k := item.Key

			// Optimization: B-Tree is sorted by Category then Name.
			if k.Category < category {
				// Keep going
			} else if k.Category == category {
				val, _ := ks.store.GetCurrentValue(ctx)
				result[k.Name] = val
			} else {
				// k.Category > category -> We are done
				break
			}

			if ok, _ := ks.store.Next(ctx); !ok {
				break
			}
		}
	}
	return result, nil
}

// ListCategories returns a list of all unique categories in the knowledge base.
func (ks *KnowledgeStore) ListCategories(ctx context.Context) ([]string, error) {
	categories := make(map[string]bool)

	if ok, err := ks.store.First(ctx); ok && err == nil {
		for {
			item := ks.store.GetCurrentKey()
			k := item.Key
			categories[k.Category] = true
			if ok, _ := ks.store.Next(ctx); !ok {
				break
			}
		}
	}
	var list []string
	for c := range categories {
		list = append(list, c)
	}
	sort.Strings(list)
	return list, nil
}

// DefaultKnowledge contains the built-in documentation for tools and system behaviors.
var DefaultKnowledge = map[string]string{
	"execute_script": ExecuteScriptInstruction,
}

// RetrieveLLMKnowledge fetches relevant knowledge for the current context.
func RetrieveLLMKnowledge(ctx context.Context, systemDB *database.Database) string {
	if systemDB == nil {
		return ""
	}
	// Use a short-lived read transaction
	tx, err := systemDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return ""
	}
	defer tx.Rollback(ctx)

	var sb strings.Builder

	// Open Knowledge Store
	ks, err := OpenKnowledgeStore(ctx, tx, systemDB.Options())
	if err != nil {
		return "" // Fail silently for prompt building
	}

	// SCALABILITY FIX: Instead of loading everything, we only load "Core" namespaces.
	// We load 'memory' (General instructions), 'term' (Business glossary), and 'schema' (Structural corrections).
	namespacesToLoad := []string{"memory", "term", "schema"}

	// Add current DB domain if applicable
	if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" {
		namespacesToLoad = append(namespacesToLoad, p.CurrentDB)
	}

	seen := make(map[string]bool)
	for _, ns := range namespacesToLoad {
		if seen[ns] {
			continue
		}
		seen[ns] = true

		content, err := ks.ListContent(ctx, ns)
		if err == nil && len(content) > 0 {
			// Sort keys for deterministic output
			keys := make([]string, 0, len(content))
			for k := range content {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				sb.WriteString(fmt.Sprintf("%s: %s\n", k, content[k]))
			}
		}
	}

	// Inform about other available categories (so LLM knows it can peek)
	if allCats, err := ks.ListCategories(ctx); err == nil {
		var available []string
		for _, c := range allCats {
			if !seen[c] {
				available = append(available, c)
			}
		}
		if len(available) > 0 {
			sb.WriteString(fmt.Sprintf("\n[System] Additional Knowledge Categories Available: %s\n(Use 'manage_knowledge' with action='list'/'read' to access them if relevant to your task.)\n", strings.Join(available, ", ")))
		}
	}

	return sb.String()
}
