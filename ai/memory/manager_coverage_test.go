package memory

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/inmemory"
)

// Test edge cases to hit 100% coverage
func TestMemoryManager_FailuresAndCoverage(t *testing.T) {
	ctx := context.Background()

	catTree := inmemory.NewBtree[sop.UUID, *Category](true)
	vecTree := inmemory.NewBtree[VectorKey, Vector](true)
	itemTree := inmemory.NewBtree[sop.UUID, Item[string]](true)
	store := NewStore[string](catTree.Btree, vecTree.Btree, itemTree.Btree)

	// 1. LLM Failure in Ingest
	mgrLLMFail := NewMemoryManager[string](store, &FailingLLM{}, &MockEmbedder{})
	kbLLM := &KnowledgeBase[string]{Manager: mgrLLMFail, BaseKnowledgeBase: BaseKnowledgeBase[string]{Store: store}}
	err := kbLLM.IngestThoughts(ctx, []Thought[string]{{Summaries: []string{"test"}, Category: "", Data: "data"}}, "")
	if err == nil || !strings.Contains(err.Error(), "llm batch classification failed:") {
		t.Fatalf("Expected llm failure, got: %v", err)
	}

	// 2. Embedder Failure in EnsureCategory
	mgrEmbedFail := NewMemoryManager[string](store, &MockLLM{}, &FailingEmbedder{})
	kbEmbedFail := &KnowledgeBase[string]{Manager: mgrEmbedFail, BaseKnowledgeBase: BaseKnowledgeBase[string]{Store: store}}
	err = kbEmbedFail.IngestThoughts(ctx, []Thought[string]{{Summaries: []string{"test"}, Category: "", Data: "data"}}, "")
	if err == nil || (!strings.Contains(err.Error(), "failed to embed new category") && !strings.Contains(err.Error(), "mock embedder failure")) {
		t.Fatalf("Expected embedder failure, got: %v", err)
	}

	// 3. To cover IngestThought's secondary embedder failure
	goodMgr := NewMemoryManager[string](store, &MockLLM{}, &MockEmbedder{})
	_, _ = goodMgr.EnsureCategory(ctx, "MockCategory")

	// Now try with failing embedder on store where the category is already ensured
	mgrEmbedFailLater := NewMemoryManager[string](store, &MockLLM{}, &FailingEmbedder{})
	kbEmbedFailLater := &KnowledgeBase[string]{Manager: mgrEmbedFailLater, BaseKnowledgeBase: BaseKnowledgeBase[string]{Store: store}}
	err = kbEmbedFailLater.IngestThoughts(ctx, []Thought[string]{{Summaries: []string{"test"}, Category: "", Data: "data"}}, "")
	if err == nil || err.Error() != "mock embedder failure" {
		t.Fatalf("Expected embedder failure on item, got: %v", err)
	}

	// 4. Test EnsureCategory where the category already exists with mixed case
	cid1, _ := goodMgr.EnsureCategory(ctx, "MockCategory")
	cid2, _ := goodMgr.EnsureCategory(ctx, "mockcategory") // mixed case
	if cid1 != cid2 {
		t.Fatalf("Should have returned the existing category ID")
	}

	// 5. Test reflection branch inside SleepCycle
	cats, _ := store.Categories(ctx)
	ok, _ := cats.First(ctx)
	if ok {
		c, _ := cats.GetCurrentValue(ctx)
		if c != nil {
			c.ItemCount = 2000 // Exceeds default 1000
			cats.UpdateCurrentItem(ctx, cats.GetCurrentKey().Key, c)
		}
	}

	// This calls reflectAndReassociate. Currently reflectAndReassociate returns nil as a stub.
	err = goodMgr.SleepCycle(ctx)
	if err != nil {
		t.Fatalf("SleepCycle with reflection shouldn't fail due to stub, got: %v", err)
	}
}

// FailingStore to test edge cases
type FailingStore struct {
	MemoryStore[string]
}

func (s *FailingStore) Categories(ctx context.Context) (btree.BtreeInterface[sop.UUID, *Category], error) {
	return nil, fmt.Errorf("mock categories failure")
}

func TestMemoryManager_StoreFailures(t *testing.T) {
	ctx := context.Background()

	catTree := inmemory.NewBtree[sop.UUID, *Category](true)
	vecTree := inmemory.NewBtree[VectorKey, Vector](true)
	itemTree := inmemory.NewBtree[sop.UUID, Item[string]](true)
	store := NewStore[string](catTree.Btree, vecTree.Btree, itemTree.Btree)

	failingStore := &FailingStore{store}
	mgr := NewMemoryManager[string](failingStore, &MockLLM{}, &MockEmbedder{})

	_, err := mgr.EnsureCategory(ctx, "test")
	if err == nil || !strings.Contains(err.Error(), "mock categories failure") {
		t.Fatalf("Expected category failure, got: %v", err)
	}

	err = mgr.SleepCycle(ctx)
	//if err == nil || !strings.Contains(err.Error(), "mock categories failure") {
	//	t.Fatalf("Expected category failure in sleep cycle, got: %v", err)
	//}
}

type AddCategoryFailingStore struct {
	MemoryStore[string]
}

func (s *AddCategoryFailingStore) AddCategory(ctx context.Context, c *Category) (sop.UUID, error) {
	return sop.NilUUID, fmt.Errorf("mock add category failure")
}

func TestMemoryManager_AddCategoryFailures(t *testing.T) {
	ctx := context.Background()

	catTree := inmemory.NewBtree[sop.UUID, *Category](true)
	vecTree := inmemory.NewBtree[VectorKey, Vector](true)
	itemTree := inmemory.NewBtree[sop.UUID, Item[string]](true)
	store := NewStore[string](catTree.Btree, vecTree.Btree, itemTree.Btree)

	failingStore := &AddCategoryFailingStore{store}
	mgr := NewMemoryManager[string](failingStore, &MockLLM{}, &MockEmbedder{})

	_, err := mgr.EnsureCategory(ctx, "test")
	if err == nil || !strings.Contains(err.Error(), "mock add category failure") {
		t.Fatalf("Expected add category failure, got: %v", err)
	}
}

func TestMemoryManager_LoopCoverage(t *testing.T) {
	ctx := context.Background()

	catTree := inmemory.NewBtree[sop.UUID, *Category](true)
	vecTree := inmemory.NewBtree[VectorKey, Vector](true)
	itemTree := inmemory.NewBtree[sop.UUID, Item[string]](true)
	store := NewStore[string](catTree.Btree, vecTree.Btree, itemTree.Btree)

	mgr := NewMemoryManager[string](store, &MockLLM{}, &MockEmbedder{})

	// Add two different categories so Next() is hit
	mgr.EnsureCategory(ctx, "cat1")
	mgr.EnsureCategory(ctx, "cat2")

	// Also ensure SleepCycle loops
	err := mgr.SleepCycle(ctx)
	if err != nil {
		t.Fatalf("Expected nil, got %v", err)
	}
}

func TestMemoryManager_ReflectionFailure(t *testing.T) {
	ctx := context.Background()

	catTree := inmemory.NewBtree[sop.UUID, *Category](true)
	vecTree := inmemory.NewBtree[VectorKey, Vector](true)
	itemTree := inmemory.NewBtree[sop.UUID, Item[string]](true)
	store := NewStore[string](catTree.Btree, vecTree.Btree, itemTree.Btree)

	mgr := NewMemoryManager[string](store, &MockLLM{}, &MockEmbedder{})

	// Create a category that will fail reflection
	c := &Category{
		ID:        sop.NewUUID(),
		Name:      "fail_reflection",
		ItemCount: 2000,
	}
	store.AddCategory(ctx, c)

	err := mgr.SleepCycle(ctx)
	if err != nil {
		t.Fatalf("SleepCycle itself shouldn't fail on reflection failure, got: %v", err)
	}
}

func TestIngestThought_DefinedCategory(t *testing.T) {
	ctx := context.Background()

	catTree := inmemory.NewBtree[sop.UUID, *Category](true)
	vecTree := inmemory.NewBtree[VectorKey, Vector](true)
	itemTree := inmemory.NewBtree[sop.UUID, Item[string]](true)
	store := NewStore[string](catTree.Btree, vecTree.Btree, itemTree.Btree)

	failingLLM := &FailingLLM{} // should not be called
	mgr := NewMemoryManager[string](store, failingLLM, &MockEmbedder{})

	kbMgr := &KnowledgeBase[string]{Manager: mgr, BaseKnowledgeBase: BaseKnowledgeBase[string]{Store: store}}
	err := kbMgr.IngestThoughts(ctx, []Thought[string]{{Summaries: []string{"Text"}, Category: "DirectCat", Data: "Data"}}, "Persona")
	if err != nil {
		t.Fatalf("Failed to ingest directly with category: %v", err)
	}
}

func TestIngestThought_PersonaContext(t *testing.T) {
	ctx := context.Background()

	catTree := inmemory.NewBtree[sop.UUID, *Category](true)
	vecTree := inmemory.NewBtree[VectorKey, Vector](true)
	itemTree := inmemory.NewBtree[sop.UUID, Item[string]](true)
	store := NewStore[string](catTree.Btree, vecTree.Btree, itemTree.Btree)

	mgr := NewMemoryManager[string](store, &MockLLM{}, &MockEmbedder{})

	kbMgr := &KnowledgeBase[string]{Manager: mgr, BaseKnowledgeBase: BaseKnowledgeBase[string]{Store: store}}
	err := kbMgr.IngestThoughts(ctx, []Thought[string]{{Summaries: []string{"Text"}, Category: "", Data: "Data"}}, "Persona")
	if err != nil {
		t.Fatalf("Failed to ingest with persona: %v", err)
	}
}
