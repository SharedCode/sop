package memory

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/inmemory"
)

func TestImportJSON_NestedCategories(t *testing.T) {
	ctx := context.Background()
	llm := &MockLLM{}
	embedder := &MockEmbedder{}
	cats := inmemory.NewBtree[sop.UUID, *Category](true)
	vecs := inmemory.NewBtree[VectorKey, Vector](true)
	items := inmemory.NewBtree[ItemKey, Item[string]](true)
	st := NewStore[string](cats.Btree, inmemory.NewBtree[string, sop.UUID](false).Btree, vecs.Btree, items.Btree).(*store[string])
	st.SetTextIndex(&MockTextIndex{})
	kb := &KnowledgeBase[string]{
		Store:   st,
		Manager: NewMemoryManager[string](st, llm, embedder),
	}

	jsonPayload := `{"categories":[{"id":"11111111-1111-1111-1111-111111111111","name":"Engineering"},{"id":"22222222-2222-2222-2222-222222222222","name":"Backend","parents":[{"parent_id":"11111111-1111-1111-1111-111111111111"}]}],"items":[{"category":"22222222-2222-2222-2222-222222222222","data":"We use SOP","summaries":["SOP usage"]}]}`

	err := kb.ImportJSON(ctx, bytes.NewReader([]byte(jsonPayload)), "test_persona")
	if err != nil {
		t.Fatalf("ImportJSON failed: %v", err)
	}

	c, _ := st.Count(ctx)
	if c != 1 {
		t.Errorf("Expected 1 item, got %d", c)
	}

	catCount := cats.Count()
	if catCount != 2 {
		t.Errorf("Expected 2 categories, got %d", catCount)
	}

	catBtree, _ := st.Categories(ctx)
	catBtree.First(ctx)
	foundRoot := false
	foundChild := false
	for {
		cat, err := catBtree.GetCurrentValue(ctx)
		if err == nil && cat != nil {
			if cat.Name == "Engineering" {
				foundRoot = true
				if len(cat.ChildrenIDs) != 1 {
					t.Errorf("Expected 1 child for Engineering, got %d", len(cat.ChildrenIDs))
				} else if cat.ChildrenIDs[0].String() != "22222222-2222-2222-2222-222222222222" {
					t.Errorf("Child ID mismatch for Engineering. Got %v", cat.ChildrenIDs[0].String())
				}
			}
			if cat.Name == "Backend" {
				foundChild = true
				if len(cat.ParentIDs) != 1 {
					t.Errorf("Expected 1 parent for Backend, got %d. Cat details: %+v", len(cat.ParentIDs), cat)
				} else if cat.ParentIDs[0].ParentID.String() != "11111111-1111-1111-1111-111111111111" {
					t.Errorf("Parent ID mismatch. Got %v", cat.ParentIDs[0].ParentID.String())
				}
			}
		} else {
			t.Logf("Category: %+v", cat)
		}
		ok, _ := catBtree.Next(ctx)
		if !ok {
			break
		}
	}
	if !foundRoot || !foundChild {
		t.Errorf("Did not find expected categories: root=%v, child=%v", foundRoot, foundChild)
	}
}

func TestImportJSON_RealSOPKnowledgeBase(t *testing.T) {
	ctx := context.Background()
	llm := &MockLLM{}
	embedder := &MockEmbedder{}
	cats := inmemory.NewBtree[sop.UUID, *Category](true)
	vecs := inmemory.NewBtree[VectorKey, Vector](true)
	items := inmemory.NewBtree[ItemKey, Item[map[string]any]](true)
	st := NewStore[map[string]any](cats.Btree, inmemory.NewBtree[string, sop.UUID](false).Btree, vecs.Btree, items.Btree).(*store[map[string]any])
	st.SetTextIndex(&MockTextIndex{})
	kb := &KnowledgeBase[map[string]any]{
		Store:   st,
		Manager: NewMemoryManager[map[string]any](st, llm, embedder),
	}

	payload, err := os.ReadFile("../sop_base_knowledge.json")
	if err != nil {
		t.Skipf("sop_base_knowledge.json not found, skipping test: %v", err)
	}

	err = kb.ImportJSON(ctx, bytes.NewReader(payload), "test_persona")
	if err != nil {
		t.Fatalf("ImportJSON failed on real data: %v", err)
	}

	catCount := cats.Count()
	if catCount == 0 {
		t.Errorf("Expected >0 categories, got %d", catCount)
	}

	itemCount, _ := st.Count(ctx)
	if itemCount == 0 {
		t.Errorf("Expected >0 items, got %d", itemCount)
	}

	hasParents := false
	catBtree, _ := st.Categories(ctx)
	ok, _ := catBtree.First(ctx)
	for ok {
		cat, _ := catBtree.GetCurrentValue(ctx)
		if cat != nil && len(cat.ParentIDs) > 0 {
			hasParents = true
			for _, p := range cat.ParentIDs {
				found, _ := catBtree.Find(ctx, p.ParentID, false)
				if !found {
					t.Errorf("Category %s (%s) references non-existent ParentID: %s", cat.Name, cat.ID.String(), p.ParentID.String())
				} else {
					parent, _ := catBtree.GetCurrentValue(ctx)
					if parent != nil {
						childFound := false
						for _, childID := range parent.ChildrenIDs {
							if childID == cat.ID {
								childFound = true
								break
							}
						}
						if !childFound {
							t.Errorf("Parent %s does not contain child %s in ChildrenIDs array", parent.Name, cat.Name)
						}
					}
				}
				catBtree.Find(ctx, cat.ID, false)
			}
		}
		ok, _ = catBtree.Next(ctx)
	}

	if !hasParents {
		t.Errorf("Expected at least one category to have a parent in the real sop_base_knowledge.json")
	}
}
