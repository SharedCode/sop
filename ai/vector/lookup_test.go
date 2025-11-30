package vector

/*
func TestLookupFunctionality(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-lookup-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db := NewDatabase[map[string]any](Standalone)
	db.SetStoragePath(tmpDir)
	idx := db.Open("test_lookup")
	dIdx := idx.(*domainIndex)

	// 1. Test IndexAll Populates Lookup
	// Ingest 10 items via UpsertContent (staged)
	var items []ai.Item
	for i := 0; i < 10; i++ {
		items = append(items, ai.Item{
			ID:     fmt.Sprintf("item-%d", i),
			Vector: []float32{float32(i), float32(i)},
			Meta:   map[string]any{"val": i},
		})
	}

	if err := dIdx.UpsertContent(items); err != nil {
		t.Fatalf("UpsertContent failed: %v", err)
	}

	// Run IndexAll
	if err := dIdx.IndexAll(); err != nil {
		t.Fatalf("IndexAll failed: %v", err)
	}

	// Verify Lookup
	// Should be able to fetch 0..9
	for i := 0; i < 10; i++ {
		item, err := dIdx.GetBySequenceID(i)
		if err != nil {
			t.Errorf("GetBySequenceID(%d) failed: %v", i, err)
			continue
		}
		expectedID := fmt.Sprintf("item-%d", i)
		if item.ID != expectedID {
			t.Errorf("GetBySequenceID(%d) returned ID %s, expected %s", i, item.ID, expectedID)
		}
	}

	// Verify Out of Bounds
	if _, err := dIdx.GetBySequenceID(10); err == nil {
		t.Error("Expected error for GetBySequenceID(10), got nil")
	}

	// 2. Test Incremental IndexAll (Append)
	// Ingest 5 more items
	var newItems []ai.Item
	for i := 10; i < 15; i++ {
		newItems = append(newItems, ai.Item{
			ID:     fmt.Sprintf("item-%d", i),
			Vector: []float32{float32(i), float32(i)},
			Meta:   map[string]any{"val": i},
		})
	}
	if err := dIdx.UpsertContent(newItems); err != nil {
		t.Fatalf("UpsertContent batch 2 failed: %v", err)
	}
	if err := dIdx.IndexAll(); err != nil {
		t.Fatalf("IndexAll batch 2 failed: %v", err)
	}

	// Verify 10..14
	for i := 10; i < 15; i++ {
		item, err := dIdx.GetBySequenceID(i)
		if err != nil {
			t.Errorf("GetBySequenceID(%d) failed: %v", i, err)
		} else if item.ID != fmt.Sprintf("item-%d", i) {
			t.Errorf("GetBySequenceID(%d) returned ID %s", i, item.ID)
		}
	}

	// 3. Test Optimize (Rebuilds Lookup)
	// Delete item-5 (Sequence 5)
	// Note: Delete() removes from Content/Vectors but NOT Lookup (as per design)
	if err := idx.Delete("item-5"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify Lookup 5 still points to item-5, but GetBySequenceID should fail or return error because Content is gone?
	// GetBySequenceID checks Content.Find. If not found, it returns error "item ID ... found in lookup but not in content".
	if _, err := dIdx.GetBySequenceID(5); err == nil {
		t.Error("Expected error fetching deleted item via lookup, got nil")
	}

	// Verify item-6 is still healthy
	if item, err := dIdx.GetBySequenceID(6); err != nil {
		t.Errorf("GetBySequenceID(6) failed after delete item-5: %v", err)
	} else if item.ID != "item-6" {
		t.Errorf("GetBySequenceID(6) returned %s", item.ID)
	}

	// Run Optimize
	// This should rebuild Lookup.
	// The remaining items are 0..4, 6..14 (14 items total).
	// New Lookup should be 0..13.
	if err := dIdx.Optimize(); err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	// Verify Count
	// We don't have a Count() on Lookup exposed, but we can check bounds.
	if _, err := dIdx.GetBySequenceID(13); err != nil {
		t.Errorf("GetBySequenceID(13) failed after optimize: %v", err)
	}
	if _, err := dIdx.GetBySequenceID(14); err == nil {
		t.Error("Expected GetBySequenceID(14) to fail after optimize (should have shrunk)")
	}

	// Verify continuity
	// The items should be re-packed. The order depends on how Optimize iterates Vectors.
	// Vectors iteration order is by Centroid, then Distance, then ID.
	// So the sequence ID mapping will change and follow the physical layout in Vectors store.
	// We just verify that we can fetch *some* valid item for each index.
	for i := 0; i < 14; i++ {
		item, err := dIdx.GetBySequenceID(i)
		if err != nil {
			t.Errorf("GetBySequenceID(%d) failed after optimize: %v", i, err)
		}
		if item == nil {
			t.Errorf("GetBySequenceID(%d) returned nil item", i)
		}
	}
}
*/
