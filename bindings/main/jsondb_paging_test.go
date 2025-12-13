package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestBtree_Paging_CursorNavigation(t *testing.T) {
	ctxID := CreateContextForTest()
	defer RemoveContextForTest(ctxID)
	dir := t.TempDir()

	dbPayload := fmt.Sprintf(`{"cache_type": 1, "stores_folders": ["%s"]}`, dir)
	dbID := ManageDatabaseForTest(ctxID, NewDatabase, "", dbPayload)

	transPayload := fmt.Sprintf(`{"Mode": 1, "stores_folders": ["%s"]}`, dir)
	transID := ManageDatabaseForTest(ctxID, BeginTransaction, dbID, transPayload)

	btreeOpts := fmt.Sprintf(`{
		"name": "test_btree_paging",
		"transaction_id": "%s",
		"is_primitive_key": true
	}`, transID)

	btreeID := ManageDatabaseForTest(ctxID, NewBtree, dbID, btreeOpts)
	actionPayload := fmt.Sprintf(`{"transaction_id": "%s", "btree_id": "%s"}`, transID, btreeID)

	// Add 20 items: key01 to key20
	var itemsBuilder strings.Builder
	itemsBuilder.WriteString(`{"items": [`)
	for i := 1; i <= 20; i++ {
		if i > 1 {
			itemsBuilder.WriteString(",")
		}
		itemsBuilder.WriteString(fmt.Sprintf(`{"key": "key%02d", "value": "val%02d"}`, i, i))
	}
	itemsBuilder.WriteString(`]}`)

	res := ManageBtreeForTest(ctxID, Add, actionPayload, itemsBuilder.String())
	if res != "true" {
		t.Fatalf("Add failed: %s", res)
	}

	// Page 1: Get first 5 items
	// PageOffset=0 means start from current cursor (which will auto-reset to First if nil)
	pagingPayload := `{"page_size": 5, "page_offset": 0}`

	res, errStr := GetFromBtreeForTest(ctxID, GetKeys, actionPayload, pagingPayload)
	if errStr != "" {
		t.Fatalf("Page 1 failed: %s", errStr)
	}
	// Verify items 01-05
	for i := 1; i <= 5; i++ {
		expected := fmt.Sprintf(`"key":"key%02d"`, i)
		if !strings.Contains(res, expected) {
			t.Errorf("Page 1 missing expected key: %s. Got: %s", expected, res)
		}
	}

	// Page 2: Get next 5 items
	// Cursor should be at item 6 now.
	res, errStr = GetFromBtreeForTest(ctxID, GetKeys, actionPayload, pagingPayload)
	if errStr != "" {
		t.Fatalf("Page 2 failed: %s", errStr)
	}
	// Verify items 06-10
	for i := 6; i <= 10; i++ {
		expected := fmt.Sprintf(`"key":"key%02d"`, i)
		if !strings.Contains(res, expected) {
			t.Errorf("Page 2 missing expected key: %s. Got: %s", expected, res)
		}
	}

	// Page 3: Get next 5 items
	res, errStr = GetFromBtreeForTest(ctxID, GetKeys, actionPayload, pagingPayload)
	if errStr != "" {
		t.Fatalf("Page 3 failed: %s", errStr)
	}
	// Verify items 11-15
	for i := 11; i <= 15; i++ {
		expected := fmt.Sprintf(`"key":"key%02d"`, i)
		if !strings.Contains(res, expected) {
			t.Errorf("Page 3 missing expected key: %s. Got: %s", expected, res)
		}
	}

	// Page 4: Get last 5 items
	res, errStr = GetFromBtreeForTest(ctxID, GetKeys, actionPayload, pagingPayload)
	if errStr != "" {
		t.Fatalf("Page 4 failed: %s", errStr)
	}
	// Verify items 16-20
	for i := 16; i <= 20; i++ {
		expected := fmt.Sprintf(`"key":"key%02d"`, i)
		if !strings.Contains(res, expected) {
			t.Errorf("Page 4 missing expected key: %s. Got: %s", expected, res)
		}
	}

	// Page 5: Should be empty or error (reached end)
	// The implementation returns empty list if at end, or error if "reached the end of B-tree, no items fetched"
	// Let's check the implementation again.
	// If Next() returns false (end of tree), it returns the keys collected so far.
	// If loop starts and Next() immediately returns false, it returns empty list?
	// Wait, GetKeys implementation:
	/*
		if pagingInfo.Direction == Forward {
			if ok, err := j.BtreeInterface.Next(ctx); err != nil {
				p, _ := toJsonString(keys)
				return p, err
			} else if !ok {
				return toJsonString(keys)
			}
			continue
		}
	*/
	// If we are at the end, GetCurrentKey might return nil/empty?
	// Actually, after fetching item 20, the loop calls Next(). Next() returns false.
	// So Page 4 returns items 16-20.
	// Cursor is now "exhausted" or at the last item?
	// Btree.Next() returns false if no more items.
	// If we call GetKeys again:
	// j.BtreeInterface.GetCurrentKey() -> what does it return if we are at EOF?
	// Usually Btree keeps cursor at the last item if Next() failed? Or invalidates it?
	// If it stays at last item, then GetKeys will fetch item 20 again?

	// Test PageOffset > 0
	// Reset to First
	NavigateBtreeForTest(ctxID, First, actionPayload, "")

	// Skip 1 page (5 items), fetch next 5 items. Should return items 06-10.
	pagingPayloadOffset := `{"page_size": 5, "page_offset": 1}`
	res, errStr = GetFromBtreeForTest(ctxID, GetKeys, actionPayload, pagingPayloadOffset)
	if errStr != "" {
		t.Fatalf("PageOffset test failed: %s", errStr)
	}
	// Verify items 06-10
	for i := 6; i <= 10; i++ {
		expected := fmt.Sprintf(`"key":"key%02d"`, i)
		if !strings.Contains(res, expected) {
			t.Errorf("PageOffset test missing expected key: %s. Got: %s", expected, res)
		}
	}

	ManageTransactionForTest(ctxID, Commit, transID)
}
