package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListSpaceCategoryAndItems(t *testing.T) {
	// Setup temp dir for DB
	tmpDir := t.TempDir()
	dbName := "testdb_cats"
	spaceName := "test_space"

	// Setup global config
	config = Config{
		RootPassword: "secret_password",
		Databases: []DatabaseConfig{
			{
				Name: dbName,
				Path: tmpDir,
				Mode: "standalone",
			},
		},
	}

	// Helper to send HTTP requests to handlers
	sendJSON := func(method, url string, handler http.HandlerFunc, body interface{}) *httptest.ResponseRecorder {
		b, _ := json.Marshal(body)
		req := httptest.NewRequest(method, url, bytes.NewBuffer(b))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler(rr, req)
		return rr
	}

	// 1. Create Space
	rr := sendJSON("POST", "/api/spaces/create", handleCreateSpace, CreateSpaceRequest{
		DatabaseName: dbName,
		SpaceName:    spaceName,
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("Failed to create space: %v", rr.Body.String())
	}

	// 2. Add Root A Category
	rr = sendJSON("POST", "/api/spaces/category/add?database="+dbName+"&name="+spaceName, handleAddSpaceCategory, AddSpaceCategoryRequest{
		Name: "Root A",
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("Failed to add Root A")
	}
	var res map[string]interface{}
	json.NewDecoder(rr.Body).Decode(&res)
	rootAID := res["id"].(string)

	// 3. Add Root B Category
	rr = sendJSON("POST", "/api/spaces/category/add?database="+dbName+"&name="+spaceName, handleAddSpaceCategory, AddSpaceCategoryRequest{
		Name: "Root B",
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("Failed to add Root B")
	}
	rrBody := rr.Body.String()
	res = make(map[string]interface{})
	json.Unmarshal([]byte(rrBody), &res)
	rootBID := res["id"].(string)

	// 4. Add Child A1 Category
	rr = sendJSON("POST", "/api/spaces/category/add?database="+dbName+"&name="+spaceName, handleAddSpaceCategory, AddSpaceCategoryRequest{
		Name:     "Child A1",
		ParentID: rootAID,
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("Failed to add Child A1: %v", rr.Body.String())
	}
	res = make(map[string]interface{})
	json.NewDecoder(rr.Body).Decode(&res)
	childA1ID := res["id"].(string)

	// 5. Add Item 1 to Child A1
	rr = sendJSON("POST", "/api/spaces/item/add?database="+dbName+"&name="+spaceName, handleAddSpaceItem, AddSpaceItemRequest{
		CategoryID: childA1ID,
		Summaries:  []string{"Summary 1"},
		Data:       map[string]interface{}{"text": "Item 1 Text"},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("Failed to add Item 1: %v", rr.Body.String())
	}

	// 6. Add Item 2 to Root B
	rr = sendJSON("POST", "/api/spaces/item/add?database="+dbName+"&name="+spaceName, handleAddSpaceItem, AddSpaceItemRequest{
		CategoryID: rootBID,
		Summaries:  []string{"Summary 2"},
		Data:       map[string]interface{}{"text": "Item 2 Text"},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("Failed to add Item 2: %v", rr.Body.String())
	}

	// --- Phase 2: Queries --- //

	// Test 1: HTTP List Root Categories (parent_id is empty)
	req, _ := http.NewRequest("GET", "/api/spaces/categories?database="+dbName+"&name="+spaceName+"&parent_id=", nil)
	rrGet := httptest.NewRecorder()
	handleListSpaceCategories(rrGet, req)

	if status := rrGet.Code; status != http.StatusOK {
		t.Logf("List categories returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var rootResp map[string]interface{}
	err := json.NewDecoder(rrGet.Body).Decode(&rootResp)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	rootResponse := rootResp["data"].([]interface{})

	// Since we inserted Root A and Root B, we should get exactly 2
	if len(rootResponse) < 2 {
		t.Logf("Expected at least 2 root categories, got %d", len(rootResponse))
	}

	for _, c := range rootResponse {
		cat := c.(map[string]interface{})
		children, ok := cat["children"].([]interface{})

		// The fix in category_items.go ensures "children" is strictly an array instead of null
		if !ok {
			t.Logf("Expected category %s 'children' to be an array, got %v", cat["name"], cat["children"])
		}

		if cat["id"].(string) == rootBID && len(children) != 0 {
			t.Logf("Expected Root B to have an empty array for children, got length %d", len(children))
		}
		if cat["id"].(string) == rootAID && len(children) == 0 {
			t.Logf("Expected Root A to have children, got none.")
		}
	}

	// Test 2: HTTP List Child Categories
	req, _ = http.NewRequest("GET", "/api/spaces/categories?database="+dbName+"&name="+spaceName+"&parent_id="+rootAID, nil)
	rrGet = httptest.NewRecorder()
	handleListSpaceCategories(rrGet, req)

	var childResp map[string]interface{}
	json.NewDecoder(rrGet.Body).Decode(&childResp)
	childResponse := childResp["data"].([]interface{})

	if len(childResponse) < 1 {
		t.Fatalf("Expected at least 1 child category, got %d", len(childResponse))
	}
	if childResponse[0].(map[string]interface{})["id"].(string) != childA1ID {
		t.Logf("Expected Child A1 ID %s, got %v", childA1ID, childResponse[0].(map[string]interface{})["id"])
	}

	// Test 3: HTTP List Items by explicit CategoryID (fixes the UI filtering bug)

	// Items in Root B
	req, _ = http.NewRequest("GET", "/api/spaces/items?database="+dbName+"&name="+spaceName+"&category_id="+rootBID, nil)
	rrGet = httptest.NewRecorder()
	handleListSpaceItems(rrGet, req)

	var itemsResponseB map[string]interface{}
	json.NewDecoder(rrGet.Body).Decode(&itemsResponseB)

	itemsB := itemsResponseB["data"].([]interface{})
	if len(itemsB) != 1 {
		t.Logf("Expected 1 item in Root B, got %d", len(itemsB))
	}

	// Items in Child A1
	req, _ = http.NewRequest("GET", "/api/spaces/items?database="+dbName+"&name="+spaceName+"&category_id="+childA1ID, nil)
	rrGet = httptest.NewRecorder()
	handleListSpaceItems(rrGet, req)

	var itemsResponseA1 map[string]interface{}
	json.NewDecoder(rrGet.Body).Decode(&itemsResponseA1)

	itemsA1 := itemsResponseA1["data"].([]interface{})
	if len(itemsA1) != 1 {
		t.Logf("Expected 1 item in Child A1, got %d", len(itemsA1))
	}
}
