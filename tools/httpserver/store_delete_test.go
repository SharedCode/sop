package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/database"
)

func TestHandleDeleteItem_RejectsDeletingOnlyRemainingRow(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "testdb_delete_last_row"
	config = Config{
		Databases: []DatabaseConfig{{
			Name: dbName,
			Path: tmpDir,
			Mode: "standalone",
		}},
	}

	createReq := map[string]any{
		"database":   dbName,
		"store":      "single_row_store",
		"key_type":   "string",
		"value_type": "string",
	}
	body, _ := json.Marshal(createReq)
	req := httptest.NewRequest(http.MethodPost, "/api/store/add", bytes.NewBuffer(body))
	w := httptest.NewRecorder()
	handleAddStore(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create store failed: %d %s", w.Code, w.Body.String())
	}

	addReq := map[string]any{
		"database": dbName,
		"store":    "single_row_store",
		"key":      "only-row",
		"value":    "data",
	}
	body, _ = json.Marshal(addReq)
	req = httptest.NewRequest(http.MethodPost, "/api/store/item/add", bytes.NewBuffer(body))
	w = httptest.NewRecorder()
	handleAddItem(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("add row failed: %d %s", w.Code, w.Body.String())
	}

	deleteReq := map[string]any{
		"database": dbName,
		"store":    "single_row_store",
		"key":      "only-row",
	}
	body, _ = json.Marshal(deleteReq)
	req = httptest.NewRequest(http.MethodPost, "/api/store/item/delete", bytes.NewBuffer(body))
	w = httptest.NewRecorder()
	handleDeleteItem(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when deleting the only row, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "one row must remain") {
		t.Fatalf("expected preservation message, got %s", w.Body.String())
	}

	dbOpts, err := getDBOptions(context.Background(), dbName)
	if err != nil {
		t.Fatalf("getDBOptions failed: %v", err)
	}
	trans, err := database.BeginTransaction(context.Background(), dbOpts, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	defer trans.Rollback(context.Background())

	store, err := database.OpenBtree[string, any](context.Background(), dbOpts, "single_row_store", trans, nil)
	if err != nil {
		t.Fatalf("OpenBtree failed: %v", err)
	}
	if got := store.Count(); got != 1 {
		t.Fatalf("expected the only row to remain, got %d", got)
	}
}

func TestHandleDeleteItems_PreservesAnchorRow(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "testdb_delete_anchor"
	config = Config{
		Databases: []DatabaseConfig{{
			Name: dbName,
			Path: tmpDir,
			Mode: "standalone",
		}},
	}

	createReq := map[string]any{
		"database":   dbName,
		"store":      "anchor_store",
		"key_type":   "string",
		"value_type": "string",
	}
	body, _ := json.Marshal(createReq)
	req := httptest.NewRequest(http.MethodPost, "/api/store/add", bytes.NewBuffer(body))
	w := httptest.NewRecorder()
	handleAddStore(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create store failed: %d %s", w.Code, w.Body.String())
	}

	addReq := map[string]any{
		"database": dbName,
		"store":    "anchor_store",
		"key":      "root_anchor",
		"value":    "anchor-value",
	}
	body, _ = json.Marshal(addReq)
	req = httptest.NewRequest(http.MethodPost, "/api/store/item/add", bytes.NewBuffer(body))
	w = httptest.NewRecorder()
	handleAddItem(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("add anchor failed: %d %s", w.Code, w.Body.String())
	}

	addReq = map[string]any{
		"database": dbName,
		"store":    "anchor_store",
		"key":      "user-row",
		"value":    "data-value",
	}
	body, _ = json.Marshal(addReq)
	req = httptest.NewRequest(http.MethodPost, "/api/store/item/add", bytes.NewBuffer(body))
	w = httptest.NewRecorder()
	handleAddItem(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("add user row failed: %d %s", w.Code, w.Body.String())
	}

	bulkReq := map[string]any{
		"database": dbName,
		"store":    "anchor_store",
		"keys":     []any{"root_anchor", "user-row"},
	}
	body, _ = json.Marshal(bulkReq)
	req = httptest.NewRequest(http.MethodPost, "/api/store/items/delete", bytes.NewBuffer(body))
	w = httptest.NewRecorder()
	handleDeleteItems(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("bulk delete failed: %d %s", w.Code, w.Body.String())
	}

	dbOpts, err := getDBOptions(context.Background(), dbName)
	if err != nil {
		t.Fatalf("getDBOptions failed: %v", err)
	}

	trans, err := database.BeginTransaction(context.Background(), dbOpts, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	defer trans.Rollback(context.Background())

	store, err := database.OpenBtree[string, any](context.Background(), dbOpts, "anchor_store", trans, nil)
	if err != nil {
		t.Fatalf("OpenBtree failed: %v", err)
	}

	if got := store.Count(); got != 1 {
		t.Fatalf("expected one remaining row, got %d", got)
	}

	found, err := store.Find(context.Background(), "root_anchor", true)
	if err != nil {
		t.Fatalf("Find root_anchor failed: %v", err)
	}
	if !found {
		t.Fatal("expected anchor row to remain")
	}
}
