package main

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/encoding"
)

func TestHandleUpdateStoreInfo_AllowsNonStructuralUpdateOnNonEmptyStore(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "testdb_update_details"
	config = Config{
		Databases: []DatabaseConfig{{
			Name: dbName,
			Path: tmpDir,
			Mode: "standalone",
		}},
	}

	createReq := map[string]any{
		"database":   dbName,
		"store":      "store_details",
		"key_type":   "string",
		"value_type": "string",
		"data_size":  1,
	}
	body, _ := json.Marshal(createReq)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/store/add", bytes.NewReader(body))
	handleAddStore(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create store failed: %d %s", w.Code, w.Body.String())
	}

	addReq := map[string]any{
		"database": dbName,
		"store":    "store_details",
		"key":      "k1",
		"value":    "v1",
	}
	body, _ = json.Marshal(addReq)
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/store/item/add", bytes.NewReader(body))
	handleAddItem(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("add item failed: %d %s", w.Code, w.Body.String())
	}

	var target string
	err := filepath.WalkDir(tmpDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || filepath.Base(path) != "storeinfo.txt" || filepath.Base(filepath.Dir(path)) != "store_details" {
			return nil
		}
		target = path
		return nil
	})
	if err != nil {
		t.Fatalf("find storeinfo path: %v", err)
	}
	if target == "" {
		t.Fatal("storeinfo.txt not found")
	}

	ba, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read storeinfo: %v", err)
	}
	var info sop.StoreInfo
	if err := encoding.DefaultMarshaler.Unmarshal(ba, &info); err != nil {
		t.Fatalf("unmarshal storeinfo: %v", err)
	}
	info.IsValueDataGloballyCached = false
	ba, err = encoding.DefaultMarshaler.Marshal(info)
	if err != nil {
		t.Fatalf("marshal storeinfo: %v", err)
	}
	if err := os.WriteFile(target, ba, 0o644); err != nil {
		t.Fatalf("write storeinfo: %v", err)
	}

	updateReq := map[string]any{
		"database":    dbName,
		"storeName":   "store_details",
		"description": "updated description",
		"dataSize":    1,
		"slotLength":  1000,
		"isUnique":    true,
	}
	body, _ = json.Marshal(updateReq)
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/store/update", bytes.NewReader(body))
	handleUpdateStoreInfo(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected non-structural update to succeed, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleUpdateStoreInfo_IgnoresStaleStructuralDefaultsOnNonEmptyStore(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "testdb_update_details_stale"
	config = Config{
		Databases: []DatabaseConfig{{
			Name: dbName,
			Path: tmpDir,
			Mode: "standalone",
		}},
	}

	createReq := map[string]any{
		"database":   dbName,
		"store":      "store_details_stale",
		"key_type":   "string",
		"value_type": "string",
		"data_size":  1,
	}
	body, _ := json.Marshal(createReq)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/store/add", bytes.NewReader(body))
	handleAddStore(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create store failed: %d %s", w.Code, w.Body.String())
	}

	addReq := map[string]any{
		"database": dbName,
		"store":    "store_details_stale",
		"key":      "k1",
		"value":    "v1",
	}
	body, _ = json.Marshal(addReq)
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/store/item/add", bytes.NewReader(body))
	handleAddItem(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("add item failed: %d %s", w.Code, w.Body.String())
	}

	updateReq := map[string]any{
		"database":    dbName,
		"storeName":   "store_details_stale",
		"description": "updated description",
		"dataSize":    0,
		"slotLength":  0,
		"isUnique":    false,
	}
	body, _ = json.Marshal(updateReq)
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/store/update", bytes.NewReader(body))
	handleUpdateStoreInfo(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected stale structural defaults to be ignored for non-structural update, got %d: %s", w.Code, w.Body.String())
	}
}
