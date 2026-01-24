package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestPathRelaxation(t *testing.T) {
	// Setup temporary directory structure
	tmpDir, err := os.MkdirTemp("", "sop_path_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create 'system_db' subfolder and artifacts
	sysDB := filepath.Join(tmpDir, "system_db")
	if err := os.Mkdir(sysDB, 0755); err != nil {
		t.Fatal(err)
	}

	// Create dboptions.json
	dbOptsFile := filepath.Join(sysDB, "dboptions.json")
	if err := os.WriteFile(dbOptsFile, []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}
	// Create registry_hash_mod_value.txt
	regHashFile := filepath.Join(sysDB, "registry_hash_mod_value.txt")
	if err := os.WriteFile(regHashFile, []byte("0"), 0644); err != nil {
		t.Fatal(err)
	}

	// Test 1: ValidatePath(root) should pass
	t.Run("ValidatePath", func(t *testing.T) {
		// handleValidatePath reads 'path' from Query Param
		req := httptest.NewRequest("GET", "/api/v1/validate-path?path="+tmpDir, nil)
		w := httptest.NewRecorder()

		handleValidatePath(w, req)

		resp := w.Result()
		if resp.StatusCode != 200 {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)

		if hasDB, ok := result["hasDBOptions"].(bool); !ok || !hasDB {
			t.Errorf("Expected hasDBOptions=true for parent folder of system_db, got %v", result["hasDBOptions"])
		}
	})

	// Test 2: SaveConfig(root) should detect Shared Brain
	t.Run("SaveConfig", func(t *testing.T) {
		payload := map[string]interface{}{
			"registry_path":    tmpDir,
			"use_shared_brain": true,
			"type":             "clustered",
		}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest("POST", "/api/v1/config", bytes.NewReader(body))
		w := httptest.NewRecorder()

		handleSaveConfig(w, req)

		// Check if dboptions.json appeared in tmpDir root
		if _, err := os.Stat(filepath.Join(tmpDir, "dboptions.json")); err == nil {
			t.Errorf("SaveConfig created new DB in root instead of reusing system_db subdir")
		}
	})
}
