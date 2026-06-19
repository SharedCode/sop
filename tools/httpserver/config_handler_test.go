package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHandleUninstallSystem_RejectsSystemDBDeletion(t *testing.T) {
	defer func() { config = Config{} }()

	tmpDir := t.TempDir()
	systemDBPath := filepath.Join(tmpDir, "system-db")
	if err := os.MkdirAll(systemDBPath, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	config = Config{
		ConfigFile: filepath.Join(tmpDir, "config.json"),
		SystemDB:   &DatabaseConfig{Path: systemDBPath},
	}

	reqBody := map[string]any{"delete_system_db": true}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/system/uninstall", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	handleUninstallSystem(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("handleUninstallSystem() status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	if _, err := os.Stat(systemDBPath); err != nil {
		t.Fatalf("system DB path was removed unexpectedly: %v", err)
	}
}
