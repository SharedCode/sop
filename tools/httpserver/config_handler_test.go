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

func TestHandleDeleteEnvironment_RejectsPathTraversal(t *testing.T) {
	defer func() { config = Config{} }()

	tmpDir := t.TempDir()
	victim := filepath.Join(tmpDir, "victim.json")
	if err := os.WriteFile(victim, []byte("{}"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	reqBody := map[string]any{"filename": "../" + filepath.Base(victim)}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	// Run from inside tmpDir so a naive relative traversal would actually reach victim.json.
	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	nested := filepath.Join(tmpDir, "nested")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.Chdir(nested); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}
	defer os.Chdir(origWD)

	req := httptest.NewRequest(http.MethodPost, "/api/config/environments/delete", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	handleDeleteEnvironment(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("handleDeleteEnvironment() status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("victim file was removed by a path-traversal filename: %v", err)
	}
}

func TestHandleSwitchEnvironment_RejectsPathTraversal(t *testing.T) {
	defer func() { config = Config{} }()

	reqBody := map[string]any{"filename": "../../etc/passwd"}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/config/environments/switch", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	handleSwitchEnvironment(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("handleSwitchEnvironment() status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestHandleCreateEnvironment_RejectsPathTraversal(t *testing.T) {
	defer func() { config = Config{} }()

	reqBody := map[string]any{"name": "../../tmp/evil"}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/config/environments/create", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	handleCreateEnvironment(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("handleCreateEnvironment() status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestValidateEnvironmentFilename(t *testing.T) {
	for _, name := range []string{"", ".", "..", "../outside.json", "nested/config.json", `/tmp/config.json`, `..\outside.json`, `nested\config.json`, "config\x00.json"} {
		if got, err := validateEnvironmentFilename(name); err == nil {
			t.Errorf("accepted %q as %q", name, got)
		}
	}
	for _, name := range []string{"config.json", "team prod.json", "prod", "release..json"} {
		got, err := validateEnvironmentFilename("  " + name + "  ")
		if err != nil || got != name {
			t.Errorf("filename %q: got %q err=%v", name, got, err)
		}
	}
}

func TestEnvironmentHandlersRejectNonBasenames(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("SOP_ROOT", "")
	oldCatalog := modelCatalog
	t.Cleanup(func() { modelCatalog = oldCatalog })
	for _, handler := range []struct {
		name, field string
		serve       http.HandlerFunc
	}{
		{"create", "name", handleCreateEnvironment},
		{"switch", "filename", handleSwitchEnvironment},
		{"delete", "filename", handleDeleteEnvironment},
	} {
		for _, name := range []string{"../outside.json", `..\outside.json`, "bad\x00.json"} {
			t.Run(handler.name+"/"+name, func(t *testing.T) {
				old := config
				t.Cleanup(func() { config = old })
				config = Config{ConfigFile: "unchanged.json"}
				body, err := json.Marshal(map[string]string{handler.field: name})
				if err != nil {
					t.Fatal(err)
				}
				rec := httptest.NewRecorder()
				handler.serve(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
				}
				if config.ConfigFile != "unchanged.json" {
					t.Fatal("rejected filename changed active config")
				}
			})
		}
	}
}

func TestEnvironmentHandlersAcceptBareFilenames(t *testing.T) {
	oldConfig, oldCatalog := config, modelCatalog
	t.Cleanup(func() { config, modelCatalog = oldConfig, oldCatalog })
	t.Chdir(t.TempDir())
	t.Setenv("SOP_ROOT", "")
	if err := os.WriteFile("existing.json", []byte(`{"port":8081}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, request := range []struct {
		name, body string
		serve      http.HandlerFunc
		status     int
	}{
		{"create", `{"name":"new-team"}`, handleCreateEnvironment, http.StatusOK},
		{"switch", `{"filename":"existing.json"}`, handleSwitchEnvironment, http.StatusOK},
	} {
		t.Run(request.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			request.serve(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(request.body)))
			if rec.Code != request.status {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
	if config.ConfigFile != "existing.json" {
		t.Fatalf("active config=%q", config.ConfigFile)
	}
}
