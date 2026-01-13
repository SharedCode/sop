package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/database"
)

// handleSaveConfig writes the provided configuration to the specified file path.
func handleSaveConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		RegistryPath string `json:"registry_path"`
		Port         int    `json:"port"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Update global config
	if req.Port > 0 {
		config.Port = req.Port
	}

	// We don't have a specific field for RegistryPath in Config struct yet,
	// but usually the first DB is treated as system/default.
	// Or maybe we should add a SystemDB field to Config?
	// For now, let's just ensure the config file exists.

	if config.ConfigFile == "" {
		config.ConfigFile = "config.json"
	}

	// Ensure directory exists
	dir := filepath.Dir(config.ConfigFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create directory: %v", err), http.StatusInternalServerError)
		return
	}

	// Write config file
	f, err := os.Create(config.ConfigFile)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create config file: %v", err), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "    ")
	if err := encoder.Encode(config); err != nil {
		http.Error(w, fmt.Sprintf("Failed to write config file: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "Configuration saved successfully"})
}

// handleInitDatabase initializes a database folder structure and writes configuration.
func handleInitDatabase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Name         string `json:"name"`
		Path         string `json:"path"`
		Type         string `json:"type"`
		Connection   string `json:"connection"`
		PopulateDemo bool   `json:"populate_demo"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.Path == "" || req.Name == "" {
		http.Error(w, "Database path and name are required", http.StatusBadRequest)
		return
	}

	// Construct Options
	options := sop.DatabaseOptions{
		StoresFolders: []string{req.Path},
	}

	// 1. Setup Database (Creates folders, writes dboptions.json)
	// This uses the official SOP setup routine.
	ctx := context.Background()
	if _, err := database.Setup(ctx, options); err != nil {
		http.Error(w, fmt.Sprintf("Failed to setup database: %v", err), http.StatusInternalServerError)
		return
	}

	// 2. Initialize Registry (Trigger reghashmod.txt creation)
	// We use the provided options to initialize the database instance.

	if req.PopulateDemo {
		if err := PopulateDemoData(ctx, options); err != nil {
			http.Error(w, fmt.Sprintf("Failed to populate demo data: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		// Start a transaction to force initialization of the registry
		tx, err := database.BeginTransaction(ctx, options, sop.ForWriting)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to begin transaction: %v", err), http.StatusInternalServerError)
			return
		}

		comparer := func(a, b string) int {
			return cmp.Compare(a, b)
		}

		// Open a dummy store to force repository initialization
		_, err = database.NewBtree[string, string](ctx, options, "system_check", tx, comparer)
		if err != nil {
			tx.Rollback(ctx)
			http.Error(w, fmt.Sprintf("Failed to initialize database registry: %v", err), http.StatusInternalServerError)
			return
		}

		// We don't add anything.
		if err := tx.Commit(ctx); err != nil {
			http.Error(w, fmt.Sprintf("Failed to commit initialization transaction: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Add to global config and save
	newDB := DatabaseConfig{
		Name: req.Name,
		Path: req.Path,
		Mode: "standalone", // Default to standalone for now
	}

	if req.Type == "redis" {
		newDB.Mode = "clustered"
		newDB.RedisURL = req.Connection
		if newDB.RedisURL == "" {
			newDB.RedisURL = "localhost:6379"
		}
	}

	// Check if exists
	exists := false
	for i, db := range config.Databases {
		if db.Name == newDB.Name {
			config.Databases[i] = newDB
			exists = true
			break
		}
	}
	if !exists {
		config.Databases = append(config.Databases, newDB)
	}

	// Save Config
	if config.ConfigFile != "" {
		f, err := os.Create(config.ConfigFile)
		if err == nil {
			defer f.Close()
			encoder := json.NewEncoder(f)
			encoder.SetIndent("", "    ")
			encoder.Encode(config)
		} else {
			log.Error(fmt.Sprintf("Failed to save config after db init: %v", err))
		}
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "Database initialized successfully"})
}

// handleValidatePath checks if a path is valid/writable
func handleValidatePath(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "Path is required", http.StatusBadRequest)
		return
	}

	// Check if exists
	info, err := os.Stat(path)
	exists := err == nil
	isDir := exists && info.IsDir()

	// Check writability (try to create a temp file)
	writable := false
	if exists && isDir {
		tmpFile := filepath.Join(path, ".write_test")
		if f, err := os.Create(tmpFile); err == nil {
			f.Close()
			os.Remove(tmpFile)
			writable = true
		}
	} else if !exists {
		// Check if parent is writable
		parent := filepath.Dir(path)
		if _, err := os.Stat(parent); err == nil {
			tmpFile := filepath.Join(parent, ".write_test")
			if f, err := os.Create(tmpFile); err == nil {
				f.Close()
				os.Remove(tmpFile)
				writable = true
			}
		}
	}

	json.NewEncoder(w).Encode(map[string]any{
		"exists":   exists,
		"isDir":    isDir,
		"writable": writable,
	})
}
