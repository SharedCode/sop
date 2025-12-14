package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/infs"
)

// Config holds the server configuration
type Config struct {
	Port         int
	RegistryPath string
}

var config Config

func main() {
	flag.IntVar(&config.Port, "port", 8080, "Port to run the server on")
	flag.StringVar(&config.RegistryPath, "registry", "/tmp/sop_data", "Path to the SOP registry/data directory")
	flag.Parse()

	// Ensure registry path exists (basic check)
	if _, err := os.Stat(config.RegistryPath); os.IsNotExist(err) {
		log.Printf("Warning: Registry path %s does not exist. Please ensure it is correct.", config.RegistryPath)
	}

	// Setup Routes
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/api/stores", handleListStores)
	http.HandleFunc("/api/store/items", handleListItems)

	// Start Server
	addr := fmt.Sprintf(":%d", config.Port)
	log.Printf("SOP Data Browser running at http://localhost%s", addr)
	log.Printf("Target Registry: %s", config.RegistryPath)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	tmplPath := filepath.Join("templates", "index.html")
	// In a real build, we might embed this using embed package
	tmpl, err := template.ParseFiles(tmplPath)
	if err != nil {
		// Fallback for running from different CWD
		tmpl, err = template.ParseFiles(filepath.Join("tools", "data_browser", "templates", "index.html"))
		if err != nil {
			http.Error(w, "Could not load template: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	tmpl.Execute(w, nil)
}

func handleListStores(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{config.RegistryPath},
	}

	// Open a read-only transaction to fetch stores
	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	stores, err := trans.GetStores(ctx)
	if err != nil {
		http.Error(w, "Failed to list stores: "+err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(stores)
}

func handleListItems(w http.ResponseWriter, r *http.Request) {
	storeName := r.URL.Query().Get("name")
	if storeName == "" {
		http.Error(w, "Store name is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{config.RegistryPath},
	}

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	// Open the B-Tree using 'any' for Key and Value to support generic browsing.
	// We pass nil for comparer to let SOP auto-detect/coerce the key type.
	store, err := infs.OpenBtree[any, any](ctx, storeName, trans, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open store '%s': %v", storeName, err), http.StatusInternalServerError)
		return
	}

	var items []map[string]any
	limit := 100 // Hardcoded limit for Phase 1
	count := 0

	// Determine start position
	query := r.URL.Query().Get("q")
	var ok bool
	if query != "" {
		// Try to find the item with the query key.
		// Note: This assumes the key is a string. If the underlying store uses int/float keys,
		// this might fail or need type coercion logic.
		ok, err = store.Find(ctx, query, true)
	} else {
		// Iterate from the first item
		ok, err = store.First(ctx)
	}

	for ok && err == nil && count < limit {
		kItem := store.GetCurrentKey()
		v, err := store.GetCurrentValue(ctx)
		if err != nil {
			log.Printf("Error reading value for key %v: %v", kItem.Key, err)
			// Continue to next item even if value fetch fails? Or break?
			// Let's try to continue.
		}

		items = append(items, map[string]any{
			"key":   kItem.Key,
			"value": v,
		})

		ok, err = store.Next(ctx)
		count++
	}

	if err != nil {
		log.Printf("Error during iteration: %v", err)
	}

	json.NewEncoder(w).Encode(items)
}
