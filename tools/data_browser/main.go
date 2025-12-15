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
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/infs"
	"github.com/sharedcode/sop/jsondb"
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
	http.HandleFunc("/api/store/info", handleGetStoreInfo)
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

func handleGetStoreInfo(w http.ResponseWriter, r *http.Request) {
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

	// We need to open the store to get its info.
	// We can use a dummy comparer since we are not doing any operations.
	comparer := func(a, b any) int { return 0 }
	store, err := infs.OpenBtree[any, any](ctx, storeName, trans, comparer)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open store '%s': %v", storeName, err), http.StatusInternalServerError)
		return
	}

	si := store.GetStoreInfo()

	// Prepare response
	response := map[string]any{
		"name":           si.Name,
		"description":    si.Description,
		"count":          si.Count,
		"isPrimitiveKey": si.IsPrimitiveKey,
	}

	if !si.IsPrimitiveKey && si.MapKeyIndexSpecification != "" {
		var is jsondb.IndexSpecification
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(si.MapKeyIndexSpecification), &is); err == nil {
			response["indexSpec"] = is
		}
	}

	json.NewEncoder(w).Encode(response)
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

	// Variables to hold state for the closure
	var indexSpec *jsondb.IndexSpecification
	var isComplexKey bool

	// Proxy comparer
	comparer := func(a, b any) int {
		if !isComplexKey {
			return btree.Compare(a, b)
		}

		mapA, okA := a.(map[string]any)
		mapB, okB := b.(map[string]any)
		if !okA || !okB {
			return btree.Compare(a, b)
		}

		if indexSpec != nil {
			return indexSpec.Comparer(mapA, mapB)
		}

		// Default Map Comparer (Dynamic)
		// Collect all keys, sort them, compare values.
		keys := make([]string, 0, len(mapA)+len(mapB))
		seen := make(map[string]struct{})
		for k := range mapA {
			if _, exists := seen[k]; !exists {
				keys = append(keys, k)
				seen[k] = struct{}{}
			}
		}
		for k := range mapB {
			if _, exists := seen[k]; !exists {
				keys = append(keys, k)
				seen[k] = struct{}{}
			}
		}
		// We need to sort keys to be deterministic
		// Since we can't import "sort" inside function, we assume it's imported or use a simple bubble sort for small maps
		// But better to add "sort" to imports.
		// For now, let's use a simple swap sort since maps are usually small.
		for i := 0; i < len(keys); i++ {
			for j := i + 1; j < len(keys); j++ {
				if keys[i] > keys[j] {
					keys[i], keys[j] = keys[j], keys[i]
				}
			}
		}

		for _, k := range keys {
			valA, existsA := mapA[k]
			valB, existsB := mapB[k]

			if !existsA && !existsB {
				continue
			}
			if !existsA {
				return -1 // A is missing key, so A < B
			}
			if !existsB {
				return 1 // B is missing key, so A > B
			}

			res := btree.Compare(valA, valB)
			if res != 0 {
				return res
			}
		}
		return 0
	}

	// Open the B-Tree using 'any' for Key and Value to support generic browsing.
	store, err := infs.OpenBtree[any, any](ctx, storeName, trans, comparer)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open store '%s': %v", storeName, err), http.StatusInternalServerError)
		return
	}

	// Configure the comparer based on StoreInfo
	si := store.GetStoreInfo()
	if !si.IsPrimitiveKey {
		isComplexKey = true
		if si.MapKeyIndexSpecification != "" {
			var is jsondb.IndexSpecification
			if err := encoding.DefaultMarshaler.Unmarshal([]byte(si.MapKeyIndexSpecification), &is); err == nil {
				indexSpec = &is
			} else {
				log.Printf("Error unmarshaling index spec: %v", err)
			}
		}
	}

	var items []map[string]any
	limit := 100 // Hardcoded limit for Phase 1
	count := 0

	// Determine start position
	query := r.URL.Query().Get("q")
	action := r.URL.Query().Get("action")
	refKeyStr := r.URL.Query().Get("key")

	var ok bool

	// Helper to parse key
	parseKey := func(kStr string) any {
		var k any = kStr
		if isComplexKey {
			var mapKey map[string]any
			if err := json.Unmarshal([]byte(kStr), &mapKey); err == nil {
				k = mapKey
			}
		}
		return k
	}

	switch action {
	case "first":
		ok, err = store.First(ctx)
	case "last":
		ok, err = store.Last(ctx)
		if ok {
			// Go back limit-1 items to find start of last page
			for i := 0; i < limit-1; i++ {
				if ok, _ := store.Previous(ctx); !ok {
					break
				}
			}
		}
	case "next":
		if refKeyStr != "" {
			k := parseKey(refKeyStr)
			if found, _ := store.Find(ctx, k, true); found {
				ok, err = store.Next(ctx)
			} else {
				// If key not found, fallback to first
				ok, err = store.First(ctx)
			}
		} else {
			ok, err = store.First(ctx)
		}
	case "prev":
		if refKeyStr != "" {
			k := parseKey(refKeyStr)
			if found, _ := store.Find(ctx, k, true); found {
				if ok, _ = store.Previous(ctx); ok {
					// Go back limit-1 more
					for i := 0; i < limit-1; i++ {
						if okPrev, _ := store.Previous(ctx); !okPrev {
							break
						}
					}
					// We are positioned at the start of the previous page
					ok = true
				} else {
					// Already at start
					ok, err = store.First(ctx)
				}
			} else {
				ok, err = store.First(ctx)
			}
		} else {
			ok, err = store.First(ctx)
		}
	default:
		if query != "" {
			// Try to find the item with the query key.
			searchKey := parseKey(query)
			fmt.Printf("searchKey: %v/n", searchKey)
			ok, err = store.Find(ctx, searchKey, false)
			if store.GetCurrentKey().Key != nil {
				ok = true
			}
		} else {
			// Iterate from the first item
			ok, err = store.First(ctx)
		}
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
