package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/vector"
	"github.com/sharedcode/sop/fs"
)

// --- Vector Database & Store Management ---

// We use map[string]any as the generic type T for Python interoperability.
type PyVectorDB = *vector.Database[map[string]any]
type PyVectorStore = ai.VectorStore[map[string]any]

var vectorDBRegistry = NewRegistry[PyVectorDB]()
var vectorStoreRegistry = NewRegistry[PyVectorStore]()

// Unified Database Lookup (replaces modelDBLookup)
var dbRegistry = NewRegistry[*database.Database]()
var modelStoreRegistry = NewRegistry[ai.ModelStore]()

const (
	VectorActionUnknown = iota
	NewVectorDB
	OpenVectorStore
	UpsertVector
	UpsertBatchVector
	GetVector
	DeleteVector
	QueryVector
	VectorCount
	VectorWithTransaction
	OptimizeVector
)

type VectorDBOptions struct {
	StoragePath   string                            `json:"storage_path"`
	UsageMode     int                               `json:"usage_mode"` // 0: BuildOnce, 1: DynamicCount, 2: Dynamic
	DBType        int                               `json:"db_type"`    // 0: Standalone, 1: Clustered
	ErasureConfig map[string]fs.ErasureCodingConfig `json:"erasure_config,omitempty"`
	StoresFolders []string                          `json:"stores_folders,omitempty"`
}

type VectorQueryOptions struct {
	Vector []float32      `json:"vector"`
	K      int            `json:"k"`
	Filter map[string]any `json:"filter"` // Simple equality match for now
}

//export manageVectorDB
func manageVectorDB(ctxID C.longlong, action C.int, targetID *C.char, payload *C.char) *C.char {
	ctx := getContext(ctxID)
	if ctx == nil {
		return C.CString(fmt.Sprintf("context with ID %v not found", int64(ctxID)))
	}

	targetUUID, _ := sop.ParseUUID(C.GoString(targetID))
	jsonPayload := C.GoString(payload)

	switch int(action) {
	case NewVectorDB:
		var opts VectorDBOptions
		if err := json.Unmarshal([]byte(jsonPayload), &opts); err != nil {
			return C.CString(fmt.Sprintf("invalid options: %v", err))
		}

		// Create DB
		db := vector.NewDatabase[map[string]any](ai.DatabaseType(opts.DBType))
		if opts.StoragePath != "" {
			db.SetStoragePath(opts.StoragePath)
		}
		db.SetUsageMode(ai.UsageMode(opts.UsageMode))

		if len(opts.ErasureConfig) > 0 || len(opts.StoresFolders) > 0 {
			db.SetReplicationConfig(opts.ErasureConfig, opts.StoresFolders)
		}

		id := vectorDBRegistry.Add(db)

		return C.CString(id.String())

	case OpenVectorStore:
		// targetID is the DB UUID
		db, ok := vectorDBRegistry.Get(targetUUID)
		if !ok {
			// Try finding in unified DB lookup
			unifiedDB, uOk := dbRegistry.Get(targetUUID)

			if uOk {
				storeName := jsonPayload
				store := database.OpenVectorStore[map[string]any](ctx, unifiedDB, storeName)

				id := vectorStoreRegistry.Add(store)

				return C.CString(id.String())
			}

			return C.CString("Vector DB not found")
		}

		storeName := jsonPayload // Payload is just the name
		store := db.Open(ctx, storeName)

		id := vectorStoreRegistry.Add(store)

		return C.CString(id.String())

	case UpsertVector:
		// targetID is the Store UUID
		store, ok := vectorStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Vector Store not found")
		}

		var item ai.Item[map[string]any]
		if err := json.Unmarshal([]byte(jsonPayload), &item); err != nil {
			return C.CString(fmt.Sprintf("invalid item: %v", err))
		}

		if err := store.Upsert(ctx, item); err != nil {
			return C.CString(err.Error())
		}

	case UpsertBatchVector:
		store, ok := vectorStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Vector Store not found")
		}

		var items []ai.Item[map[string]any]
		if err := json.Unmarshal([]byte(jsonPayload), &items); err != nil {
			return C.CString(fmt.Sprintf("invalid items: %v", err))
		}

		if err := store.UpsertBatch(ctx, items); err != nil {
			return C.CString(err.Error())
		}

	case GetVector:
		store, ok := vectorStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Vector Store not found")
		}

		itemID := jsonPayload
		item, err := store.Get(ctx, itemID)
		if err != nil {
			return C.CString(err.Error())
		}

		data, _ := json.Marshal(item)
		return C.CString(string(data))

	case DeleteVector:
		store, ok := vectorStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Vector Store not found")
		}

		itemID := jsonPayload
		if err := store.Delete(ctx, itemID); err != nil {
			return C.CString(err.Error())
		}

	case QueryVector:
		store, ok := vectorStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Vector Store not found")
		}

		var opts VectorQueryOptions
		if err := json.Unmarshal([]byte(jsonPayload), &opts); err != nil {
			return C.CString(fmt.Sprintf("invalid query options: %v", err))
		}

		// Simple equality filter
		var filterFunc func(map[string]any) bool
		if len(opts.Filter) > 0 {
			filterFunc = func(payload map[string]any) bool {
				for k, v := range opts.Filter {
					if val, ok := payload[k]; !ok || val != v {
						return false
					}
				}
				return true
			}
		}

		hits, err := store.Query(ctx, opts.Vector, opts.K, filterFunc)
		if err != nil {
			return C.CString(err.Error())
		}

		data, _ := json.Marshal(hits)
		return C.CString(string(data))

	case VectorCount:
		store, ok := vectorStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Vector Store not found")
		}

		count, err := store.Count(ctx)
		if err != nil {
			return C.CString(err.Error())
		}
		return C.CString(fmt.Sprintf("%d", count))

	case VectorWithTransaction:
		store, ok := vectorStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Vector Store not found")
		}

		// Payload is Transaction UUID
		transUUID, err := sop.ParseUUID(jsonPayload)
		if err != nil {
			return C.CString("Invalid transaction UUID")
		}

		item, ok := Transactions.GetItem(transUUID)
		if !ok {
			return C.CString("Transaction not found")
		}

		newStore := store.WithTransaction(item.Transaction)
		id := vectorStoreRegistry.Add(newStore)

		return C.CString(id.String())

	case OptimizeVector:
		store, ok := vectorStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Vector Store not found")
		}

		if err := store.Optimize(ctx); err != nil {
			return C.CString(err.Error())
		}
	}

	return nil
}

// --- Model Store Management ---

const (
	ModelActionUnknown = iota
	NewBTreeModelStore
	NewModelDB
	OpenModelStore
	SaveModel
	LoadModel
	ListModels
	DeleteModel
)

type ModelDBOptions struct {
	StoragePath   string                            `json:"storage_path"`
	DBType        int                               `json:"db_type"` // 0: Standalone, 1: Clustered
	ErasureConfig map[string]fs.ErasureCodingConfig `json:"erasure_config,omitempty"`
	StoresFolders []string                          `json:"stores_folders,omitempty"`
}

type ModelStoreOptions struct {
	Path          string `json:"path"`
	TransactionID string `json:"transaction_id"`
}

type ModelItem struct {
	Category string `json:"category"`
	Name     string `json:"name"`
	Model    any    `json:"model"`
}

//export manageModelStore
func manageModelStore(ctxID C.longlong, action C.int, targetID *C.char, payload *C.char) *C.char {
	ctx := getContext(ctxID)
	if ctx == nil {
		return C.CString(fmt.Sprintf("context with ID %v not found", int64(ctxID)))
	}

	targetUUID, _ := sop.ParseUUID(C.GoString(targetID))
	jsonPayload := C.GoString(payload)

	switch int(action) {
	case NewModelDB:
		var opts ModelDBOptions
		if err := json.Unmarshal([]byte(jsonPayload), &opts); err != nil {
			return C.CString(fmt.Sprintf("invalid options: %v", err))
		}

		db := database.NewDatabase(ai.DatabaseType(opts.DBType), opts.StoragePath)
		if len(opts.ErasureConfig) > 0 || len(opts.StoresFolders) > 0 {
			db.SetReplicationConfig(opts.ErasureConfig, opts.StoresFolders)
		}
		id := dbRegistry.Add(db)

		return C.CString(id.String())

	case OpenModelStore:
		db, ok := dbRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Model DB not found")
		}

		storeName := jsonPayload
		store, err := db.OpenModelStore(storeName)
		if err != nil {
			return C.CString(err.Error())
		}

		id := modelStoreRegistry.Add(store)

		return C.CString(id.String())

	case NewBTreeModelStore:
		var opts ModelStoreOptions
		if err := json.Unmarshal([]byte(jsonPayload), &opts); err != nil {
			return C.CString(fmt.Sprintf("invalid options: %v", err))
		}

		transUUID, err := sop.ParseUUID(opts.TransactionID)
		if err != nil {
			return C.CString("Invalid transaction UUID")
		}

		item, ok := Transactions.GetItem(transUUID)
		if !ok {
			return C.CString("Transaction not found")
		}

		store, err := database.NewBTreeModelStore(ctx, item.Transaction)
		if err != nil {
			return C.CString(err.Error())
		}

		id := modelStoreRegistry.Add(store)

		return C.CString(id.String())

	case SaveModel:
		store, ok := modelStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Model Store not found")
		}

		var item ModelItem
		if err := json.Unmarshal([]byte(jsonPayload), &item); err != nil {
			return C.CString(fmt.Sprintf("invalid item: %v", err))
		}

		if err := store.Save(ctx, item.Category, item.Name, item.Model); err != nil {
			return C.CString(err.Error())
		}

	case LoadModel:
		store, ok := modelStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Model Store not found")
		}

		var item ModelItem
		if err := json.Unmarshal([]byte(jsonPayload), &item); err != nil {
			return C.CString(fmt.Sprintf("invalid item: %v", err))
		}

		var target any
		if err := store.Load(ctx, item.Category, item.Name, &target); err != nil {
			return C.CString(err.Error())
		}

		data, _ := json.Marshal(target)
		return C.CString(string(data))

	case ListModels:
		store, ok := modelStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Model Store not found")
		}

		// Payload is the category string
		category := jsonPayload
		names, err := store.List(ctx, category)
		if err != nil {
			return C.CString(err.Error())
		}

		data, _ := json.Marshal(names)
		return C.CString(string(data))

	case DeleteModel:
		store, ok := modelStoreRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Model Store not found")
		}

		var item ModelItem
		if err := json.Unmarshal([]byte(jsonPayload), &item); err != nil {
			return C.CString(fmt.Sprintf("invalid item: %v", err))
		}

		if err := store.Delete(ctx, item.Category, item.Name); err != nil {
			return C.CString(err.Error())
		}
	}

	return nil
}
