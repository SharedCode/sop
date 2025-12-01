package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/model"
	"github.com/sharedcode/sop/ai/vector"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/inredfs"
)

// --- Vector Database & Store Management ---

// We use map[string]any as the generic type T for Python interoperability.
// PyVectorDB is now just *database.Database
type PyVectorStore = ai.VectorStore[map[string]any]

const (
	KeyID                = "ID"
	KeyVector            = "Vector"
	KeyPayload           = "Payload"
	KeyCentroidID        = "CentroidID"
	KeyScore             = "Score"
	KeyMetaID            = "id"
	KeyMetaTransactionID = "transaction_id"
)

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
	VectorBeginTransaction // New Action
	OptimizeVector
)

type VectorDBOptions struct {
	StoragePath   string                            `json:"storage_path"`
	DBType        int                               `json:"db_type"` // 0: Standalone, 1: Clustered
	ErasureConfig map[string]fs.ErasureCodingConfig `json:"erasure_config,omitempty"`
	StoresFolders []string                          `json:"stores_folders,omitempty"`
}

type VectorStoreConfig struct {
	UsageMode   int `json:"usage_mode"`
	ContentSize int `json:"content_size"`
}

type VectorStoreTransportOptions struct {
	TransactionID string            `json:"transaction_id"`
	Name          string            `json:"name"`
	Config        VectorStoreConfig `json:"config"`
	StoragePath   string            `json:"storage_path"`
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

	targetIDStr := C.GoString(targetID)
	jsonPayload := C.GoString(payload)

	// Helper to get store from metadata
	getStore := func() (PyVectorStore, error) {
		var meta map[string]string
		if err := json.Unmarshal([]byte(targetIDStr), &meta); err != nil {
			return nil, fmt.Errorf("invalid store metadata: %v", err)
		}
		transUUID, err := sop.ParseUUID(meta[KeyMetaTransactionID])
		if err != nil {
			return nil, fmt.Errorf("invalid transaction UUID: %v", err)
		}
		storeUUID, err := sop.ParseUUID(meta[KeyMetaID])
		if err != nil {
			return nil, fmt.Errorf("invalid store UUID: %v", err)
		}

		obj, ok := Transactions.GetBtree(transUUID, storeUUID)
		if !ok {
			return nil, fmt.Errorf("Vector Store not found in transaction")
		}
		store, ok := obj.(PyVectorStore)
		if !ok {
			return nil, fmt.Errorf("object is not a Vector Store")
		}
		return store, nil
	}

	switch int(action) {
	case NewVectorDB:
		var opts VectorDBOptions
		if err := json.Unmarshal([]byte(jsonPayload), &opts); err != nil {
			return C.CString(fmt.Sprintf("invalid options: %v", err))
		}

		// Create DB
		db := database.NewDatabase(database.DatabaseType(opts.DBType), opts.StoragePath)
		if len(opts.ErasureConfig) > 0 || len(opts.StoresFolders) > 0 {
			db.SetReplicationConfig(opts.ErasureConfig, opts.StoresFolders)
		}

		id := dbRegistry.Add(db)

		return C.CString(id.String())

	case VectorBeginTransaction:
		// targetID is the DB UUID
		targetUUID, _ := sop.ParseUUID(targetIDStr)
		db, ok := dbRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Database not found")
		}

		// Payload is Transaction Mode (int). Default to ReadWrite (1) if not specified or 0?
		// sop.TransactionMode: Read=0, ReadWrite=1.
		// Let's assume payload is just the integer string.
		mode := sop.ForWriting
		var opts inredfs.TransationOptionsWithReplication
		if jsonPayload != "" {
			if err := json.Unmarshal([]byte(jsonPayload), &opts); err == nil {
				mode = opts.Mode
				// Adjust MaxTime from minutes to Duration.
				opts.MaxTime = opts.MaxTime * time.Minute
			} else {
				var m int
				if err := json.Unmarshal([]byte(jsonPayload), &m); err == nil {
					mode = sop.TransactionMode(m)
				}
			}
		}

		tx, err := db.BeginTransaction(ctx, mode, opts)
		if err != nil {
			return C.CString(err.Error())
		}

		id := Transactions.Add(tx)
		return C.CString(id.String())

	case OpenVectorStore:
		var opts VectorStoreTransportOptions
		if err := json.Unmarshal([]byte(jsonPayload), &opts); err != nil {
			return C.CString(fmt.Sprintf("invalid options: %v", err))
		}
		log.Printf("DEBUG: OpenVectorStore StoragePath='%s' Payload='%s'\n", opts.StoragePath, jsonPayload)

		// targetID is the DB UUID
		targetUUID, _ := sop.ParseUUID(targetIDStr)
		db, ok := dbRegistry.Get(targetUUID)
		if !ok {
			return C.CString("Database not found")
		}

		transUUID, err := sop.ParseUUID(opts.TransactionID)
		if err != nil {
			return C.CString("Invalid transaction UUID")
		}

		item, ok := Transactions.GetItem(transUUID)
		if !ok {
			return C.CString("Transaction not found")
		}

		cfg := vector.Config{
			UsageMode:   ai.UsageMode(opts.Config.UsageMode),
			ContentSize: sop.ValueDataSize(opts.Config.ContentSize),
			StoragePath: opts.StoragePath,
		}

		store, err := db.OpenVectorStore(ctx, opts.Name, item.Transaction, cfg)
		if err != nil {
			return C.CString(err.Error())
		}

		id, err := Transactions.AddBtree(transUUID, store)
		if err != nil {
			return C.CString(err.Error())
		}
		if id.IsNil() {
			return C.CString("Transaction not found during registration")
		}

		return C.CString(id.String())

	case UpsertVector:
		store, err := getStore()
		if err != nil {
			return C.CString(err.Error())
		}

		var item ai.Item[map[string]any]
		if err := json.Unmarshal([]byte(jsonPayload), &item); err != nil {
			return C.CString(fmt.Sprintf("invalid item: %v", err))
		}

		if err := store.Upsert(ctx, item); err != nil {
			return C.CString(err.Error())
		}

	case UpsertBatchVector:
		store, err := getStore()
		if err != nil {
			return C.CString(err.Error())
		}

		var items []ai.Item[map[string]any]
		if err := json.Unmarshal([]byte(jsonPayload), &items); err != nil {
			return C.CString(fmt.Sprintf("invalid items: %v", err))
		}

		if err := store.UpsertBatch(ctx, items); err != nil {
			return C.CString(err.Error())
		}

	case GetVector:
		store, err := getStore()
		if err != nil {
			return C.CString(err.Error())
		}

		itemID := jsonPayload
		item, err := store.Get(ctx, itemID)
		if err != nil {
			return C.CString(err.Error())
		}

		response := map[string]any{
			KeyID:         item.ID,
			KeyVector:     item.Vector,
			KeyPayload:    item.Payload,
			KeyCentroidID: item.CentroidID,
		}

		data, _ := json.Marshal(response)
		return C.CString(string(data))

	case DeleteVector:
		store, err := getStore()
		if err != nil {
			return C.CString(err.Error())
		}

		itemID := jsonPayload
		if err := store.Delete(ctx, itemID); err != nil {
			return C.CString(err.Error())
		}

	case QueryVector:
		store, err := getStore()
		if err != nil {
			return C.CString(err.Error())
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

		response := make([]map[string]any, len(hits))
		for i, h := range hits {
			response[i] = map[string]any{
				KeyID:      h.ID,
				KeyScore:   h.Score,
				KeyPayload: h.Payload,
			}
		}

		data, _ := json.Marshal(response)
		return C.CString(string(data))

	case VectorCount:
		store, err := getStore()
		if err != nil {
			return C.CString(err.Error())
		}

		count, err := store.Count(ctx)
		if err != nil {
			return C.CString(err.Error())
		}
		return C.CString(fmt.Sprintf("%d", count))

	case OptimizeVector:
		store, err := getStore()
		if err != nil {
			return C.CString(err.Error())
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
	CloseModelDB
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

	targetIDStr := C.GoString(targetID)
	jsonPayload := C.GoString(payload)

	// Helper to get store from metadata
	getStore := func() (ai.ModelStore, error) {
		var meta map[string]string
		if err := json.Unmarshal([]byte(targetIDStr), &meta); err != nil {
			return nil, fmt.Errorf("invalid store metadata: %v", err)
		}
		transUUID, err := sop.ParseUUID(meta[KeyMetaTransactionID])
		if err != nil {
			return nil, fmt.Errorf("invalid transaction UUID: %v", err)
		}
		storeUUID, err := sop.ParseUUID(meta[KeyMetaID])
		if err != nil {
			return nil, fmt.Errorf("invalid store UUID: %v", err)
		}

		obj, ok := Transactions.GetBtree(transUUID, storeUUID)
		if !ok {
			return nil, fmt.Errorf("Model Store not found in transaction")
		}
		store, ok := obj.(ai.ModelStore)
		if !ok {
			return nil, fmt.Errorf("object is not a Model Store")
		}
		return store, nil
	}

	switch int(action) {
	case NewModelDB:
		var opts ModelDBOptions
		if err := json.Unmarshal([]byte(jsonPayload), &opts); err != nil {
			return C.CString(fmt.Sprintf("invalid options: %v", err))
		}

		db := database.NewDatabase(database.DatabaseType(opts.DBType), opts.StoragePath)
		if len(opts.ErasureConfig) > 0 || len(opts.StoresFolders) > 0 {
			db.SetReplicationConfig(opts.ErasureConfig, opts.StoresFolders)
		}
		id := dbRegistry.Add(db)

		return C.CString(id.String())

	case OpenModelStore:
		return C.CString("OpenModelStore not supported in this version. Use NewBTreeModelStore.")

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

		// Use a default name "global" for the model store when created this way.
		store := model.New("global", item.Transaction)

		id, err := Transactions.AddBtree(transUUID, store)
		if err != nil {
			return C.CString(err.Error())
		}
		if id.IsNil() {
			return C.CString("Transaction not found during registration")
		}

		return C.CString(id.String())

	case SaveModel:
		store, err := getStore()
		if err != nil {
			return C.CString(err.Error())
		}

		var item ModelItem
		if err := json.Unmarshal([]byte(jsonPayload), &item); err != nil {
			return C.CString(fmt.Sprintf("invalid item: %v", err))
		}

		if err := store.Save(ctx, item.Category, item.Name, item.Model); err != nil {
			return C.CString(err.Error())
		}

	case LoadModel:
		store, err := getStore()
		if err != nil {
			return C.CString(err.Error())
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
		store, err := getStore()
		if err != nil {
			return C.CString(err.Error())
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
		store, err := getStore()
		if err != nil {
			return C.CString(err.Error())
		}

		var item ModelItem
		if err := json.Unmarshal([]byte(jsonPayload), &item); err != nil {
			return C.CString(fmt.Sprintf("invalid item: %v", err))
		}

		if err := store.Delete(ctx, item.Category, item.Name); err != nil {
			return C.CString(err.Error())
		}

	case CloseModelDB:
		targetUUID, _ := sop.ParseUUID(targetIDStr)
		dbRegistry.Remove(targetUUID)
	}

	return nil
}
