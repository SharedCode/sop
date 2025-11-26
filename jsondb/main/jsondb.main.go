package main

/*
#include <stdio.h> // C.longlong
#include <stdlib.h> // For free
*/
import "C"
import (
	"context"
	"fmt"
	"sync"
	"time"
	"unsafe"

	log "log/slog"

	"github.com/google/uuid"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/inredfs"
	"github.com/sharedcode/sop/redis"
)

var contextLookup map[int64]context.Context = make(map[int64]context.Context)
var contextLookupLocker sync.Mutex
var contextLastID int64

// Context related API to allow Python code to be able to get access to the context objects and thus,
// allow things like cancellation if needed.

//export createContext
func createContext() C.longlong {
	ctx := context.Background()
	contextLookupLocker.Lock()
	contextLastID++
	id := contextLastID

	contextLookup[id] = ctx
	contextLookupLocker.Unlock()
	return C.longlong(id)
}

//export cancelContext
func cancelContext(ctxID C.longlong) {
	id := int64(ctxID)
	contextLookupLocker.Lock()

	ctx, ok := contextLookup[id]
	var c context.CancelFunc
	if ok {
		_, c = context.WithCancel(ctx)
	}
	delete(contextLookup, id)
	contextLookupLocker.Unlock()

	// Call the cancel function for the ctx context.
	if c != nil {
		c()
	}
}

//export removeContext
func removeContext(ctxID C.longlong) {
	id := int64(ctxID)
	contextLookupLocker.Lock()
	delete(contextLookup, id)
	contextLookupLocker.Unlock()
}

// Private get context for use internally here.
func getContext(ctxID C.longlong) context.Context {
	contextLookupLocker.Lock()
	ctx := contextLookup[int64(ctxID)]
	contextLookupLocker.Unlock()
	return ctx
}

// Redis global connection management related.
//
//export openRedisConnection
func openRedisConnection(host *C.char, port C.int, password *C.char) *C.char {
	_, err := redis.OpenConnection(redis.Options{
		Address:  fmt.Sprintf("%s:%d", C.GoString(host), int(port)),
		DB:       0,
		Password: C.GoString(password),
	})
	if err != nil {
		errMsg := fmt.Sprintf("error encountered opening Redis connection, details: %v", err)
		log.Error(errMsg)

		// Remember to deallocate errInfo.message!
		return C.CString(errMsg)
	}
	return nil
}

//export closeRedisConnection
func closeRedisConnection() *C.char {
	err := redis.CloseConnection()
	if err != nil {
		errMsg := fmt.Sprintf("error encountered closing Redis connection, details: %v", err)
		log.Error(errMsg)

		// Remember to deallocate errMsg!
		return C.CString(errMsg)
	}
	return nil
}

// Transaction management related.

// Transaction lookup table is comprised of the transaction & its related B-trees.
var transactionLookup map[sop.UUID]sop.Tuple[sop.Transaction, map[sop.UUID]any] = make(map[sop.UUID]sop.Tuple[sop.Transaction, map[sop.UUID]any])
var transactionLookupLocker sync.Mutex

type transactionAction int

const (
	TransactionActionUnknown = iota
	NewTransaction
	Begin
	Commit
	Rollback
)

//export manageTransaction
func manageTransaction(ctxID C.longlong, action C.int, payload *C.char) *C.char {
	ps := C.GoString(payload)

	extractTrans := func() (*sop.Tuple[sop.Transaction, map[sop.UUID]any], *C.char) {
		uuid, err := sop.ParseUUID(ps)
		if err != nil {
			errMsg := fmt.Sprintf("error parsing UUID, details: %v", err)
			return nil, C.CString(errMsg)
		}

		transactionLookupLocker.Lock()
		tup, ok := transactionLookup[uuid]
		transactionLookupLocker.Unlock()

		if !ok {
			errMsg := fmt.Sprintf("UUID %v not found", uuid.String())
			return nil, C.CString(errMsg)
		}
		return &tup, nil
	}

	var ctx context.Context
	if int64(ctxID) > 0 {
		ctx = getContext(ctxID)
		if ctx == nil {
			return C.CString(fmt.Sprintf("context with ID %v not found", int64(ctxID)))
		}
	}
	switch int(action) {
	case NewTransaction:
		var to inredfs.TransationOptionsWithReplication
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &to); err != nil {
			// Rare for an error to occur, but do return an errMsg if it happens.
			errMsg := fmt.Sprintf("error Unmarshal TransactionOptions, details: %v", err)
			return C.CString(errMsg)
		}

		// Convert Maxtime from minutes to Duration.
		to.MaxTime = to.MaxTime * time.Minute

		log.Debug(fmt.Sprintf("TransactionOptions: %v", to))
		tid := sop.NewUUID()
		t, err := inredfs.NewTransactionWithReplication(ctx, to)
		if err != nil {
			errMsg := fmt.Sprintf("error creating a Transaction, details: %v", err)
			return C.CString(errMsg)
		}
		transactionLookupLocker.Lock()
		transactionLookup[tid] = sop.Tuple[sop.Transaction, map[sop.UUID]any]{First: t, Second: map[sop.UUID]any{}}
		transactionLookupLocker.Unlock()

		// Return the transction ID if succeeded.
		return C.CString(tid.String())

	case Begin:
		t, err := extractTrans()
		if err != nil {
			return err
		}
		if err := t.First.Begin(ctx); err != nil {
			errMsg := fmt.Sprintf("transaction %v Begin failed, details: %v", t.First.GetID().String(), err)

			transactionLookupLocker.Lock()
			delete(transactionLookup, t.First.GetID())
			transactionLookupLocker.Unlock()

			return C.CString(errMsg)
		}
	case Commit:
		t, err := extractTrans()
		if err != nil {
			return err
		}

		if err := t.First.Commit(ctx); err != nil {
			errMsg := fmt.Sprintf("transaction %v Commit failed, details: %v", t.First.GetID().String(), err)
			transactionLookupLocker.Lock()
			delete(transactionLookup, t.First.GetID())
			transactionLookupLocker.Unlock()
			return C.CString(errMsg)
		}

		transactionLookupLocker.Lock()
		delete(transactionLookup, t.First.GetID())
		transactionLookupLocker.Unlock()

	case Rollback:
		t, err := extractTrans()
		if err != nil {
			return err
		}

		if err := t.First.Rollback(ctx); err != nil {
			errMsg := fmt.Sprintf("transaction %v Rollback failed, details: %v", t.First.GetID().String(), err)
			transactionLookupLocker.Lock()
			delete(transactionLookup, t.First.GetID())
			transactionLookupLocker.Unlock()
			return C.CString(errMsg)
		}

		transactionLookupLocker.Lock()
		delete(transactionLookup, t.First.GetID())
		transactionLookupLocker.Unlock()

	default:
		errMsg := fmt.Sprintf("unsupported action %d", int(action))
		return C.CString(errMsg)
	}
	return nil
}

// Some B-tree related artifacts.

type btreeAction int

const (
	BtreeActionUnknown = iota
	NewBtree
	OpenBtree
	Add
	AddIfNotExist
	Update
	Upsert
	Remove
	Find
	FindWithID
	GetItems
	GetValues
	GetKeys
	First
	Last
	IsUnique
	Count
	GetStoreInfo
)

// BtreeOptions is used to package the Btree StoreInfo.
type BtreeOptions struct {
	Name                         string               `json:"name" minLength:"1" maxLength:"128"`
	SlotLength                   int                  `json:"slot_length" min:"2" max:"10000"`
	IsUnique                     bool                 `json:"is_unique"`
	Description                  string               `json:"description" maxLength:"500"`
	IsValueDataInNodeSegment     bool                 `json:"is_value_data_in_node_segment"`
	IsValueDataActivelyPersisted bool                 `json:"is_value_data_actively_persisted"`
	IsValueDataGloballyCached    bool                 `json:"is_value_data_globally_cached"`
	LeafLoadBalancing            bool                 `json:"leaf_load_balancing"`
	CacheConfig                  sop.StoreCacheConfig `json:"cache_config"`

	IndexSpecification string    `json:"index_specification"`
	TransactionID      uuid.UUID `json:"transaction_id"`
	IsPrimitiveKey     bool      `json:"is_primitive_key"`
}

// Extract from StoreInfo. StoreInfo has annotations useful for JSON serialization, thus, it can get used
// packaging from compatible client's JSON, could be written on other languages.
func convertTo(si *BtreeOptions) *sop.StoreOptions {
	so := sop.StoreOptions{}
	so.Name = si.Name
	so.SlotLength = si.SlotLength
	so.IsUnique = si.IsUnique
	so.IsValueDataActivelyPersisted = si.IsValueDataActivelyPersisted
	so.IsValueDataGloballyCached = si.IsValueDataGloballyCached
	so.IsValueDataInNodeSegment = si.IsValueDataInNodeSegment
	so.LeafLoadBalancing = si.LeafLoadBalancing
	so.Description = si.Description
	so.CacheConfig = &si.CacheConfig
	// Adjust the Durations from "minute" unit into proper time.Duration values.
	so.CacheConfig.NodeCacheDuration = so.CacheConfig.NodeCacheDuration * time.Minute
	so.CacheConfig.RegistryCacheDuration = so.CacheConfig.RegistryCacheDuration * time.Minute
	so.CacheConfig.StoreInfoCacheDuration = so.CacheConfig.StoreInfoCacheDuration * time.Minute
	so.CacheConfig.ValueDataCacheDuration = so.CacheConfig.ValueDataCacheDuration * time.Minute
	so.CELexpression = si.IndexSpecification
	so.IsPrimitiveKey = si.IsPrimitiveKey
	return &so
}
func (bo *BtreeOptions) extract(si *sop.StoreInfo) {
	bo.Name = si.Name
	bo.SlotLength = si.SlotLength
	bo.IsUnique = si.IsUnique
	bo.IsValueDataActivelyPersisted = si.IsValueDataActivelyPersisted
	bo.IsValueDataGloballyCached = si.IsValueDataGloballyCached
	bo.IsValueDataInNodeSegment = si.IsValueDataInNodeSegment
	bo.LeafLoadBalancing = si.LeafLoadBalancing
	bo.Description = si.Description
	bo.CacheConfig = si.CacheConfig
	bo.IndexSpecification = si.MapKeyIndexSpecification
	bo.IsPrimitiveKey = si.IsPrimitiveKey
	// Restore back to "minute" unit.
	bo.CacheConfig.NodeCacheDuration = si.CacheConfig.NodeCacheDuration / time.Minute
	bo.CacheConfig.RegistryCacheDuration = si.CacheConfig.RegistryCacheDuration / time.Minute
	bo.CacheConfig.StoreInfoCacheDuration = si.CacheConfig.StoreInfoCacheDuration / time.Minute
	bo.CacheConfig.ValueDataCacheDuration = si.CacheConfig.ValueDataCacheDuration / time.Minute
}

//export freeString
func freeString(cString *C.char) {
	if cString != nil {
		C.free(unsafe.Pointer(cString))
	}
}

func main() {
	// main function is required for building a shared library, but can be empty
}
