package main

/*
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

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/in_red_fs"
	"github.com/SharedCode/sop/redis"
)

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
func manageTransaction(action C.int, payload *C.char) *C.char {
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

	ctx := context.Background()
	switch int(action) {
	case NewTransaction:
		var to in_red_fs.TransationOptionsWithReplication
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &to); err != nil {
			// Rare for an error to occur, but do return an errMsg if it happens.
			errMsg := fmt.Sprintf("error Unmarshal TransactionOptions, details: %v", err)
			return C.CString(errMsg)
		}

		// Convert Maxtime from minutes to Duration.
		to.MaxTime = to.MaxTime * time.Minute

		log.Debug(fmt.Sprintf("TransactionOptions: %v", to))
		tid := sop.NewUUID()
		t, err := in_red_fs.NewTransactionWithReplication(ctx, to)
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
		if err := t.First.Begin(); err != nil {
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
	Name                         string               `json:"name" minLength:"1" maxLength:"20"`
	SlotLength                   int                  `json:"slot_length" min:"2" max:"10000"`
	IsUnique                     bool                 `json:"is_unique"`
	Description                  string               `json:"description" maxLength:"500"`
	IsValueDataInNodeSegment     bool                 `json:"is_value_data_in_node_segment"`
	IsValueDataActivelyPersisted bool                 `json:"is_value_data_actively_persisted"`
	IsValueDataGloballyCached    bool                 `json:"is_value_data_globally_cached"`
	LeafLoadBalancing            bool                 `json:"leaf_load_balancing"`
	CacheConfig                  sop.StoreCacheConfig `json:"cache_config"`

	CELexpression  string    `json:"cel_expression"`
	TransactionID  uuid.UUID `json:"transaction_id"`
	IsPrimitiveKey bool      `json:"is_primitive_key"`
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
	so.CELexpression = si.CELexpression
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
	bo.CELexpression = si.CELexpression
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
