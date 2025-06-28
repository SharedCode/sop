package main

/*
#include <stdlib.h> // For free
*/
import "C"
import (
	"context"
	"fmt"
	"time"
	"unsafe"

	log "log/slog"
	"github.com/google/uuid"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/in_red_fs"
	"github.com/SharedCode/sop/jsondb"
	"github.com/SharedCode/sop/redis"
)

//export open_redis_connection
func open_redis_connection(host *C.char, port C.int, password *C.char) *C.char {
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

//export close_redis_connection
func close_redis_connection() *C.char {
	err := redis.CloseConnection()
	if err != nil {
		errMsg := fmt.Sprintf("error encountered closing Redis connection, details: %v", err)
		log.Error(errMsg)

		// Remember to deallocate errMsg!
		return C.CString(errMsg)
	}
	return nil
}

// Transaction lookup table.
var transactionLookup map[sop.UUID]sop.Transaction = make(map[sop.UUID]sop.Transaction)

// Btree lookup tables.
var btreeLookup map[sop.UUID]*jsondb.JsonAnyKey = make(map[sop.UUID]*jsondb.JsonAnyKey)
var btreeLookupMapKey map[sop.UUID]*jsondb.JsonMapKey = make(map[sop.UUID]*jsondb.JsonMapKey)

type transactionAction int

const (
	TransactionActionUnknown = iota
	NewTransaction
	Begin
	Commit
	Rollback
)

//export manage_transaction
func manage_transaction(action C.int, payload *C.char) *C.char {
	ps := C.GoString(payload)

	extractTrans := func() (sop.Transaction, *C.char) {
		uuid, err := sop.ParseUUID(ps)
		if err != nil {
			errMsg := fmt.Sprintf("error parsing UUID, details: %v", err)
			return nil, C.CString(errMsg)
		}
		t, ok := transactionLookup[uuid]
		if !ok {
			errMsg := fmt.Sprintf("UUID %v not found", uuid.String())
			return nil, C.CString(errMsg)
		}
		return t, nil
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
		transactionLookup[tid] = t
		// Return the transction ID if succeeded.
		return C.CString(tid.String())

	case Begin:
		t, err := extractTrans()
		if err != nil {
			return err
		}
		if err := t.Begin(); err != nil {
			errMsg := fmt.Sprintf("transaction %v Begin failed, details: %v", t.GetID().String(), err)
			delete(transactionLookup, t.GetID())
			return C.CString(errMsg)
		}
	case Commit:
		t, err := extractTrans()
		if err != nil {
			return err
		}
		if err := t.Commit(ctx); err != nil {
			errMsg := fmt.Sprintf("transaction %v Commit failed, details: %v", t.GetID().String(), err)
			delete(transactionLookup, t.GetID())
			return C.CString(errMsg)
		}
		delete(transactionLookup, t.GetID())

	case Rollback:
		t, err := extractTrans()
		if err != nil {
			return err
		}
		if err := t.Rollback(ctx); err != nil {
			errMsg := fmt.Sprintf("transaction %v Rollback failed, details: %v", t.GetID().String(), err)
			delete(transactionLookup, t.GetID())
			return C.CString(errMsg)
		}
		delete(transactionLookup, t.GetID())

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
	return &so
}

type PagingDirection int

const (
	Forward = iota
	Backward
)

type PagingInfo struct {
	PageNumber int             `json:"page_number"`
	PageSize   int             `json:"page_size"`
	Direction  PagingDirection `json:"direction"`
}

// Manage Btree payload struct is used for communication between SOP language binding, e.g. Python,
// and the SOP's jsondb package each of the B-tree management action parameters including data payload.
//
// BtreeID is used to lookup the Btree from the Btree lookup table.
type ManageBtreeMetaData struct {
	IsPrimitiveKey bool      `json:"is_primitive_key"`
	BtreeID        uuid.UUID `json:"btree_id"`
}
type ManageBtreePayload struct {
	Items []jsondb.Item `json:"items"`
	// PagingInfo `json:"paging_info"`
}
type ManageBtreePayloadMapKey struct {
	Items []jsondb.ItemMapKey `json:"items"`
	//PagingInfo `json:"paging_info"`
}

//export manage_btree
func manage_btree(action C.int, payload *C.char, payload2 *C.char) *C.char {
	ps := C.GoString(payload)

	log.Info(fmt.Sprintf("at top, payload: %v", ps))

	ctx := context.Background()
	switch int(action) {
	case NewBtree:
		var b3o BtreeOptions
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &b3o); err != nil {
			// Rare for an error to occur, but do return an errMsg if it happens.
			errMsg := fmt.Sprintf("error Unmarshal BtreeOptions, details: %v", err)
			return C.CString(errMsg)
		}
		log.Debug(fmt.Sprintf("BtreeOptions: %v", b3o))
		b3id := sop.NewUUID()
		t, ok := transactionLookup[sop.UUID(b3o.TransactionID)]
		if !ok {
			errMsg := fmt.Sprintf("can't find Transaction %v", b3o.TransactionID.String())
			return C.CString(errMsg)
		}
		so := convertTo(&b3o)

		if b3o.IsPrimitiveKey {
			b3, err := jsondb.NewJsonBtree(ctx, *so, t)
			if err != nil {
				errMsg := fmt.Sprintf("error creating Btree, details: %v", err)
				return C.CString(errMsg)
			}
			btreeLookup[b3id] = b3
		} else {
			b3, err := jsondb.NewJsonMapKeyBtree(ctx, *so, t, b3o.CELexpression)
			if err != nil {
				errMsg := fmt.Sprintf("error creating Btree, details: %v", err)
				return C.CString(errMsg)
			}
			btreeLookupMapKey[b3id] = b3
		}
		// Return the Btree ID if succeeded.
		return C.CString(b3id.String())
	case OpenBtree:
		var b3o BtreeOptions
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &b3o); err != nil {
			// Rare for an error to occur, but do return an errMsg if it happens.
			errMsg := fmt.Sprintf("error Unmarshal BtreeOptions, details: %v", err)
			return C.CString(errMsg)
		}
		log.Debug(fmt.Sprintf("BtreeOptions: %v", b3o))
		b3id := sop.NewUUID()
		t, ok := transactionLookup[sop.UUID(b3o.TransactionID)]
		if !ok {
			errMsg := fmt.Sprintf("can't find Transaction %v", b3o.TransactionID.String())
			return C.CString(errMsg)
		}
		so := convertTo(&b3o)

		if b3o.IsPrimitiveKey {
			b3, err := jsondb.OpenJsonBtree(ctx, so.Name, t)
			if err != nil {
				errMsg := fmt.Sprintf("error opening Btree (%s), details: %v", so.Name, err)
				return C.CString(errMsg)
			}
			btreeLookup[b3id] = b3
		} else {
			b3, err := jsondb.OpenJsonMapKeyBtree(ctx, so.Name, t)
			if err != nil {
				errMsg := fmt.Sprintf("error opening Btree (%s), details: %v", so.Name, err)
				return C.CString(errMsg)
			}
			btreeLookupMapKey[b3id] = b3
		}
		// Return the Btree ID if succeeded.
		return C.CString(b3id.String())
	case Add:
		log.Info("entering add")
		log.Info(fmt.Sprintf("payload: %v", ps))
		var p ManageBtreeMetaData
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &p); err != nil {
			errMsg := fmt.Sprintf("error Unmarshal ManageBtreeMetaData, details: %v", err)
			return C.CString(errMsg)
		}

		if p.IsPrimitiveKey {
			b3, ok := btreeLookup[sop.UUID(p.BtreeID)]
			if !ok {
				errMsg := fmt.Sprintf("did not find B-tree(id=%v) from lookup", p.BtreeID)
				return C.CString(errMsg)
			}

			ps2 := C.GoString(payload2)
			log.Info(fmt.Sprintf("ps2: %v", ps2))
			var payload ManageBtreePayload
			if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
				errMsg := fmt.Sprintf("error Unmarshal ManageBtreePayload, details: %v", err)
				return C.CString(errMsg)
			}
			log.Info(fmt.Sprintf("items: %v", payload))
			ok, err := b3.Add(ctx, payload.Items)
			if err != nil {
				errMsg := fmt.Sprintf("error add of item to B-tree (id=%v), details: %v", p.BtreeID, err)
				return C.CString(errMsg)
			}
			return C.CString(fmt.Sprintf("%v", ok))
		} else {
			b3, ok := btreeLookupMapKey[sop.UUID(p.BtreeID)]
			if !ok {
				errMsg := fmt.Sprintf("did not find B-tree(id=%v) from lookup", p.BtreeID)
				return C.CString(errMsg)
			}

			ps2 := C.GoString(payload2)
			var payload ManageBtreePayloadMapKey
			if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
				errMsg := fmt.Sprintf("error Unmarshal ManageBtreePayload, details: %v", err)
				return C.CString(errMsg)
			}
			log.Info(fmt.Sprintf("items: %v", payload))
			ok, err := b3.Add(ctx, payload.Items)
			if err != nil {
				errMsg := fmt.Sprintf("error add of item to B-tree (id=%v), details: %v", p.BtreeID, err)
				return C.CString(errMsg)
			}
			return C.CString(fmt.Sprintf("%v", ok))
		}
	}

	return nil
}

//export free_string
func free_string(cString *C.char) {
	if cString != nil {
		C.free(unsafe.Pointer(cString))
	}
}

func main() {
	// main function is required for building a shared library, but can be empty
}
