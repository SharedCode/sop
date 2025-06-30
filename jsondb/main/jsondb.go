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
	"github.com/SharedCode/sop/jsondb"
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
	return &so
}

// Manage Btree payload struct is used for communication between SOP language binding, e.g. Python,
// and the SOP's jsondb package each of the B-tree management action parameters including data payload.
//
// BtreeID is used to lookup the Btree from the Btree lookup table.
type ManageBtreeMetaData struct {
	IsPrimitiveKey bool      `json:"is_primitive_key"`
	TransactionID  uuid.UUID `json:"transaction_id"`
	BtreeID        uuid.UUID `json:"btree_id"`
}
type ManageBtreePayload struct {
	Items      []jsondb.Item      `json:"items"`
	PagingInfo *jsondb.PagingInfo `json:"paging_info"`
}
type ManageBtreePayloadMapKey struct {
	Items      []jsondb.ItemMapKey `json:"items"`
	PagingInfo *jsondb.PagingInfo  `json:"paging_info"`
}

//export manageBtree
func manageBtree(action C.int, payload *C.char, payload2 *C.char) *C.char {
	ps := C.GoString(payload)
	ctx := context.Background()

	switch int(action) {
	case NewBtree:
		return newBtree(ctx, ps)
	case OpenBtree:
		return openBtree(ctx, ps)
	case Add:
		fallthrough
	case AddIfNotExist:
		fallthrough
	case Upsert:
		fallthrough
	case Update:
		return manage(ctx, int(action), ps, payload2)
	case Remove:
		return remove(ctx, ps, payload2)
	default:
		errMsg := fmt.Sprintf("unsupported manage action(%d) of item to B-tree (unknown)", int(action))
		return C.CString(errMsg)
	}
}

//export getFromBtree
func getFromBtree(action C.int, payload *C.char, payload2 *C.char) (*C.char, *C.char) {
	ps := C.GoString(payload)
	ctx := context.Background()

	switch int(action) {
	case GetItems:
		return getItems(ctx, ps, payload2)
	default:
		errMsg := fmt.Sprintf("unsupported manage action(%d) of item to B-tree (unknown)", int(action))
		return nil, C.CString(errMsg)
	}
}

//export freeString
func freeString(cString *C.char) {
	if cString != nil {
		C.free(unsafe.Pointer(cString))
	}
}

func newBtree(ctx context.Context, ps string) *C.char {
	var b3o BtreeOptions
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &b3o); err != nil {
		// Rare for an error to occur, but do return an errMsg if it happens.
		errMsg := fmt.Sprintf("error Unmarshal BtreeOptions, details: %v", err)
		return C.CString(errMsg)
	}
	log.Debug(fmt.Sprintf("BtreeOptions: %v", b3o))
	b3id := sop.NewUUID()

	transactionLookupLocker.Lock()
	tup, ok := transactionLookup[sop.UUID(b3o.TransactionID)]
	transactionLookupLocker.Unlock()

	if !ok {
		errMsg := fmt.Sprintf("can't find Transaction %v", b3o.TransactionID.String())
		return C.CString(errMsg)
	}
	so := convertTo(&b3o)

	if b3o.IsPrimitiveKey {
		b3, err := jsondb.NewJsonBtree(ctx, *so, tup.First)
		if err != nil {
			errMsg := fmt.Sprintf("error creating Btree, details: %v", err)
			return C.CString(errMsg)
		}
		// Add the B-tree to the transaction btree map so it can get lookedup.
		tup.Second[b3id] = b3
	} else {
		b3, err := jsondb.NewJsonMapKeyBtree(ctx, *so, tup.First, b3o.CELexpression)
		if err != nil {
			errMsg := fmt.Sprintf("error creating Btree, details: %v", err)
			return C.CString(errMsg)
		}
		// Add the B-tree to the transaction btree map so it can get lookedup.
		tup.Second[b3id] = b3
	}

	transactionLookupLocker.Lock()
	transactionLookup[tup.First.GetID()] = tup
	transactionLookupLocker.Unlock()

	// Return the Btree ID if succeeded.
	return C.CString(b3id.String())
}

func openBtree(ctx context.Context, ps string) *C.char {
	var b3o BtreeOptions
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &b3o); err != nil {
		// Rare for an error to occur, but do return an errMsg if it happens.
		errMsg := fmt.Sprintf("error Unmarshal BtreeOptions, details: %v", err)
		return C.CString(errMsg)
	}
	log.Debug(fmt.Sprintf("BtreeOptions: %v", b3o))
	b3id := sop.NewUUID()

	transactionLookupLocker.Lock()
	tup, ok := transactionLookup[sop.UUID(b3o.TransactionID)]
	transactionLookupLocker.Unlock()

	if !ok {
		errMsg := fmt.Sprintf("can't find Transaction %v", b3o.TransactionID.String())
		return C.CString(errMsg)
	}
	so := convertTo(&b3o)

	if b3o.IsPrimitiveKey {
		b3, err := jsondb.OpenJsonBtree(ctx, so.Name, tup.First)
		if err != nil {
			errMsg := fmt.Sprintf("error opening Btree (%s), details: %v", so.Name, err)
			return C.CString(errMsg)
		}
		tup.Second[b3id] = b3
	} else {
		b3, err := jsondb.OpenJsonMapKeyBtree(ctx, so.Name, tup.First)
		if err != nil {
			errMsg := fmt.Sprintf("error opening Btree (%s), details: %v", so.Name, err)
			return C.CString(errMsg)
		}
		tup.Second[b3id] = b3
	}

	transactionLookupLocker.Lock()
	transactionLookup[tup.First.GetID()] = tup
	transactionLookupLocker.Unlock()

	// Return the Btree ID if succeeded.
	return C.CString(b3id.String())
}

func manage(ctx context.Context, action int, ps string, payload2 *C.char) *C.char {
	var p ManageBtreeMetaData
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &p); err != nil {
		errMsg := fmt.Sprintf("error Unmarshal ManageBtreeMetaData, details: %v", err)
		return C.CString(errMsg)
	}

	if p.IsPrimitiveKey {
		tup, ok := transactionLookup[sop.UUID(p.TransactionID)]
		if !ok {
			errMsg := fmt.Sprintf("did not find Transaction(id=%v) from lookup", p.TransactionID)
			return C.CString(errMsg)
		}
		b32, ok := tup.Second[sop.UUID(p.BtreeID)]
		if !ok {
			errMsg := fmt.Sprintf("did not find B-tree(id=%v) from lookup", p.BtreeID)
			return C.CString(errMsg)
		}
		b3, ok := b32.(*jsondb.JsonAnyKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return C.CString(errMsg)
		}

		ps2 := C.GoString(payload2)
		var payload ManageBtreePayload
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
			errMsg := fmt.Sprintf("error Unmarshal ManageBtreePayload, details: %v", err)
			return C.CString(errMsg)
		}

		var err error
		switch action {
		case Add:
			ok, err = b3.Add(ctx, payload.Items)
		case AddIfNotExist:
			ok, err = b3.AddIfNotExist(ctx, payload.Items)
		case Update:
			ok, err = b3.Update(ctx, payload.Items)
		case Upsert:
			ok, err = b3.Upsert(ctx, payload.Items)
		default:
			errMsg := fmt.Sprintf("unsupported manage action(%d) of item to B-tree (id=%v)", action, p.BtreeID)
			return C.CString(errMsg)
		}

		if err != nil {
			errMsg := fmt.Sprintf("error manage of item to B-tree (id=%v), details: %v", p.BtreeID, err)
			return C.CString(errMsg)
		}
		return C.CString(fmt.Sprintf("%v", ok))
	} else {
		tup, ok := transactionLookup[sop.UUID(p.TransactionID)]
		if !ok {
			errMsg := fmt.Sprintf("did not find Transaction(id=%v) from lookup", p.TransactionID)
			return C.CString(errMsg)
		}
		b32, ok := tup.Second[sop.UUID(p.BtreeID)]
		if !ok {
			errMsg := fmt.Sprintf("did not find B-tree(id=%v) from lookup", p.BtreeID)
			return C.CString(errMsg)
		}

		b3, ok := b32.(*jsondb.JsonMapKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return C.CString(errMsg)
		}

		ps2 := C.GoString(payload2)
		var payload ManageBtreePayloadMapKey
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
			errMsg := fmt.Sprintf("error Unmarshal ManageBtreePayload, details: %v", err)
			return C.CString(errMsg)
		}
		var err error
		switch action {
		case Add:
			ok, err = b3.Add(ctx, payload.Items)
		case AddIfNotExist:
			ok, err = b3.AddIfNotExist(ctx, payload.Items)
		case Update:
			ok, err = b3.Update(ctx, payload.Items)
		case Upsert:
			ok, err = b3.Upsert(ctx, payload.Items)
		default:
			errMsg := fmt.Sprintf("unsupported manage action(%d) of item to B-tree (id=%v)", action, p.BtreeID)
			return C.CString(errMsg)
		}

		if err != nil {
			errMsg := fmt.Sprintf("error manage of item to B-tree (id=%v), details: %v", p.BtreeID, err)
			return C.CString(errMsg)
		}
		return C.CString(fmt.Sprintf("%v", ok))
	}
}

func remove(ctx context.Context, ps string, payload2 *C.char) *C.char {
	var p ManageBtreeMetaData
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &p); err != nil {
		errMsg := fmt.Sprintf("error Unmarshal ManageBtreeMetaData, details: %v", err)
		return C.CString(errMsg)
	}

	if p.IsPrimitiveKey {
		tup, ok := transactionLookup[sop.UUID(p.TransactionID)]
		if !ok {
			errMsg := fmt.Sprintf("did not find Transaction(id=%v) from lookup", p.TransactionID)
			return C.CString(errMsg)
		}
		b32, ok := tup.Second[sop.UUID(p.BtreeID)]
		if !ok {
			errMsg := fmt.Sprintf("did not find B-tree(id=%v) from lookup", p.BtreeID)
			return C.CString(errMsg)
		}

		b3, ok := b32.(*jsondb.JsonAnyKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return C.CString(errMsg)
		}

		ps2 := C.GoString(payload2)
		var payload []any
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
			errMsg := fmt.Sprintf("error Unmarshal keys array, details: %v", err)
			return C.CString(errMsg)
		}
		ok, err := b3.Remove(ctx, payload)
		if err != nil {
			errMsg := fmt.Sprintf("error remove of item from B-tree (id=%v), details: %v", p.BtreeID, err)
			return C.CString(errMsg)
		}
		return C.CString(fmt.Sprintf("%v", ok))
	} else {
		tup, ok := transactionLookup[sop.UUID(p.TransactionID)]
		if !ok {
			errMsg := fmt.Sprintf("did not find Transaction(id=%v) from lookup", p.TransactionID)
			return C.CString(errMsg)
		}
		b32, ok := tup.Second[sop.UUID(p.BtreeID)]
		if !ok {
			errMsg := fmt.Sprintf("did not find B-tree(id=%v) from lookup", p.BtreeID)
			return C.CString(errMsg)
		}

		b3, ok := b32.(*jsondb.JsonMapKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return C.CString(errMsg)
		}

		ps2 := C.GoString(payload2)
		var payload []map[string]any
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
			errMsg := fmt.Sprintf("error Unmarshal keys array, details: %v", err)
			return C.CString(errMsg)
		}
		var err error
		ok, err = b3.Remove(ctx, payload)
		if err != nil {
			errMsg := fmt.Sprintf("error remove of item from B-tree (id=%v), details: %v", p.BtreeID, err)
			return C.CString(errMsg)
		}
		return C.CString(fmt.Sprintf("%v", ok))
	}
}

func main() {
	// main function is required for building a shared library, but can be empty
}
