package main

/*
#include <stdlib.h> // For free
*/
import "C"
import (
	"context"
	"fmt"
	"unsafe"

	log "log/slog"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/in_red_fs"
	"github.com/SharedCode/sop/jsondb"
	"github.com/SharedCode/sop/redis"
)

//export free_string
func free_string(cString *C.char) {
	if cString != nil {
		C.free(unsafe.Pointer(cString))
	}
}

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

var transactionLookup map[sop.UUID]sop.Transaction = make(map[sop.UUID]sop.Transaction)
var btreeLookup map[sop.UUID]*jsondb.JsonStringWrapper = make(map[sop.UUID]*jsondb.JsonStringWrapper)

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
	Open
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

//export manage_btree
func manage_btree(action C.int, payload *C.char) *C.char {
	return nil
}

func main() {
	// main function is required for building a shared library, but can be empty
}
