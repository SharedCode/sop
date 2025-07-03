package main

/*
#include <stdlib.h> // For free
*/
import "C"
import (
	"context"
	"fmt"

	"github.com/google/uuid"

	log "log/slog"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/common"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/jsondb"
)

// Manage Btree payload struct is used for communication between SOP language binding, e.g. Python,
// and the SOP's jsondb package each of the B-tree management action parameters including data payload.
//
// BtreeID is used to lookup the Btree from the Btree lookup table.
type ManageBtreeMetaData struct {
	IsPrimitiveKey bool      `json:"is_primitive_key"`
	TransactionID  uuid.UUID `json:"transaction_id"`
	BtreeID        uuid.UUID `json:"btree_id"`
}
type ManageBtreePayload[TK, TV any] struct {
	Items      []jsondb.Item[TK, TV] `json:"items"`
	PagingInfo *jsondb.PagingInfo    `json:"paging_info"`
}

//export manageBtree
func manageBtree(ctxID C.longlong, action C.int, payload *C.char, payload2 *C.char) *C.char {
	ps := C.GoString(payload)
	ctx := getContext(ctxID)
	if ctx == nil {
		return C.CString(fmt.Sprintf("context with ID %v not found", int64(ctxID)))
	}

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
		return manage(ctx, int(action), payload, payload2)
	case Remove:
		return remove(ctx, payload, payload2)
	default:
		errMsg := fmt.Sprintf("unsupported manage action(%d) of item to B-tree (unknown)", int(action))
		return C.CString(errMsg)
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
		log.Debug(fmt.Sprintf("NewBtree %s, primitiveKey: %v", b3o.Name, b3o.IsPrimitiveKey))
		b3, err := jsondb.NewJsonBtree[any, any](ctx, *so, tup.First, nil)
		if err != nil {
			errMsg := fmt.Sprintf("error creating Btree, details: %v", err)
			return C.CString(errMsg)
		}
		// Add the B-tree to the transaction btree map so it can get lookedup.
		tup.Second[b3id] = b3
	} else {
		log.Debug(fmt.Sprintf("NewBtree %s, primitiveKey: %v", b3o.Name, b3o.IsPrimitiveKey))
		b3, err := jsondb.NewJsonBtreeMapKey(ctx, *so, tup.First, b3o.IndexSpecification)
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

	// Get StoreInfo from backend DB and determine if key is primitive or not.
	intf := tup.First.(interface{})
	t2 := intf.(*sop.SinglePhaseTransaction).SopPhaseCommitTransaction
	intf = t2
	t := intf.(*common.Transaction)

	sr := t.StoreRepository
	si, err := sr.Get(ctx, so.Name)
	isPrimitiveKey := false
	if err == nil {
		isPrimitiveKey = si[0].IsPrimitiveKey
	}

	if isPrimitiveKey {
		b3, err := jsondb.OpenJsonBtree[any, any](ctx, so.Name, tup.First, nil)
		if err != nil {
			errMsg := fmt.Sprintf("error opening Btree (%s), details: %v", so.Name, err)
			return C.CString(errMsg)
		}
		ce := b3.GetStoreInfo().MapKeyIndexSpecification
		if ce != "" {
			errMsg := fmt.Sprintf("error opening for 'Primitive Type' Btree (%s), CELexpression %s is restricted for class type Key", so.Name, ce)
			log.Error(errMsg)
			return C.CString(errMsg)
		}
		tup.Second[b3id] = b3
	} else {
		b3, err := jsondb.OpenJsonBtreeMapKey(ctx, so.Name, tup.First)
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

func manage(ctx context.Context, action int, payload, payload2 *C.char) *C.char {
	p, b32, errMsg := extractMetaData(payload)
	if errMsg != nil {
		return errMsg
	}
	if b3, ok := b32.(*jsondb.JsonDBMapKey); ok {
		ps2 := C.GoString(payload2)
		var payload ManageBtreePayload[map[string]any, any]
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
		b3, ok := b32.(*jsondb.JsonDBAnyKey[any, any])
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return C.CString(errMsg)
		}

		ps2 := C.GoString(payload2)
		var payload ManageBtreePayload[any, any]
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

func remove(ctx context.Context, payload, payload2 *C.char) *C.char {
	p, b32, errMsg := extractMetaData(payload)
	if errMsg != nil {
		return errMsg
	}
	if b3, ok := b32.(*jsondb.JsonDBMapKey); ok {
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
	} else {
		b3, ok := b32.(*jsondb.JsonDBAnyKey[any, any])
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
	}
}
