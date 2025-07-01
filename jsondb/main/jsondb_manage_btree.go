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
type ManageBtreePayload struct {
	Items      []jsondb.Item[any, any] `json:"items"`
	PagingInfo *jsondb.PagingInfo      `json:"paging_info"`
}
type ManageBtreePayloadMapKey struct {
	Items      []jsondb.Item[map[string]any, any] `json:"items"`
	PagingInfo *jsondb.PagingInfo                 `json:"paging_info"`
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
		b3, err := jsondb.NewJsonBtree[any, any](ctx, *so, tup.First, nil)
		if err != nil {
			errMsg := fmt.Sprintf("error creating Btree, details: %v", err)
			return C.CString(errMsg)
		}
		// Add the B-tree to the transaction btree map so it can get lookedup.
		tup.Second[b3id] = b3
	} else {
		b3, err := jsondb.NewJsonBtreeMapKey(ctx, *so, tup.First, b3o.CELexpression)
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
		b3, err := jsondb.OpenJsonBtree[any, any](ctx, so.Name, tup.First, nil)
		if err != nil {
			errMsg := fmt.Sprintf("error opening Btree (%s), details: %v", so.Name, err)
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

func manage(ctx context.Context, action int, ps string, payload2 *C.char) *C.char {
	var p ManageBtreeMetaData
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &p); err != nil {
		errMsg := fmt.Sprintf("error Unmarshal ManageBtreeMetaData, details: %v", err)
		return C.CString(errMsg)
	}

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
	if p.IsPrimitiveKey {
		b3, ok := b32.(*jsondb.JsonDBAnyKey[any, any])
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
		b3, ok := b32.(*jsondb.JsonDBMapKey)
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
	if p.IsPrimitiveKey {
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
	} else {
		b3, ok := b32.(*jsondb.JsonDBMapKey)
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
