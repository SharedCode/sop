package main

import "C"
import (
	"context"
	"fmt"

	log "log/slog"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/jsondb"
)

//export navigateBtree
func navigateBtree(action C.int, payload *C.char, payload2 *C.char) *C.char {
	ps := C.GoString(payload)
	ctx := context.Background()

	switch int(action) {
	case First:
		fallthrough
	case Last:
		return moveTo(ctx, int(action), ps)
	case Find:
		fallthrough
	case FindWithID:
		return find(ctx, ps, payload2)
	default:
		errMsg := fmt.Sprintf("unsupported manage action(%d) of item to B-tree (unknown)", int(action))
		return C.CString(errMsg)
	}
}

//export isUniqueBtree
func isUniqueBtree(payload *C.char) *C.char {
	ps := C.GoString(payload)
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
		b3, ok := b32.(*jsondb.JsonAnyKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return C.CString(errMsg)
		}

		ok = b3.IsUnique()
		return C.CString(fmt.Sprintf("%v", ok))
	} else {
		b3, ok := b32.(*jsondb.JsonMapKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return C.CString(errMsg)
		}

		ok = b3.IsUnique()
		return C.CString(fmt.Sprintf("%v", ok))
	}
}

//export getFromBtree
func getFromBtree(action C.int, payload *C.char, payload2 *C.char) (*C.char, *C.char) {
	ps := C.GoString(payload)

	// GetStoreInfo does not need Context.
	if action == GetStoreInfo {
		return getStoreInfo(ps)
	}

	ctx := context.Background()

	switch int(action) {
	case GetKeys:
		fallthrough
	case GetItems:
		return get(ctx, int(action), ps, payload2)
	case GetValues:
		return getValues(ctx, ps, payload2)
	default:
		errMsg := fmt.Sprintf("unsupported manage action(%d) of item to B-tree (unknown)", int(action))
		return nil, C.CString(errMsg)
	}
}

//export getBtreeItemCount
func getBtreeItemCount(payload *C.char) (C.long, *C.char) {
	ps := C.GoString(payload)

	var p ManageBtreeMetaData
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &p); err != nil {
		errMsg := fmt.Sprintf("error Unmarshal ManageBtreeMetaData, details: %v", err)
		return 0, C.CString(errMsg)
	}

	tup, ok := transactionLookup[sop.UUID(p.TransactionID)]
	if !ok {
		errMsg := fmt.Sprintf("did not find Transaction(id=%v) from lookup", p.TransactionID)
		return 0, C.CString(errMsg)
	}
	b32, ok := tup.Second[sop.UUID(p.BtreeID)]
	if !ok {
		errMsg := fmt.Sprintf("did not find B-tree(id=%v) from lookup", p.BtreeID)
		return 0, C.CString(errMsg)
	}
	if p.IsPrimitiveKey {
		b3, ok := b32.(*jsondb.JsonAnyKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return 0, C.CString(errMsg)
		}

		res := b3.Count()
		return C.long(res), nil
	} else {
		b3, ok := b32.(*jsondb.JsonMapKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return 0, C.CString(errMsg)
		}

		res := b3.Count()
		return C.long(res), nil
	}
}

func getStoreInfo(ps string) (*C.char, *C.char) {
	var p ManageBtreeMetaData
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &p); err != nil {
		errMsg := fmt.Sprintf("error Unmarshal ManageBtreeMetaData, details: %v", err)
		return nil, C.CString(errMsg)
	}

	tup, ok := transactionLookup[sop.UUID(p.TransactionID)]
	if !ok {
		errMsg := fmt.Sprintf("did not find Transaction(id=%v) from lookup", p.TransactionID)
		return nil, C.CString(errMsg)
	}
	b32, ok := tup.Second[sop.UUID(p.BtreeID)]
	if !ok {
		errMsg := fmt.Sprintf("did not find B-tree(id=%v) from lookup", p.BtreeID)
		return nil, C.CString(errMsg)
	}
	if p.IsPrimitiveKey {
		b3, ok := b32.(*jsondb.JsonAnyKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return nil, C.CString(errMsg)
		}

		si := b3.GetStoreInfo()
		bo := BtreeOptions{}
		bo.extract(&si)
		bo.TransactionID = p.TransactionID
		ba, err := encoding.DefaultMarshaler.Marshal(bo)
		if err != nil {
			// Should not happen but in case.
			return nil, C.CString(err.Error())
		}
		return C.CString(string(ba)), nil
	} else {
		b3, ok := b32.(*jsondb.JsonMapKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return nil, C.CString(errMsg)
		}

		si := b3.GetStoreInfo()
		bo := BtreeOptions{}
		bo.extract(&si)
		bo.TransactionID = p.TransactionID
		ba, err := encoding.DefaultMarshaler.Marshal(bo)
		if err != nil {
			// Should not happen but in case.
			return nil, C.CString(err.Error())
		}
		return C.CString(string(ba)), nil
	}
}

func get(ctx context.Context, getAction int, ps string, payload2 *C.char) (*C.char, *C.char) {
	var p ManageBtreeMetaData
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &p); err != nil {
		errMsg := fmt.Sprintf("error Unmarshal ManageBtreeMetaData, details: %v", err)
		return nil, C.CString(errMsg)
	}

	tup, ok := transactionLookup[sop.UUID(p.TransactionID)]
	if !ok {
		errMsg := fmt.Sprintf("did not find Transaction(id=%v) from lookup", p.TransactionID)
		return nil, C.CString(errMsg)
	}
	b32, ok := tup.Second[sop.UUID(p.BtreeID)]
	if !ok {
		errMsg := fmt.Sprintf("did not find B-tree(id=%v) from lookup", p.BtreeID)
		return nil, C.CString(errMsg)
	}
	if p.IsPrimitiveKey {
		b3, ok := b32.(*jsondb.JsonAnyKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return nil, C.CString(errMsg)
		}

		ps2 := C.GoString(payload2)
		var payload jsondb.PagingInfo
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
			errMsg := fmt.Sprintf("error Unmarshal keys array, details: %v", err)
			return nil, C.CString(errMsg)
		}
		var res string
		var err error
		switch getAction {
		case GetKeys:
			res, err = b3.GetKeys(ctx, payload)
		case GetItems:
			res, err = b3.GetItems(ctx, payload)
		}
		if err != nil {
			errMsg := fmt.Sprintf("error get objects from B-tree (id=%v), details: %v", p.BtreeID, err)
			return nil, C.CString(errMsg)
		}
		return C.CString(res), nil
	} else {
		b3, ok := b32.(*jsondb.JsonMapKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return nil, C.CString(errMsg)
		}

		ps2 := C.GoString(payload2)
		var payload jsondb.PagingInfo
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
			errMsg := fmt.Sprintf("error Unmarshal keys array, details: %v", err)
			return nil, C.CString(errMsg)
		}
		var err error
		var res string
		switch getAction {
		case GetKeys:
			res, err = b3.GetKeys(ctx, payload)
		case GetItems:
			res, err = b3.GetItems(ctx, payload)
		}
		if err != nil {
			errMsg := fmt.Sprintf("error get objects from B-tree (id=%v), details: %v", p.BtreeID, err)
			return nil, C.CString(errMsg)
		}
		return C.CString(res), nil
	}
}

func getValues(ctx context.Context, ps string, payload2 *C.char) (*C.char, *C.char) {
	var p ManageBtreeMetaData
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &p); err != nil {
		errMsg := fmt.Sprintf("error Unmarshal ManageBtreeMetaData, details: %v", err)
		return nil, C.CString(errMsg)
	}

	tup, ok := transactionLookup[sop.UUID(p.TransactionID)]
	if !ok {
		errMsg := fmt.Sprintf("did not find Transaction(id=%v) from lookup", p.TransactionID)
		return nil, C.CString(errMsg)
	}
	b32, ok := tup.Second[sop.UUID(p.BtreeID)]
	if !ok {
		errMsg := fmt.Sprintf("did not find B-tree(id=%v) from lookup", p.BtreeID)
		return nil, C.CString(errMsg)
	}
	if p.IsPrimitiveKey {
		b3, ok := b32.(*jsondb.JsonAnyKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return nil, C.CString(errMsg)
		}

		ps2 := C.GoString(payload2)
		var payload ManageBtreePayload
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
			errMsg := fmt.Sprintf("error Unmarshal keys array, details: %v", err)
			return nil, C.CString(errMsg)
		}
		res, err := b3.GetValues(ctx, payload.Items)
		if err != nil {
			errMsg := fmt.Sprintf("error get objects from B-tree (id=%v), details: %v", p.BtreeID, err)
			return nil, C.CString(errMsg)
		}
		return C.CString(res), nil
	} else {
		b3, ok := b32.(*jsondb.JsonMapKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return nil, C.CString(errMsg)
		}

		ps2 := C.GoString(payload2)
		var payload ManageBtreePayloadMapKey
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
			errMsg := fmt.Sprintf("error Unmarshal keys array, details: %v", err)
			return nil, C.CString(errMsg)
		}
		res, err := b3.GetValues(ctx, payload.Items)
		if err != nil {
			errMsg := fmt.Sprintf("error get objects from B-tree (id=%v), details: %v", p.BtreeID, err)
			return nil, C.CString(errMsg)
		}
		return C.CString(res), nil
	}
}

func find(ctx context.Context, ps string, payload2 *C.char) *C.char {
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
		b3, ok := b32.(*jsondb.JsonAnyKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return C.CString(errMsg)
		}

		ps2 := C.GoString(payload2)

		log.Info(fmt.Sprintf("Payload: %v", ps2))

		var payload ManageBtreePayload
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
			errMsg := fmt.Sprintf("error Unmarshal keys array, details: %v", err)
			return C.CString(errMsg)
		}
		var err error
		id := sop.UUID(payload.Items[0].ID)
		if id.IsNil() {
			ok, err = b3.Find(ctx, payload.Items[0].Key, true)
		} else {
			ok, err = b3.FindWithID(ctx, payload.Items[0].Key, id)
		}
		if err != nil {
			errMsg := fmt.Sprintf("error find from B-tree (id=%v), details: %v", p.BtreeID, err)
			return C.CString(errMsg)
		}
		return C.CString(fmt.Sprintf("%v", ok))
	} else {
		b3, ok := b32.(*jsondb.JsonMapKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return C.CString(errMsg)
		}

		ps2 := C.GoString(payload2)
		var payload ManageBtreePayloadMapKey
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps2), &payload); err != nil {
			errMsg := fmt.Sprintf("error Unmarshal keys array, details: %v", err)
			return C.CString(errMsg)
		}
		var err error
		id := sop.UUID(payload.Items[0].ID)
		if id.IsNil() {
			ok, err = b3.Find(ctx, payload.Items[0].Key, true)
		} else {
			ok, err = b3.FindWithID(ctx, payload.Items[0].Key, id)
		}
		if err != nil {
			errMsg := fmt.Sprintf("error find with ID from B-tree (id=%v), details: %v", p.BtreeID, err)
			return C.CString(errMsg)
		}
		return C.CString(fmt.Sprintf("%v", ok))
	}
}

func moveTo(ctx context.Context, action int, ps string) *C.char {
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
		b3, ok := b32.(*jsondb.JsonAnyKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return C.CString(errMsg)
		}

		var err error
		switch action {
		case First:
			ok, err = b3.First(ctx)
			if err != nil {
				errMsg := fmt.Sprintf("error moving cursor to First item of B-tree (id=%v), details: %v", p.BtreeID, err)
				return C.CString(errMsg)
			}
		case Last:
			ok, err = b3.Last(ctx)
			if err != nil {
				errMsg := fmt.Sprintf("error moving cursor to Last item of B-tree (id=%v), details: %v", p.BtreeID, err)
				return C.CString(errMsg)
			}
		}
		return C.CString(fmt.Sprintf("%v", ok))
	} else {
		b3, ok := b32.(*jsondb.JsonMapKey)
		if !ok {
			errMsg := fmt.Sprintf("found B-tree(id=%v) from lookup is of wrong type", p.BtreeID)
			return C.CString(errMsg)
		}

		var err error
		switch action {
		case First:
			ok, err = b3.First(ctx)
			if err != nil {
				errMsg := fmt.Sprintf("error moving cursor to First item of B-tree (id=%v), details: %v", p.BtreeID, err)
				return C.CString(errMsg)
			}
		case Last:
			ok, err = b3.Last(ctx)
			if err != nil {
				errMsg := fmt.Sprintf("error moving cursor to Last item of B-tree (id=%v), details: %v", p.BtreeID, err)
				return C.CString(errMsg)
			}
		}
		return C.CString(fmt.Sprintf("%v", ok))
	}
}
