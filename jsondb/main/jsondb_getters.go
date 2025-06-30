package main

import "C"
import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/jsondb"
)

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
