package main

import "C"
import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/jsondb"
)

func getItems(ctx context.Context, ps string, payload2 *C.char) (*C.char, *C.char) {
	var p ManageBtreeMetaData
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(ps), &p); err != nil {
		errMsg := fmt.Sprintf("error Unmarshal ManageBtreeMetaData, details: %v", err)
		return nil, C.CString(errMsg)
	}

	if p.IsPrimitiveKey {
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
		res, err := b3.GetItems(ctx, payload)
		if err != nil {
			errMsg := fmt.Sprintf("error getItems from B-tree (id=%v), details: %v", p.BtreeID, err)
			return nil, C.CString(errMsg)
		}
		return C.CString(res), nil
	} else {
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
		res, err := b3.GetItems(ctx, payload)
		if err != nil {
			errMsg := fmt.Sprintf("error getItems from B-tree (id=%v), details: %v", p.BtreeID, err)
			return nil, C.CString(errMsg)
		}
		return C.CString(res), nil
	}
}
