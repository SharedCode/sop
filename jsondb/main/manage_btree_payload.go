package main

import (
	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/jsondb"
)

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
type ManageBtreePayload struct {
	BtreeID    sop.UUID      `json:"id"`
	Items      []jsondb.Item `json:"items"`
	PagingInfo `json:"paging_info"`
}
