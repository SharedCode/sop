// Package Cassandra contains code for integration or inter-operation with Cassandra DB.
package cassandra

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
)

// Manage or fetch Virtual Id request/response payload.
type VirtualIdPayload[T sop.Handle | btree.UUID] struct {
	// Registry table (name) where the Virtual Ids will be stored or fetched from.
	RegistryTable string
	// IDs is an array containing the Virtual Ids details to be stored or to be fetched.
	IDs []T
}

// Virtual Id registry is essential in our support for all or nothing (sub)feature,
// which is essential in "fault tolerant" & "self healing" feature.
//
// All methods are taking in a set of items and need to be implemented to do
// all or nothing feature, e.g. wrapped in transaction in Cassandra.
type VirtualIdRegistry interface {
	// Get will fetch handles(given their Ids) from stores(given a store name).
	// Supports an array of store names with a set of handle Ids each.
	Get(context.Context, ...VirtualIdPayload[btree.UUID]) ([]VirtualIdPayload[sop.Handle], error)
	// Add will insert handles to stores(given a store name).
	// Supports an array of store names with a set of handles each.
	Add(context.Context, ...VirtualIdPayload[sop.Handle]) error
	// Update will update handles of stores(given a store name).
	// Supports an array of store names with a set of handle each.
	Update(context.Context, ...VirtualIdPayload[sop.Handle]) error
	// Remove will delete handles(given their Ids) from stores(given a store name).
	// Supports an array of store names with a set of handle each.
	Remove(context.Context, ...VirtualIdPayload[btree.UUID]) error
}
