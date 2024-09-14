package aws_s3_cache

import (
	"context"

	"github.com/SharedCode/sop"
)

// KeyValueStore is a general purpose Store interface specifying methods or CRUD operations on Key & Value pair.
// Implementations don't need to be too fancy, it can be as simple as supporting partial success.
type KeyValueStore[TK any, TV any] interface {
	// Fetch entry(ies) with given key(s).
	// Fetch term is used here because this CRUD interface is NOT part of the B-Tree system, thus, the context is
	// to "fetch" from the remote data storage sub-system like AWS S3.
	Fetch(context.Context, ...TK) sop.KeyValueStoreResponse[sop.KeyValuePair[TK, TV]]
	// Fetch a large entry with the given key.
	FetchLargeObject(context.Context, TK) (TV, error)
	// Add entry(ies) to the store.
	Add(context.Context, ...sop.KeyValuePair[TK, TV]) sop.KeyValueStoreResponse[sop.KeyValuePair[TK, TV]]
	// Update entry(ies) of the store.
	Update(context.Context, ...sop.KeyValuePair[TK, TV]) sop.KeyValueStoreResponse[sop.KeyValuePair[TK, TV]]
	// Remove entry(ies) from the store given their names.
	Remove(context.Context, ...TK) sop.KeyValueStoreResponse[TK]
}
