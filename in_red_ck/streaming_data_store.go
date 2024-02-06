package in_red_ck

// import (
// 	"context"
// 	"io"
// 	"encoding/json"

// 	"github.com/SharedCode/sop"
// 	"github.com/SharedCode/sop/btree"
// )

// type StreamingDataStore[TK btree.Comparable] interface {
// 	// Add adds an item to the b-tree and does not check for duplicates.
// 	Add(ctx context.Context, key TK) (json.Encoder, error)
// 	// AddIfNotExist adds an item if there is no item matching the key yet.
// 	// Otherwise, it will do nothing and return false, for not adding the item.
// 	// This is useful for cases one wants to add an item without creating a duplicate entry.
// 	AddIfNotExist(ctx context.Context, key TK) (json.Encoder, error)

// 	// Update finds the item with key and update its value to the value argument.
// 	Update(ctx context.Context, key TK) (json.Encoder, error)
// 	// UpdateCurrentItem will update the Value of the current item.
// 	// Key is read-only, thus, no argument for the key.
// 	UpdateCurrentItem(ctx context.Context) (json.Encoder, error)
// 	// Remove will find the item with a given key then remove that item.
// 	Remove(ctx context.Context, key TK) (bool, error)
// 	// RemoveCurrentItem will remove the current key/value pair from the store.
// 	RemoveCurrentItem(ctx context.Context) (bool, error)

// 	// FindOne will search Btree for an item with a given key. Return true if found,
// 	// otherwise false. firstItemWithKey is useful when there are items with same key.
// 	// true will position pointer to the first item with the given key,
// 	// according to key ordering sequence.
// 	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
// 	FindOne(ctx context.Context, key TK, firstItemWithKey bool) (bool, error)
// 	// FindOneWithID is synonymous to FindOne but allows code to supply the Item's ID to identify it.
// 	// This is useful for B-Tree that allows duplicate keys(IsUnique = false) as it provides a way to
// 	// differentiate duplicated keys via the unique ID(sop.UUID).
// 	FindOneWithID(ctx context.Context, key TK, id sop.UUID) (bool, error)
// 	// GetCurrentKey returns the current item's key.
// 	GetCurrentKey() TK
// 	// GetCurrentValue returns the current item's value.
// 	GetCurrentValue(ctx context.Context) (json.Decoder, error)
// 	// GetCurrentItemID returns the current item's ID.
// 	GetCurrentItemID(ctx context.Context) (Item[TK, TV], error)

// 	// First positions the "cursor" to the first item as per key ordering.
// 	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
// 	First(ctx context.Context) (bool, error)
// 	// Last positionts the "cursor" to the last item as per key ordering.
// 	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
// 	Last(ctx context.Context) (bool, error)
// 	// Next positions the "cursor" to the next item as per key ordering.
// 	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
// 	Next(ctx context.Context) (bool, error)
// 	// Previous positions the "cursor" to the previous item as per key ordering.
// 	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
// 	Previous(ctx context.Context) (bool, error)

// 	// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
// 	// Specifying uniqueness base on key makes the B-Tree permanently set. If you want just a temporary
// 	// unique check during Add of an item, then you can use AddIfNotExist method for that.
// 	IsUnique() bool

// 	// Returns the number of items in this B-Tree.
// 	Count() int64
// }

// func NewEncoder() *json.Encoder {
// 	var w io.Writer
// 	je := json.NewEncoder(w)
//  je.Encode()
// }
