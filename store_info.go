package sop

// StoreInfo contains a given (B-Tree) store details.
type StoreInfo struct {
	// Short name of this (B-Tree store).
	Name string
	// Count of items that can be stored on a given node.
	SlotLength int
	// IsUnique tells whether key/value pair (items) of this tree should be unique on key.
	IsUnique bool
	// IsValueDataInNodeSegment is true if "Value" data is stored in the B-Tree node's data segment.
	// Otherwise is false.
	IsValueDataInNodeSegment bool
	// If true, each Btree Add(..) method call will persist the item value's data to another partition, then on commit,
	// it will then be a very quick action as item(s) values' data were already saved on backend.
	// This rquires 'IsValueDataInNodeSegment' field to be set to false to work.
	IsValueDataActivelyPersisted bool
	// If true, the Value data will be cached in Redis, otherwise not. This is used when 'IsValueDataInNodeSegment'
	// is set to false. Typically set to false if 'IsValueDataActivelyPersisted' is true, as value data is expected
	// to be huge rendering caching it in Redis to affect Redis performance due to the drastic size of data per item.
	IsValueDataGloballyCached bool
	// If true, node load will be balanced by pushing items to sibling nodes if there are vacant slots,
	// otherwise will not. This feature can be turned off if backend is impacted by the "balancing" act.
	LeafLoadBalancing bool
	// (optional) Description of the Store.
	Description string
}
