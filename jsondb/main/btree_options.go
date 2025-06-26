package main

import (
	"github.com/SharedCode/sop"
)

type BtreeOptions struct {
	Name string `json:"name" minLength:"1" maxLength:"20"`
	// Count of items that can be stored on a given node.
	SlotLength int `json:"slot_length" min:"2" max:"10000"`
	// IsUnique tells whether key/value pair (items) of this tree should be unique on key.
	IsUnique bool `json:"is_unique"`
	// (optional) Description of the Store.
	Description string               `json:"description" maxLength:"500"`
	ValueSize   int                  `json:"value_size" min:"0" max:"2"`
	CacheConfig sop.StoreCacheConfig `json:"cache_config"`
}
