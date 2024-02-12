package sop

// KeyValuePair is a tuple, 'used in Blob Store to allow caller to specify a
// different ID(or key) for a given blob entry.
type KeyValuePair[TK any, TV any] struct {
	// Key is the key part in the pair.
	Key TK
	// Value is the value part in the pair.
	Value TV
}

// Tuple of two items. If there is less concept of Key and Value and leaning towards more generic
// pair of items(first and second), then please use this one instead of KeyValuePair.
type Tuple[T1 any, T2 any] struct {
	// First item in the pair.
	First T1
	// Second item in the pair.
	Second T2
}
