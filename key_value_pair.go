package sop

// KeyValuePair is a tuple, 'used in Blob Store to allow caller to specify a
// different ID(or key) for a given blob entry.
type KeyValuePair[TK any, TV any] struct {
	// Key is the key part in the pair.
	Key TK
	// Value is the value part in the pair.
	Value TV
}
