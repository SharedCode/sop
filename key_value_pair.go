package sop

// KeyValuePair is a tuple, 'used in Blob Store to allow caller to specify a
// different Id(or key) for a given blob entry.
type KeyValuePair[TK any, TV any] struct {
	Key   TK
	Value TV
}
