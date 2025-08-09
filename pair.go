package sop

// KeyValuePair represents a pair of key and value, commonly used for blob operations where the key may differ from the blob ID.
type KeyValuePair[TK any, TV any | []byte] struct {
	// Key is the key part in the pair.
	Key TK
	// Value is the value part in the pair.
	Value TV
}

// Tuple represents an ordered pair of two generic values when Key/Value semantics are not desired.
type Tuple[T1 any, T2 any] struct {
	// First is the first element of the pair.
	First T1
	// Second is the second element of the pair.
	Second T2
}
