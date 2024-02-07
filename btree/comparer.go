package btree

import (
	"cmp"
)

// Comparer interface specifies the Compare function.
type Comparer interface {
	// Implement Compare to compare this object with the 'other' object.
	// Returns -1 (this object < other), 0 (this object == other), 1 (this object > other)
	Compare(other interface{}) int
}

// Comparable interface is used as a B-Tree store (generics) constraint for Key types.
type Comparable interface {
	cmp.Ordered | *Comparer | any
}

// Compare can Compare a Comparable type.
func Compare[T Comparable](x, y T) int {
	anyX := any(x)
	anyY := any(y)
	switch any(x).(type) {
	case int:
		x1, _ := anyX.(int)
		y1, _ := anyY.(int)
		return cmp.Compare(x1, y1)
	case int8:
		x1, _ := anyX.(int8)
		y1, _ := anyY.(int8)
		return cmp.Compare(x1, y1)
	case int16:
		x1, _ := anyX.(int16)
		y1, _ := anyY.(int16)
		return cmp.Compare(x1, y1)
	case int32:
		x1, _ := anyX.(int32)
		y1, _ := anyY.(int32)
		return cmp.Compare(x1, y1)
	case int64:
		x1, _ := anyX.(int64)
		y1, _ := anyY.(int64)
		return cmp.Compare(x1, y1)
	case uint:
		x1, _ := anyX.(uint)
		y1, _ := anyY.(uint)
		return cmp.Compare(x1, y1)
	case uint8:
		x1, _ := anyX.(uint8)
		y1, _ := anyY.(uint8)
		return cmp.Compare(x1, y1)
	case uint16:
		x1, _ := anyX.(uint16)
		y1, _ := anyY.(uint16)
		return cmp.Compare(x1, y1)
	case uint32:
		x1, _ := anyX.(uint32)
		y1, _ := anyY.(uint32)
		return cmp.Compare(x1, y1)
	case uint64:
		x1, _ := anyX.(uint64)
		y1, _ := anyY.(uint64)
		return cmp.Compare(x1, y1)
	case uintptr:
		x1, _ := anyX.(uintptr)
		y1, _ := anyY.(uintptr)
		return cmp.Compare(x1, y1)
	case float32:
		x1, _ := anyX.(float32)
		y1, _ := anyY.(float32)
		return cmp.Compare(x1, y1)
	case float64:
		x1, _ := anyX.(float64)
		y1, _ := anyY.(float64)
		return cmp.Compare(x1, y1)
	case string:
		x1, _ := anyX.(string)
		y1, _ := anyY.(string)
		return cmp.Compare(x1, y1)
	default:
		if anyX == nil && anyY == nil {
			return 0
		}
		if anyX == nil {
			return -1
		}
		if anyY == nil {
			return 1
		}
		cX, _ := anyX.(Comparer)
		return cX.Compare(y)
	}
}
