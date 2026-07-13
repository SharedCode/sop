package inmemory

import (
	"iter"

	"github.com/sharedcode/sop/btree"
)

// All returns an iterator over every key/value pair in ascending key order,
// usable with Go's range-over-func:
//
//	for k, v := range b3.All() {
//		...
//	}
//
// It drives the B-Tree cursor, so avoid interleaving it with manual
// First/Next/Previous navigation on the same tree.
func (b3 BtreeInterface[TK, TV]) All() iter.Seq2[TK, TV] {
	return func(yield func(TK, TV) bool) {
		for ok := b3.First(); ok; ok = b3.Next() {
			if !yield(b3.GetCurrentKey(), b3.GetCurrentValue()) {
				return
			}
		}
	}
}

// Range returns an iterator over pairs whose keys fall within from..to
// inclusive, in ascending key order. It seeks straight to the start of the
// range using the tree's search, so cost is proportional to the range size,
// not the tree size. Like All, it drives the B-Tree cursor.
func (b3 BtreeInterface[TK, TV]) Range(from, to TK) iter.Seq2[TK, TV] {
	return func(yield func(TK, TV) bool) {
		compare := btree.CoerceComparer(to)
		if !b3.Find(from, true) {
			// On a miss the cursor parks on a neighboring key: the smallest
			// key greater than from, or the largest tree key when from is
			// past the end. An empty tree leaves no current item, flagged
			// by a nil item ID.
			if b3.Btree.GetCurrentKey().ID.IsNil() {
				return
			}
			for compare(b3.GetCurrentKey(), from) < 0 {
				if !b3.Next() {
					return
				}
			}
		}
		for {
			k := b3.GetCurrentKey()
			if compare(k, to) > 0 {
				return
			}
			if !yield(k, b3.GetCurrentValue()) {
				return
			}
			if !b3.Next() {
				return
			}
		}
	}
}
