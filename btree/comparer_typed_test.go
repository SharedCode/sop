package btree

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// agree checks that the typed comparer orders every pair the same way the boxed
// CoerceComparer does, which is the behavior the B-tree had before.
func agree[T Ordered](t *testing.T, name string, vals []T) {
	t.Helper()
	typed := typedComparerFor(vals[0])
	boxed := CoerceComparer(vals[0])
	for i, a := range vals {
		for j, b := range vals {
			if got, want := typed(a, b), boxed(a, b); got != want {
				t.Fatalf("%s: typed(%v, %v)=%d but boxed=%d (i=%d j=%d)", name, a, b, got, want, i, j)
			}
		}
	}
}

func TestTypedComparerMatchesBoxedComparer(t *testing.T) {
	agree(t, "int", []int{-5, 0, 1, 1, 300, 1 << 40})
	agree(t, "int8", []int8{-128, -1, 0, 7, 127})
	agree(t, "int16", []int16{-300, 0, 5, 32000})
	agree(t, "int32", []int32{-70000, 0, 9, 1 << 30})
	agree(t, "int64", []int64{-1 << 50, 0, 3, 1 << 60})
	agree(t, "uint", []uint{0, 1, 300, 1 << 40})
	agree(t, "uint8", []uint8{0, 1, 200, 255})
	agree(t, "uint16", []uint16{0, 9, 60000})
	agree(t, "uint32", []uint32{0, 9, 1 << 31})
	agree(t, "uint64", []uint64{0, 9, 1 << 63})
	agree(t, "uintptr", []uintptr{0, 4, 99999})
	agree(t, "float32", []float32{-1.5, 0, 0.25, 3e9})
	agree(t, "float64", []float64{-1.5, 0, 0.25, 3e300})
	agree(t, "string", []string{"", "a", "ab", "b", "é"})
	// Types without a fast path keep the boxed comparer.
	agree(t, "bytes", [][]byte{nil, []byte("a"), []byte("ab")})
	agree(t, "time", []time.Time{{}, time.Unix(5, 0), time.Unix(9, 0)})
	agree(t, "uuid", []uuid.UUID{{}, uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("ffffffff-0000-0000-0000-000000000000")})
}

func TestTypedComparerDoesNotAllocateForScalars(t *testing.T) {
	ci := typedComparerFor(1)
	cs := typedComparerFor("a")
	a, b := 100000, 100001
	sa, sb := "alpha", "beta"
	if n := testing.AllocsPerRun(1000, func() { _ = ci(a, b) }); n != 0 {
		t.Fatalf("int compare allocates %v per call", n)
	}
	if n := testing.AllocsPerRun(1000, func() { _ = cs(sa, sb) }); n != 0 {
		t.Fatalf("string compare allocates %v per call", n)
	}
}
