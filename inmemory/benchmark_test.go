// Benchmarks for the in-memory B-tree. Run with:
//
//	go test ./inmemory -run xxx -bench . -benchmem
package inmemory

import (
	"math/rand"
	"testing"
)

func BenchmarkAddSequential(b *testing.B) {
	for i := 0; i < b.N; i++ {
		t := NewBtree[int, int](true)
		for k := 0; k < 20000; k++ {
			t.Add(k, k)
		}
	}
}

func BenchmarkAddShuffled(b *testing.B) {
	perm := rand.New(rand.NewSource(1)).Perm(20000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := NewBtree[int, int](true)
		for _, k := range perm {
			t.Add(k, k)
		}
	}
}

func BenchmarkFind(b *testing.B) {
	t := NewBtree[int, int](true)
	for k := 0; k < 20000; k++ {
		t.Add(k, k)
	}
	keys := rand.New(rand.NewSource(2)).Perm(20000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.Find(keys[i%len(keys)], true)
	}
}

func BenchmarkScan(b *testing.B) {
	t := NewBtree[int, int](true)
	for k := 0; k < 20000; k++ {
		t.Add(k, k)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		for range t.All() {
			n++
		}
	}
}

// A lookup in a tree with integer keys must not allocate. Before the typed
// comparer it allocated about 38 times per call, boxing keys for every compare.
func TestFindDoesNotAllocate(t *testing.T) {
	tree := NewBtree[int, int](true)
	for k := 0; k < 5000; k++ {
		tree.Add(k, k)
	}
	i := 0
	if n := testing.AllocsPerRun(2000, func() { tree.Find((i*7919)%5000, true); i++ }); n != 0 {
		t.Fatalf("Find allocates %v per call, want 0", n)
	}
}
