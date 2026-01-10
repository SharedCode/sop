package agent

import (
	"hash/fnv"
	"math"
)

// BloomFilter is a probabilistic data structure for set membership.
type BloomFilter struct {
	bitset []bool
	k      uint // Number of hash functions
	m      uint // Size of bitset
	n      uint // Estimated number of elements
}

// NewBloomFilter creates a new Bloom Filter optimized for n elements with false positive rate p.
func NewBloomFilter(n uint, p float64) *BloomFilter {
	if n == 0 {
		n = 1000
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}

	// Calculate m (optimal bit array size)
	// m = - (n * ln(p)) / (ln(2)^2)
	m := uint(math.Ceil(float64(-1) * (float64(n) * math.Log(p)) / math.Pow(math.Log(2), 2)))

	// Calculate k (optimal number of hash functions)
	// k = (m / n) * ln(2)
	k := uint(math.Ceil((float64(m) / float64(n)) * math.Log(2)))

	return &BloomFilter{
		bitset: make([]bool, m),
		k:      k,
		m:      m,
		n:      n,
	}
}

// Add adds a string key to the Bloom Filter.
func (bf *BloomFilter) Add(key string) {
	indices := bf.getIndices(key)
	for _, idx := range indices {
		bf.bitset[idx] = true
	}
}

// Test checks if a key might vary well be in the set.
// Returns true if the key MAY be present.
// Returns false if the key is DEFINITELY NOT present.
func (bf *BloomFilter) Test(key string) bool {
	indices := bf.getIndices(key)
	for _, idx := range indices {
		if !bf.bitset[idx] {
			return false
		}
	}
	return true
}

// getIndices returns the k indices for a given key.
// Uses double hashing to simulate k hash functions.
func (bf *BloomFilter) getIndices(key string) []uint {
	h1, h2 := hash(key)
	indices := make([]uint, bf.k)
	for i := uint(0); i < bf.k; i++ {
		// index = (h1 + i*h2) % m
		// using uint64 for calculation to avoid overflow before mod
		idx := (uint64(h1) + uint64(i)*uint64(h2)) % uint64(bf.m)
		indices[i] = uint(idx)
	}
	return indices
}

// hash returns two 64-bit hash values for the given key.
func hash(key string) (uint32, uint32) {
	h := fnv.New64a()
	h.Write([]byte(key))
	v := h.Sum64()

	// Split 64-bit hash into two 32-bit halves
	return uint32(v), uint32(v >> 32)
}
