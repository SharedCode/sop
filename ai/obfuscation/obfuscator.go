package obfuscation

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MetadataObfuscator handles the hashing of sensitive metadata.
type MetadataObfuscator struct {
	realToHash map[string]string
	hashToReal map[string]string
	mu         sync.RWMutex
}

// NewMetadataObfuscator creates a new MetadataObfuscator.
func NewMetadataObfuscator() *MetadataObfuscator {
	return &MetadataObfuscator{
		realToHash: make(map[string]string),
		hashToReal: make(map[string]string),
	}
}

// Obfuscate returns a hash for the given real name.
// If the name has already been obfuscated, it returns the existing hash.
func (m *MetadataObfuscator) Obfuscate(realName string, prefix string) string {
	if realName == "" {
		return ""
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if hash, ok := m.realToHash[realName]; ok {
		return hash
	}

	// Use SHA256 to generate a deterministic, ID-like hash
	sum := sha256.Sum256([]byte(realName))
	// Take first 8 bytes (16 hex chars) for a reasonable length ID
	hexStr := hex.EncodeToString(sum[:8])

	hash := fmt.Sprintf("%s_%s", prefix, hexStr)
	m.realToHash[realName] = hash
	m.hashToReal[hash] = realName
	return hash
}

// Deobfuscate returns the real name for the given hash.
func (m *MetadataObfuscator) Deobfuscate(hash string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if real, ok := m.hashToReal[hash]; ok {
		return real
	}
	return hash
}

// DeobfuscateText replaces all known hashes in the text with their real names.
func (m *MetadataObfuscator) DeobfuscateText(text string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Collect keys and sort by length descending to avoid prefix issues (e.g. replacing DB_1 in DB_10)
	hashes := make([]string, 0, len(m.hashToReal))
	for hash := range m.hashToReal {
		hashes = append(hashes, hash)
	}
	sort.Slice(hashes, func(i, j int) bool {
		return len(hashes[i]) > len(hashes[j])
	})

	for _, hash := range hashes {
		real := m.hashToReal[hash]
		text = strings.ReplaceAll(text, hash, real)
	}
	return text
}

// ObfuscateText replaces all known real names in the text with their hashes.
// Note: This can be expensive if there are many names.
func (m *MetadataObfuscator) ObfuscateText(text string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Collect keys and sort by length descending to avoid prefix issues
	reals := make([]string, 0, len(m.realToHash))
	for real := range m.realToHash {
		reals = append(reals, real)
	}
	sort.Slice(reals, func(i, j int) bool {
		return len(reals[i]) > len(reals[j])
	})

	for _, real := range reals {
		hash := m.realToHash[real]
		text = strings.ReplaceAll(text, real, hash)
	}
	return text
}

// RegisterResource registers a resource name to be obfuscated.
// It is a wrapper around Obfuscate.
func (m *MetadataObfuscator) RegisterResource(name, resourceType string) {
	m.Obfuscate(name, resourceType)
}

// GlobalObfuscator is the shared instance.
var GlobalObfuscator = NewMetadataObfuscator()
