package index

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/sharedcode/sop/ai/internal/port"
)

type SimpleIndex struct {
	name     string
	filePath string
	mu       sync.RWMutex
	data     map[string]storedItem
}

func NewSimple(name string) *SimpleIndex {
	fmt.Println("SimpleIndex: Initializing...")
	// Ensure data directory exists
	_ = os.MkdirAll("ai/data", 0755)

	idx := &SimpleIndex{
		name:     name,
		filePath: fmt.Sprintf("ai/data/%s.json", name),
		data:     make(map[string]storedItem),
	}
	idx.load()
	return idx
}

func (s *SimpleIndex) load() {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.ReadFile(s.filePath)
	if err != nil {
		return // File might not exist yet
	}
	_ = json.Unmarshal(f, &s.data)
}

func (s *SimpleIndex) save() error {
	// Lock is held by caller
	b, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.filePath, b, 0644)
}

func (s *SimpleIndex) Upsert(id string, vec []float32, meta map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[id] = storedItem{
		Vector: vec,
		Meta:   meta,
	}
	return s.save()
}

func (s *SimpleIndex) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, id)
	return s.save()
}

func (s *SimpleIndex) Query(vec []float32, k int, filters map[string]any) ([]port.Hit, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var hits []port.Hit
	for id, item := range s.data {
		if !matchFilters(item.Meta, filters) {
			continue
		}
		score := cosine(vec, item.Vector)
		hits = append(hits, port.Hit{
			ID:    id,
			Score: score,
			Meta:  item.Meta,
		})
	}

	sort.Slice(hits, func(i, j int) bool {
		return hits[i].Score > hits[j].Score
	})

	if k > len(hits) {
		k = len(hits)
	}
	return hits[:k], nil
}
