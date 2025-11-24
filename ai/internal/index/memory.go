package index

import (
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/sharedcode/sop/ai/internal/port"
)

type Memory struct {
	mu    sync.RWMutex
	vecs  map[string][]float32
	metas map[string]map[string]any
}

func NewMemory() *Memory {
	return &Memory{vecs: map[string][]float32{}, metas: map[string]map[string]any{}}
}

func (m *Memory) Upsert(id string, vec []float32, meta map[string]any) error {
	if id == "" {
		return errors.New("empty id")
	}
	cp := append([]float32(nil), vec...)
	m.mu.Lock()
	m.vecs[id] = cp
	if meta != nil {
		m.metas[id] = meta
	} else {
		delete(m.metas, id)
	}
	m.mu.Unlock()
	return nil
}

func cosine(a, b []float32) float32 {
	var dot, na, nb float32
	l := len(a)
	if len(b) < l {
		l = len(b)
	}
	for i := 0; i < l; i++ {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / float32(math.Sqrt(float64(na))*math.Sqrt(float64(nb)))
}

type hit struct {
	id    string
	score float32
}

func (m *Memory) Query(vec []float32, k int, filters map[string]any) ([]port.Hit, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tmp := make([]hit, 0, len(m.vecs))
	for id, v := range m.vecs {
		if len(filters) > 0 {
			meta := m.metas[id]
			skip := false
			for fk, fv := range filters {
				if mv, ok := meta[fk]; !ok || mv != fv {
					skip = true
					break
				}
			}
			if skip {
				continue
			}
		}
		tmp = append(tmp, hit{id: id, score: cosine(vec, v)})
	}
	sort.Slice(tmp, func(i, j int) bool { return tmp[i].score > tmp[j].score })
	if k > len(tmp) {
		k = len(tmp)
	}
	out := make([]port.Hit, 0, k)
	for i := 0; i < k; i++ {
		meta := m.metas[tmp[i].id]
		out = append(out, port.Hit{ID: tmp[i].id, Score: tmp[i].score, Meta: meta})
	}
	return out, nil
}

func (m *Memory) Delete(id string) error {
	m.mu.Lock()
	delete(m.vecs, id)
	delete(m.metas, id)
	m.mu.Unlock()
	return nil
}
