package domain

import "github.com/sharedcode/sop/ai/internal/port"

type SimpleDomain struct {
	id      string
	emb     port.Embeddings
	idx     port.VectorIndex
	pol     port.PolicyEngine
	prompts map[string]string
	gen     port.Generator
}

func NewSimple(id string, emb port.Embeddings, idx port.VectorIndex, pol port.PolicyEngine, prompts map[string]string, gen port.Generator) *SimpleDomain {
	return &SimpleDomain{id: id, emb: emb, idx: idx, pol: pol, prompts: prompts, gen: gen}
}

func (d *SimpleDomain) ID() string                  { return d.id }
func (d *SimpleDomain) Embedder() port.Embeddings   { return d.emb }
func (d *SimpleDomain) Index() port.VectorIndex     { return d.idx }
func (d *SimpleDomain) Policies() port.PolicyEngine { return d.pol }
func (d *SimpleDomain) Prompt(kind string) (string, error) {
	if p, ok := d.prompts[kind]; ok {
		return p, nil
	}
	return "", nil
}
