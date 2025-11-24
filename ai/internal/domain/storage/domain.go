package storage

import (
	"errors"

	"github.com/sharedcode/sop/ai/internal/policy"
	"github.com/sharedcode/sop/ai/internal/port"
)

type StorageDomain struct {
	embedder port.Embeddings
	index    port.VectorIndex
	policies port.PolicyEngine
}

func New(_ map[string]any) (port.Domain, error) {
	return &StorageDomain{embedder: nil, index: nil, policies: policy.NewAllow("storage-base")}, nil
}

func (d *StorageDomain) ID() string                  { return "storage" }
func (d *StorageDomain) Embedder() port.Embeddings   { return d.embedder }
func (d *StorageDomain) Index() port.VectorIndex     { return d.index }
func (d *StorageDomain) Policies() port.PolicyEngine { return d.policies }
func (d *StorageDomain) Prompt(kind string) (string, error) {
	switch kind {
	case "integrity":
		return "You are a storage integrity assistant.", nil
	default:
		return "", errors.New("unknown prompt kind")
	}
}
