package domain

import (
	"fmt"

	"github.com/sharedcode/sop/ai"
)

// Config holds the configuration for a generic domain (Agent).
// It defines the components required to run an agent, such as the embedder, index, and policies.
type Config struct {
	ID         string
	DataPath   string
	Embedder   ai.Embeddings
	Index      ai.VectorIndex
	Policy     ai.PolicyEngine
	Classifier ai.Classifier
	Prompts    map[string]string
}

// GenericDomain is a configurable implementation of the Domain interface.
// It allows creating lightweight verticals (Agents) without writing custom domain logic.
type GenericDomain struct {
	cfg Config
}

// NewGenericDomain creates a new domain instance from the provided configuration.
func NewGenericDomain(cfg Config) *GenericDomain {
	return &GenericDomain{cfg: cfg}
}

// ID returns the unique identifier of the domain.
func (d *GenericDomain) ID() string {
	return d.cfg.ID
}

// DataPath returns the file system path where the domain's data is stored.
func (d *GenericDomain) DataPath() string {
	return d.cfg.DataPath
}

// Embedder returns the embedding model used by the domain.
func (d *GenericDomain) Embedder() ai.Embeddings {
	return d.cfg.Embedder
}

// Index returns the vector index used for retrieval.
func (d *GenericDomain) Index() ai.VectorIndex {
	return d.cfg.Index
}

// Policies returns the policy engine that enforces safety and compliance rules.
func (d *GenericDomain) Policies() ai.PolicyEngine {
	return d.cfg.Policy
}

// Classifier returns the classifier used for input analysis (e.g., intent detection).
func (d *GenericDomain) Classifier() ai.Classifier {
	return d.cfg.Classifier
}

// Prompt returns a prompt template for the specified kind (e.g., "system", "user").
func (d *GenericDomain) Prompt(kind string) (string, error) {
	if p, ok := d.cfg.Prompts[kind]; ok {
		return p, nil
	}
	return "", fmt.Errorf("prompt kind %q not found in domain %s", kind, d.cfg.ID)
}
