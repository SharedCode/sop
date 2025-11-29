package domain

import (
	"fmt"

	"github.com/sharedcode/sop/ai"
)

// Config holds the configuration for a generic domain (Agent).
// It defines the components required to run an agent, such as the embedder, index, and policies.
type Config[T any] struct {
	ID         string
	Name       string
	DataPath   string
	Embedder   ai.Embeddings
	Index      ai.VectorStore[T]
	Policy     ai.PolicyEngine
	Classifier ai.Classifier
	Prompts    map[string]string
}

// GenericDomain is a configurable implementation of the Domain interface.
// It allows creating lightweight verticals (Agents) without writing custom domain logic.
type GenericDomain[T any] struct {
	cfg Config[T]
}

// NewGenericDomain creates a new domain instance from the provided configuration.
func NewGenericDomain[T any](cfg Config[T]) *GenericDomain[T] {
	return &GenericDomain[T]{cfg: cfg}
}

// ID returns the unique identifier of the domain.
func (d *GenericDomain[T]) ID() string {
	return d.cfg.ID
}

// Name returns the human-readable name of the domain.
func (d *GenericDomain[T]) Name() string {
	return d.cfg.Name
}

// DataPath returns the file system path where the domain's data is stored.
func (d *GenericDomain[T]) DataPath() string {
	return d.cfg.DataPath
}

// Embedder returns the embedding model used by the domain.
func (d *GenericDomain[T]) Embedder() ai.Embeddings {
	return d.cfg.Embedder
}

// Index returns the vector index used for retrieval.
func (d *GenericDomain[T]) Index() ai.VectorStore[T] {
	return d.cfg.Index
}

// Policies returns the policy engine that enforces safety and compliance rules.
func (d *GenericDomain[T]) Policies() ai.PolicyEngine {
	return d.cfg.Policy
}

// Classifier returns the classifier used for input analysis (e.g., intent detection).
func (d *GenericDomain[T]) Classifier() ai.Classifier {
	return d.cfg.Classifier
}

// Prompt returns a prompt template for the specified kind (e.g., "system", "user").
func (d *GenericDomain[T]) Prompt(kind string) (string, error) {
	if p, ok := d.cfg.Prompts[kind]; ok {
		return p, nil
	}
	return "", fmt.Errorf("prompt kind %q not found in domain %s", kind, d.cfg.ID)
}
