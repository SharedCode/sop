package port

type Domain interface {
	ID() string
	Embedder() Embeddings
	Index() VectorIndex
	Policies() PolicyEngine
	Prompt(kind string) (string, error)
}

type Payload map[string]any

type DomainFunction interface {
	ID() string
	Invoke(in Payload) (Payload, error)
	Policies() PolicyEngine
	Depends() []string
}
