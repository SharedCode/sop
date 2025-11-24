package port

type Embeddings interface {
	Name() string
	Dim() int
	EmbedTexts(texts []string) ([][]float32, error)
}

type VectorIndex interface {
	Upsert(id string, vec []float32, meta map[string]any) error
	Query(vec []float32, k int, filters map[string]any) ([]Hit, error)
	Delete(id string) error
}

type Hit struct {
	ID    string
	Score float32
	Meta  map[string]any
}
