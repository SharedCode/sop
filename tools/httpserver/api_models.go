package main

import (
	"encoding/json"

	"github.com/sharedcode/sop/ai/memory"
)

// AddSpaceCategoryRequest defines the JSON payload required to create a new Category in a Space.
type AddSpaceCategoryRequest struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Path        string `json:"path,omitempty"`
	Description string `json:"description"`
	ParentID    string `json:"parent_id,omitempty"`
}

// AddSpaceItemRequest defines the JSON payload required to add a new Item to a Space.
// It allows for flexible payload mapping via the Data field.
type AddSpaceItemRequest struct {
	ID         string         `json:"id,omitempty"`
	DocID      memory.DocIDs  `json:"doc_id,omitempty"`
	CategoryID string         `json:"category_id"`
	Summaries  []string       `json:"summaries,omitempty"`
	Positions  [][]float32    `json:"positions,omitempty"`
	Data       map[string]any `json:"data"`
}

// AddSpaceItemsBatchRequest defines the payload for batch processing of space items.
type AddSpaceItemsBatchRequest struct {
	Items []AddSpaceItemRequest `json:"items"`
}

// UpdateSpaceItemRequest defines the JSON payload required to update an existing Item.
type UpdateSpaceItemRequest struct {
	ID         string         `json:"id"`
	CategoryID string         `json:"category_id"`
	Summaries  []string       `json:"summaries,omitempty"`
	Positions  [][]float32    `json:"positions,omitempty"`
	Data       map[string]any `json:"data"`
}

// SpaceItemView defines the JSON model delivered to the UI for displaying Items.
// It acts as a ViewModel rendering data originally stored generically inside Item[T].
type SpaceItemView struct {
	ID          string        `json:"id"`
	DocID       memory.DocIDs `json:"doc_id,omitempty"`
	CategoryID  string        `json:"category_id,omitempty"`
	Category    string        `json:"category"`
	Text        string        `json:"text"`
	Description string        `json:"description"`
	Summaries   []string      `json:"summaries,omitempty"`
	Vector      []float32     `json:"vector,omitempty"`
	VectorSize  int           `json:"vector_size,omitempty"`
}

// TemplateMetadata defines the structure for space template info.
type TemplateMetadata struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	IsDefault   bool   `json:"is_default"`
}

// PreloadSpaceRequest defines the JSON payload for preloading a space from a template.
type PreloadSpaceRequest struct {
	TemplateID   string `json:"template_id"`
	DatabaseName string `json:"database_name"`
}

// IngestSpaceRequest defines the JSON payload for ingesting knowledge into a space.
type IngestSpaceRequest struct {
	DatabaseName    string                      `json:"database_name"`
	SpaceName       string                      `json:"space_name,omitempty"`
	URL             string                      `json:"url,omitempty"`
	PreloadFilePath string                      `json:"preload_filepath,omitempty"`
	Attributes      *memory.KnowledgeBaseConfig `json:"attributes,omitempty"`
	CustomData      json.RawMessage             `json:"custom_data,omitempty"`
}

// SpaceIngestChunk represents a discrete chunk of data/knowledge for ingestion.
type SpaceIngestChunk struct {
	ID               string         `json:"id"`
	DocID            memory.DocIDs  `json:"doc_id,omitempty"`
	Category         string         `json:"category"`
	Text             string         `json:"text"`
	Description      string         `json:"description"`
	Summaries        interface{}    `json:"summaries"`
	Vectors          [][]float32    `json:"vectors"`
	SummariesVectors [][]float32    `json:"summaries_vectors,omitempty"`
	Data             map[string]any `json:"data,omitempty"`
}

// CreateSpaceRequest defines the JSON payload to create a new space.
type CreateSpaceRequest struct {
	DatabaseName string                      `json:"database_name"`
	SpaceName    string                      `json:"space_name"`
	Attributes   *memory.KnowledgeBaseConfig `json:"attributes,omitempty"`
}
