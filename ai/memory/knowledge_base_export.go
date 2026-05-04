package memory

import (
	"context"
	"encoding/json"
	"io"

	"github.com/sharedcode/sop"
)

// ExportData defines the structure of the KnowledgeBase JSON payload.
type ExportData[T any] struct {
	Categories []*Category     `json:"categories"`
	Items      []ExportItem[T] `json:"items"`
}

// ExportItem dictates what fields from the item are serialized.
type ExportItem[T any] struct {
	Category         string      `json:"category"`
	Data             T           `json:"data"`
	Summaries        []string    `json:"summaries,omitempty"`
	SummariesVectors [][]float32 `json:"summaries_vectors,omitempty"`
}

// ExportJSON serializes the KnowledgeBase contents into a JSON stream.
func (kb *KnowledgeBase[T]) ExportJSON(ctx context.Context, writer io.Writer) error {
	catBtree, err := kb.Store.Categories(ctx)
	if err != nil {
		return err
	}

	exportData := ExportData[T]{}
	catMap := make(map[sop.UUID]string)

	ok, _ := catBtree.First(ctx)
	for ok {
		cat, err := catBtree.GetCurrentValue(ctx)
		if err == nil && cat != nil {
			exportData.Categories = append(exportData.Categories, cat)
			catMap[cat.ID] = cat.Name
		}
		ok, _ = catBtree.Next(ctx)
	}

	itemsBtree, err := kb.Store.Items(ctx)
	if err != nil {
		return err
	}

	ok, _ = itemsBtree.First(ctx)
	for ok {
		item, err := itemsBtree.GetCurrentValue(ctx)
		if err == nil {
			var vectors [][]float32
			if len(item.Positions) > 0 {
				vecBtree, _ := kb.Store.Vectors(ctx)
				if vecBtree != nil {
					for _, pos := range item.Positions {
						has, _ := vecBtree.Find(ctx, pos, false)
						if has {
							v, _ := vecBtree.GetCurrentValue(ctx)
							vectors = append(vectors, v.Data)
						}
					}
				}
			}

			catName := catMap[item.CategoryID]
			exportData.Items = append(exportData.Items, ExportItem[T]{
				Category:         catName,
				Data:             item.Data,
				Summaries:        item.Summaries,
				SummariesVectors: vectors,
			})
		}
		ok, _ = itemsBtree.Next(ctx)
	}

	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(exportData)
}

// ImportJSON deserializes a JSON stream and ingests it into the KnowledgeBase.
func (kb *KnowledgeBase[T]) ImportJSON(ctx context.Context, reader io.Reader, persona string) error {
	var exportData ExportData[T]
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(&exportData); err != nil {
		return err
	}

	catBtree, err := kb.Store.Categories(ctx)
	if err != nil {
		return err
	}

	for _, c := range exportData.Categories {
		// Use manual loop as BTree Find requires exact sop.UUID which may not match
		ok, _ := catBtree.First(ctx)
		found := false
		for ok {
			existing, _ := catBtree.GetCurrentValue(ctx)
			if existing != nil && existing.Name == c.Name {
				found = true
				break
			}
			ok, _ = catBtree.Next(ctx)
		}
		if !found {
			c.ID = sop.NewUUID()
			catBtree.Add(ctx, c.ID, c)
		}
	}

	var thoughts []Thought[T]
	for _, it := range exportData.Items {
		thoughts = append(thoughts, Thought[T]{
			Category:  it.Category,
			Data:      it.Data,
			Vectors:   it.SummariesVectors,
			Summaries: it.Summaries,
		})
	}

	return kb.IngestThoughts(ctx, thoughts, persona)
}
